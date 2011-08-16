import os
import sqlite3
import time
from error.generic_errors import *
from error.db_errors import *

class DBSQLite:
	"""This class manages SQLite connections"""
	dbfilename = ''
	connection = ''
	cursor = ''

	def _buildFieldStruct(self, sqlitefield):
		"""Builds DBSQLite field structure from Python SQLite field structure list element"""
		return {
					'name': sqlitefield[1],
					'type': sqlitefield[2],
					'not_null': sqlitefield[3],
					'default_value': str(sqlitefield[4]).replace("'", ""),
					'pk': sqlitefield[5]
				}

	def createDatabase(self, dbfilename):
		self.dbfilename = dbfilename
		
		"""Open an empty file"""
		fp = open(self.dbfilename, 'w')
		fp.close()

		"""Now connect with SQLite"""
		return self.openDatabase(dbfilename)

	def openDatabase(self, dbfilename):
		self.dbfilename = dbfilename
		
		if not os.path.exists(self.dbfilename):
			raise
		
		try:
			self.connection = sqlite3.connect(self.dbfilename)
			self.cursor = self.connection.cursor()
			
			return 1
		except sqlite3.Error, e:
			raise DBConnectionError("Could not open database file: " + self.dbfilename)
	
	def getTableList(self):
		"""Returns list of tables"""
		query = "SELECT * FROM sqlite_master"
		try:
			result = self.cursor.execute(query)
			
			tables = []
			for table in result:
				tables.append(table[1])

			return tables

		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def getTableStructure(self, table_name):
		"""Returns table structure"""
		query = "PRAGMA table_info('%s')" % table_name
		
		try:
			result = self.cursor.execute(query)
			fields = result.fetchall()
			table_struct = []
			for field in fields:
				table_struct.append(self._buildFieldStruct(field))
			
			return table_struct

		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])

	def getTableData(self, table_name, limit):
		query = "SELECT * FROM " + table_name

		if limit == 0:
			query+= " LIMIT " + limit
		
		try:
			result = self.cursor.execute(query)
			return result.fetchall()

		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def _getFieldPart(self, field):
		"""docstring for _getFieldPart"""
		query_part = field['name'] + " " + field['type']
		
		if field['not_null'] == 1:
			query_part+= " NOT NULL "
		else:
			query_part+= " null"
		
		if field['default_value'] != None:
			if field['type'] == 'integer':
				query_part+= " DEFAULT "+ field['default_value']
			else:
				query_part+= " DEFAULT '" + field['default_value'] + "'"
		
		if field['type'] == 'integer' and field['pk'] == 1:
			query_part+= " primary key"
		
		return query_part
	
	def createTable(self, table_name, fields):
		"""docstring for createTable"""
		query = "CREATE TABLE " + table_name + "("
		
		query_parts = []
		for field in fields:
			query_parts.append(self._getFieldPart(field))
		
		query+= ', '.join(query_parts) + ')'
		
		try:
			self.cursor.execute(query)
			return 1
		
		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def dropTable(self, table_name):
		"""docstring for dropTable"""
		query = "DROP TABLE " + table_name
		try:
			self.cursor.execute(query)
			return 1
		
		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def renameTable(self, old_table_name, new_table_name):
		query = "ALTER TABLE " + old_table_name + " RENAME TO " + new_table_name
		
		try:
			self.cursor.execute(query)
			return 1
		
		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def addField(self, table_name, field):
		query = "ALTER TABLE " + table_name + " ADD COLUMN " + self._getFieldPart(field)
		
		try:
			self.cursor.execute(query)
			return 1
		
		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])

	def renameField(self, table_name, old_field_name, new_field_name):
		"Get table structure first"
		old_struct = self.getTableStructure(table_name)
		
		"""New table structure"""
		createtime = str(time.time()).split('.')[0]
		tmp_table_name = "_tmp_" + createtime

		new_struct = []
		new_field_names = []
		for old_field in old_struct:
			if old_field['name'] == old_field_name:
				old_field['name'] = new_field_name

			new_field_names.append(old_field['name'])
			new_struct.append(old_field)
		
		try:
			self.createTable(tmp_table_name, new_struct)
		except DBError:
			self.connection.rollback()
			raise
		
		"""Now copy the existing data from old table to new table"""
		try:
			self.copyTableData(table_name, tmp_table_name, new_field_names)
		except DBError:
			self.connection.rollback()
			raise
		
		"""Now drop existing table, and rename temporary table to original name"""
		try:
			self.dropTable(table_name)
		except DBError:
			self.connection.rollback()
			raise
		
		try:
			self.renameTable(tmp_table_name, table_name)
		except DBError:
			self.connection.rollback()
			raise

	def dropField(self, table_name, field_name):
		"Get table structure first"
		old_struct = self.getTableStructure(table_name)
		
		"""New table structure"""
		createtime = str(time.time()).split('.')[0]
		tmp_table_name = "_tmp_" + createtime

		new_struct = []
		new_field_names = []
		for old_field in old_struct:
			if old_field['name'] != field_name:
				new_field_names.append(old_field['name'])
				new_struct.append(old_field)
		
		try:
			self.createTable(tmp_table_name, new_struct)
		except DBError:
			self.connection.rollback()
			raise
		
		"""Now copy the existing data from old table to new table"""
		try:
			self.copyTableData(table_name, tmp_table_name, new_field_names)
		except DBError:
			self.connection.rollback()
			raise
		
		"""Now drop existing table, and rename temporary table to original name"""
		try:
			self.dropTable(table_name)
		except DBError:
			self.connection.rollback()
			raise
		
		try:
			self.renameTable(tmp_table_name, table_name)
		except DBError:
			self.connection.rollback()
			raise
		
		return 1
		
	def copyTableData(self, source_table, dest_table, field_names):
		"""Copies data from source_table to dest_table with field_names"""
		query = "INSERT INTO " + dest_table + " SELECT " + ', '.join(field_names)
		query+= " FROM " + source_table
		
		try:
			self.cursor.execute(query)
			return 1
		except sqlite3.Error, e:
			raise DBQueryError("A query error occured: " + e.args[0])
	
	def executeQuery(self, query):
		"""Executes given query"""
		try:
			return self.cursor.execute(query)
		except sqlite3.Error e:
			raise DBQueryError("A query error occured: " + e.args[0])

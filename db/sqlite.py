import os
import sqlite3

class DBSQLite:
	"""This class manages SQLite connections"""
	dbfilename = ''
	connection = ''
	cursor = ''
	
	def createDatabase(self, dbfilename):
		self.dbfilename = dbfilename
		
		try:
			"""Open an empty file"""
			fp = open(self.dbfilename, 'w')
			fp.close()
			
			"""Now connect with SQLite"""
			return self.openDatabase(dbfilename)
		except BaseException:
			return -1

	def openDatabase(self, dbfilename):
		self.dbfilename = dbfilename
		
		if not os.path.exists(self.dbfilename):
			return -2
		
		try:
			self.connection = sqlite3.connect(self.dbfilename)
			self.cursor = self.connection.cursor()
			
			return 1
		except sqlite3.Error as e:
			return -3
	
	def getTableList(self):
		"""docstring for getTableList"""
		query = "SELECT * FROM sqlite_master"
		try:
			result = self.cursor.execute(query)
			
			tables = []
			for table in result:
				tables.append(table[1])

			return tables

		except sqlite3.Error:
			return -1
	
	def getTableStructure(self, table_name):
		"""docstring for getTableStructure"""
		query = "PRAGMA table_info('%s')" % table_name
		
		try:
			result = self.cursor.execute(query)
			return result.fetchall()

		except sqlite3.Error:
			return -1

	def getTableData(self, table_name, limit):
		query = "SELECT * FROM " + table_name

		if limit == 0:
			query+= " LIMIT " + limit
		
		try:
			result = self.cursor.execute(query)
			return result.fetchall()

		except sqlite3.Error:
			return -1
	
	def _getFieldPart(self, field):
		"""docstring for _getFieldPart"""
		query_part = field['name'] + " " + field['type']
		
		if field['null'] == 1:
			query_part+= " null"
		else:
			query_part+= " NOT NULL "
		
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
		
		except sqlite3.Error:
			return -1
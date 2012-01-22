#!/usr/bin/python

from db.sqlite import *

table_name = 'colors'

mydb = DBSQLite()
print mydb.openDatabase("/Users/yasar/Code/SQLiteManager/deneme.db")

tables = mydb.getTableList()
print "Table List:"
print "-----------"
for tablename in tables:
	print tablename

print ""
print "Table Structure:"
print "----------------"
try:
	table_struct = mydb.getTableStructure(table_name)
	for field in table_struct:
		print "Field Name: " + field['name']
		print "Field Type: " + field['type']
		print "Not null: " + str(field['not_null'])
		print "Default Value: " + str(field['default_value'])
		print "Primary Key: " + str(field['pk'])
		print "--"
except DBError, e:
	print "Error"

tabledata = mydb.getTableData(table_name, 1)

print ""
print "Table data: " + table_name
print "----------------"

for row in tabledata:
	print row

print ""
print "New table: test"
print "---------------"

fields = [
			{'name': 'id', 'type': 'integer', 'default_value': None, 'pk': 1, 'not_null': 1},
			{'name': 'color', 'type': 'text', 'default_value': 'red', 'not_null': 1, 'pk': 0}
		]

#mydb.createTable('test', fields)

#result = mydb.dropTable('yasar')
#print "Drop table result: " + str(result)

print ""
print "Add field to test: name"
print "-----------------------"

field = {'name': 'name', 'type': 'text', 'default_value': None, 'not_null': 0, 'pk': 0}
#mydb.addField('test', field)

print ""
print "Drop field from test: name"
print "--------------------------"
result = mydb.dropField('colors', 'code')
print result

print ""
print "Rename test to colors:"
print "----------------------"

#result = mydb.renameTable('test', 'colors')
#print result
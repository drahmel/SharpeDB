#!/usr/bin/env python
# SharpeDB by Dan Rahmel
# (c) Copyright 2012 by Dan Rahmel
# Created March 26, 2012


COLOR_RED = '\033[0;31m'
COLOR_BLUE = '\033[0;34m'
COLOR_GREEN = '\033[0;32m'
COLOR_YELLOW = '\033[0;33m'
COLOR_END = '\033[m'

import resource, sys
import syslog
import datetime, time
import sqlite3 as lite
import json

def adapt_datetime(ts):
    return time.mktime(ts.timetuple())

lite.register_adapter(datetime.datetime, adapt_datetime)



class sharpedb():
	conn = None
	cursor = None
	
	def set(self,key,val,expire=None):
		# cur.execute("UPDATE Cars SET Price=? WHERE Id=?", (uPrice, uId)) 
		cursor = self.connect()
		if cursor!=None:
			sql = "INSERT INTO keyval1 (key,val) VALUES (?,?)"
			try:
				data = json.dumps(val, separators=(',',':'))
				cursor.execute(sql,(key,data))
			except lite.Error, e:
				if e.args[0] == "column key is not unique":
					existing = self.get(key)
					if type(existing) == dict and type(val) == dict:
						for i in val:
							existing[i] = val[i]
						val = existing
					data = json.dumps(val, separators=(',',':'))
					sql = "UPDATE keyval1 SET val = ? WHERE key = ?"
					#print sql,key," DATA:",data
					cursor.execute(sql,(data,key))
					
					#print "RC:",cursor.rowcount
				else:
					print "An error occurred:", e.args[0]
					
			#print "Inserted: "+key+" val:"+str(val)
		else:
			print "No cursor"
	def get(self,key):
		# cur.execute("UPDATE Cars SET Price=? WHERE Id=?", (uPrice, uId)) 
		cursor = self.connect()
		if cursor!=None:
			sql = "SELECT * FROM keyval1 WHERE key=?"
			cursor.execute(sql,(key,))
			data = cursor.fetchone()
			return json.loads(data[1])
		else:
			print "No cursor"
		
		
	def connect(self):

		con = None
		try:
			con = lite.connect('test.db',isolation_level=None)
			cur = con.cursor()	
			cur.execute('SELECT SQLITE_VERSION()')
			data = cur.fetchone()
			#print "SQLite version: %s" % data				
			with con:
				cur = con.cursor()
				try:
					cur.execute("CREATE TABLE keyval1(key TEXT PRIMARY KEY, val TEXT, expire INT)")
				except:
					pass
			
		except lite.Error, e:
			print "Error %s:" % e.args[0]
			sys.exit(1)
			
		finally:
			if con:
				self.conn = con
				self.cursor = cur
		return cur

if __name__ == "__main__":
	gor = sharpedb()
	key = "sixth"
	gor.set(key,{'shakes2':"spits skates"})
	print key,"=",gor.get(key)




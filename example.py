#!/usr/bin/env python3

#DB lib examples

import sys

#NOTE; If you are using the conda python installation, db lib is already in path.  
#Otherwise; include this:
#if('/ccg/src/db/' not in sys.path) : sys.path.append('/ccg/src/db/')


#Import and make readonly Connection
import db_utils.db_conn as db_conn
db=db_conn.RO()

#Examples:

#Pass select query using %s as place holder for parameterized query to sanitize inputs
a=db.doquery("select num as data_num, v.* from flask_data v where date > %s limit 10",('2016-01-01',))

print("""
---
Output using using parameterized where conditions
""")
if a :
    for row in a :
        print(("num: %s date: %s time: %s" % (row['data_num'],row['date'],row['time'])))



#Get single value directly by passing numRows=0
count=db.doquery("select count(*) from flask_event", numRows=0)
print("""
---
Output using numRows=0
""")
print("Number of events: ",str(count))



#Select into a dict of numpy col arrays by using form='numpy'
a=db.doquery("select data_num,ev_datetime,site,lat,lon from flask_data_view where site=%s limit 10",('MLO',),form='numpy')
print("""
---
Output using form='numpy'
""")
print(a['lat']) #this is a numpy.float64 array of lats
print(type(a['lat']))



#Or, to build programmatically with the query builder:

sql=db.sql #handle for the query builder. See bldsql.py for documentation.

showMet=True

sql.initQuery() #Resets/initializes
sql.table("flask_event e") #Set tables and columns separately
sql.col("e.num as event_num")
sql.innerJoin("gmd.site s on e.site_num=s.num") #Can use join syntax or regular where = joins
sql.col('s.code as site')
sql.orderby("s.code")
sql.limit(10)

sql.where("e.date>%s",('2015-01-01')) #binds parameters instead of passing text
sql.where("e.date<%s",('2016-01-01'))

#conditional branching
if(showMet):
    sql.leftJoin("flask_met met on e.num=met.event_num") #outer join
    sql.col("met.narr_spec_humidity")

#bind multiple
sites=('aao','crv','bld')
sql.wherein('s.code in', sites)

a=db.doquery() #returns list of dicts by default
print("""
---
Output using query builder
""")
for row in a:
    print("Event: %s site: %s shum: %s " % (row['event_num'],row['site'],row['narr_spec_humidity']))

a=db.doquery(outfile='/tmp/out.csv', form='csv') #write csv file



import pymssql
import time
import urllib2
from datetime import datetime

HOST = 'dcb-db-p03.genesispower.co.nz'
USER = 'GENESIS\zssrsuser'
PASS = '!OB+aabo@-AAk^DuW'
DBNAME = 'WRDB'

datetimestr = '%Y-%m-%dT%H:%M:%S'

TOPtenAPI = ''

conn = pymssql.connect(host=HOST, user=USER, password=PASS, database=DBNAME) # connecting to Web Report database

def gettopId(conn): # Method to get the record id number at the begining of the month
    date_time_now = datetime.strftime(datetime.now(),datetimestr) # Getting current date and time
    month_time = int(time.mktime(time.strptime(date_time_now,datetimestr)))-2592000 # Getting monthly epoch time 
    topid = None
    find_topid_sql = 'SELECT TOP 1 detail_record_id FROM scr_fct_exact_access WHERE seconds_since_epoch >= %s' % month_time
    with conn.cursor() as cursor:
	cursor.execute(find_topid_sql)
	topid = cursor.fetchone()
	cursor.close()
    return topid


def getdata(conn, topid=None): # Method to get the top 10 user who has the most virus
    fetch_data = 'SELECT scr_dim_user.user_name, COUNT(scr_fct_exact_access.virus_id) FROM scr_dim_user,scr_fct_exact_access WHERE scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.virus_id != 2 AND scr_fct_exact_access.detail_record_id > %s GROUP BY scr_dim_user.user_name ORDER BY COUNT(scr_fct_exact_access.virus_id) DESC' % topid
    data = list().__iter__
    with conn.cursor() as cursor:
	print fetch_data
	cursor.execute(fetch_data)
	data = cursor.fetchall()
	#print data
	cursor.close()
    yield data

if __name__ == '__main__':
    now = datetime.strftime(datetime.now(),"%Y-%m-%dT%H:%M:%S%Z") # Get current date and time
    topid = gettopId(conn) # Gete the value of the id number
    print('Top id: %s' % topid)
    data_top_user = getdata(conn,topid)
    for row in data_top_user:
	#print(row[0][0].next(),row[0][1].next())
       #	next(row)
	for line in row:
	    print(line[0],line[1])

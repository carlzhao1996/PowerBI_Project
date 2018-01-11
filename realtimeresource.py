import pymssql
import time
import urllib2
from datetime import datetime

HOST = ''
USER = ''
PASS = ''
DBNAME = ''

ResourceAPI = ''

datetimestr = '%Y-%m-%dT%H:%M:%S' # Date and time format

conn = pymssql.connect(host=HOST, user=USER, password=PASS, database=DBNAME) # connecting to Web Report database

def getdatetime(): # Method to get current date and time 
    date_time_now = datetime.strftime(datetime.now(),datetimestr)
    return date_time_now

def getepoch(date_time_now): # Method to convert current date and time to epoch format
   # date_time_now = datetime.strftime(datetime.now(),datetimestr)
    print date_time_now
    epoch = int(time.mktime(time.strptime(date_time_now,datetimestr)))-2629743
    print epoch
    return epoch

def getresource(conn,epoch=None): # Get the total using bytes and the bytes from server and bytes of client.
    fetch_resource = 'SELECT SUM(bytes), SUM(bytes_from_server), SUM(bytes_from_client) FROM scr_fct_exact_access WHERE seconds_since_epoch = %s' % epoch
    resource = list()
    with conn.cursor() as cursor:
	    print(fetch_resource)
	    cursor.execute(fetch_resource)
	    resource = cursor.fetchall()
	    print resource
	    cursor.close()
    return resource


if __name__ == '__main__':

    while True:
	date_time_now = getdatetime()
        epoch = getepoch(date_time_now)
	dataresource = getresource(conn,epoch)
        for row in dataresource:
	   # bytes_kb = row[0]/1024
	   # bytes_from_server_kb = row[1]/1024
	   # bytes_from_client_kb = row[2]/1024
	    print(row[0],row[1],row[2])
	    #row_input = '[{{"date":"{0}","bytes":"{1}","bytes_from_server":"{2}","bytes_from_client":"{3}"}}]'.format(date_time_now,bytes_kb,bytes_from_server_kb,bytes_from_client_kb)
	    row_input = '[{{"date":"{0}","bytes":"{1}","bytes_from_server":"{2}","bytes_from_client":"{3}"}}]'.format(date_time_now,row[0],row[1],row[2])
	   # print(bytes_kb,bytes_from_server_kb,bytes_from_client_kb)
	    req_resource = urllib2.Request(ResourceAPI,row_input)
	    response_resource = urllib2.urlopen(req_resource)
	    print("Response: HTTP {0} {1}\n".format(response_resource.getcode(),response_resource.read()))
	time.sleep(2)
    
    print('waiting for the next loop')  

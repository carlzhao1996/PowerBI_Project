import pymssql
import time
import urllib2
from datetime import datetime

HOST = ''
USER = ''
PASS = ''
DBNAME = ''

VirusMonitorAPI = ''
datetimestr = '%Y-%m-%dT%H:%M:%S' # Date and time format

conn = pymssql.connect(host=HOST, user=USER, password=PASS, database=DBNAME) # connecting to Web Report database

def getdatetime(): # Method to get current date and time 
    date_time_now = datetime.strftime(datetime.now(),datetimestr)
    print date_time_now
    return date_time_now

def getepoch(date_time_now):
   # date_time_now = datetime.strftime(datetime.now(),datetimestr)
    print date_time_now
    epoch = int(time.mktime(time.strptime(date_time_now,datetimestr)))-2629743
    print epoch
    return epoch

def requestmonitor(conn,epoch=None):
    fetch_data = "SELECT DATEADD(s,scr_fct_exact_access.seconds_since_epoch,'1970-01-01 00:00:00'), COUNT(scr_fct_exact_access.detail_record_id), scr_dim_virus.virus_name, scr_dim_user.user_name, scr_fct_exact_access.url FROM scr_fct_exact_access, scr_dim_virus, scr_dim_user WHERE scr_fct_exact_access.virus_id = scr_dim_virus.virus_id AND scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.seconds_since_epoch = %s GROUP BY scr_fct_exact_access.seconds_since_epoch, scr_dim_virus.virus_name, scr_dim_user.user_name, scr_fct_exact_access.url" % epoch
    data = list()
    with conn.cursor() as cursor:
	    print(fetch_data)
	    cursor.execute(fetch_data)
	    data = cursor.fetchall()
	    #print data
	    cursor.close()
    return data 

def virusmonitor(conn,epoch=None):
    fetch_data = "SELECT DATEADD(s,scr_fct_exact_access.seconds_since_epoch,'1970-01-01 00:00:00'), COUNT(scr_fct_exact_access.virus_id), scr_dim_virus.virus_name, scr_dim_user.user_name, scr_fct_exact_access.url FROM scr_fct_exact_access, scr_dim_virus, scr_dim_user WHERE scr_fct_exact_access.virus_id = scr_dim_virus.virus_id AND scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.virus_id !=2 AND scr_fct_exact_access.seconds_since_epoch = %s GROUP BY scr_fct_exact_access.seconds_since_epoch, scr_dim_virus.virus_name, scr_dim_user.user_name, scr_fct_exact_access.url" % epoch
    data_virus = list()
    with conn.cursor() as cursor:
            print(fetch_data)
            cursor.execute(fetch_data)
            data_virus = cursor.fetchall()
            cursor.close()
    return data_virus

if __name__ == '__main__':

    while True:
	    try:
       	          date_time_now = getdatetime()
           	  epoch = getepoch(date_time_now)
		  data_request = requestmonitor(conn,epoch)
		  for row_request in data_request:
           	      row_request_input = '[{{"datetime":"{0}","request":"{1}","virus_name":"{2}","user_name":"{3}","URL":"{4}"}}]'.format(row_request[0],row_request[1],row_request[2],row_request[3],row_request[4])
	   	      print(row_request[0],row_request[1],row_request[2],row_request[3],row_request[4])
           	      req_user_request = urllib2.Request(VirusMonitorAPI,row_request_input)
           	      response_req = urllib2.urlopen(req_user_request)
           	      print("Response: HTTP {0} {1}\n".format(response_req.getcode(),response_req.read()))
	           # print("************************************************************************")

       	          data_virus = virusmonitor(conn,epoch)
                  for row_virus in data_virus:
                      row_virus_input = '[{{"datetime":"{0}","virus_name":"{1}","user_name":"{2}","URL":"{3}","virus_num":{4}}}]'.format(row_virus[0],row_virus[2],row_virus[3],row_virus[4],row_virus[1])
                      print(row_virus[0],row_virus[1],row_virus[2],row_virus[3],row_virus[4])
                      req_virus_request = urllib2.Request(VirusMonitorAPI,row_virus_input)
                      response_virus = urllib2.urlopen(req_virus_request)
                      print("Response: HTTP {0} {1}\n".format(response_virus.getcode(),response_virus.read()))
        	  print("***********************************************************************")
	    except urllib2.HTTPError as e:
		    print("HTTP ERROR: {0}-{1}".format(e.code,e.reason))
		
	    except urllib2.URLError as e:
		    print("URL ERROR: {0}".format(e.reason))
	    except Exception as e:
   		    print("General Exception: {0}".format(e))
            print('waiting for the net loop') 
	    time.sleep(10) 

import pymssql
import time
import urllib2
from datetime import datetime

HOST = ''
USER = ''
PASS = ''
DBNAME = ''

datetimestr = '%Y-%m-%dT%H:%M:%S'

TOPtenAPI = ''
TotalVirusAPI = ''
VirusAmountAPI = ''
VirusDetailAPI = ''
VirusRecordAPI = ''

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
    data = list()
    with conn.cursor() as cursor:
	print fetch_data
	cursor.execute(fetch_data)
	data = cursor.fetchall()
	print data
	cursor.close()
    return data

def totalvirus(conn,topid=None): # Method to get the total virus number in a month
	fetch_totalNum = 'SELECT COUNT(scr_fct_exact_access.virus_id) FROM scr_fct_exact_access WHERE scr_fct_exact_access.virus_id != 2 AND scr_fct_exact_access.detail_record_id > %s' % topid
	with conn.cursor() as cursor:
	    print(fetch_totalNum)
	    cursor.execute(fetch_totalNum)
	    totalVirusNum = cursor.fetchone()
	    cursor.close()
	return totalVirusNum

def virusamount(conn,topid=None): # Method to get the virus amount of each virus group
	fetch_virusamount = 'SELECT scr_dim_virus.virus_name, COUNT(scr_fct_exact_access.virus_id) FROM scr_dim_virus, scr_fct_exact_access WHERE scr_fct_exact_access.virus_id = scr_dim_virus.virus_id AND scr_fct_exact_access.virus_id != 2 AND scr_fct_exact_access.detail_record_id > %s GROUP BY scr_dim_virus.virus_name ORDER BY COUNT(scr_fct_exact_access.virus_id) DESC' % topid
        virusamount_data = list()
	with conn.cursor() as cursor:
	    print fetch_virusamount
	    cursor.execute(fetch_virusamount)
	    virusamount_data = cursor.fetchall()
	    print virusamount_data
            cursor.close()
	    return virusamount_data

def virusdetail(conn,topid=None): # Method to get the virus detail of virus name, user name and url
	fetch_virusdetail = 'SELECT scr_dim_virus.virus_name, scr_dim_user.user_name,scr_fct_exact_access.url FROM scr_dim_virus, scr_dim_user, scr_fct_exact_access WHERE scr_fct_exact_access.virus_id = scr_dim_virus.virus_id AND scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.virus_id ! = 2 AND scr_fct_exact_access.detail_record_id > %s' % topid
	virusdetail_data = list()
	with conn.cursor() as cursor:
		print fetch_virusdetail
		cursor.execute(fetch_virusdetail)
		virusdetail_data = cursor.fetchall()
		print virusdetail_data
	        cursor.close()
		return virusdetail_data

def virusrecord(conn,topid=None):
	fetch_virusrecord = "SELECT COUNT(virus_id), DATEADD(s,seconds_since_epoch,'1970-01-01 00:00:00') FROM scr_fct_exact_access WHERE scr_fct_exact_access.detail_record_id > %s AND virus_id != 2 GROUP BY seconds_since_epoch ORDER BY seconds_since_epoch ASC" % topid
	virusrecord_data = list()
	with conn.cursor() as cursor:
		print fetch_virusrecord
		cursor.execute(fetch_virusrecord)
		virusrecord_data = cursor.fetchall()
		print virusrecord_data
		cursor.close()
		return virusrecord_data

if __name__ == '__main__':
    while True:
	now = datetime.strftime(datetime.now(),"%Y-%m-%dT%H:%M:%S%Z") # Get current date and time
	topid = gettopId(conn) # Gete the value of the id number
	print('Top id: %s' % topid)
	data_top_user = getdata(conn,topid) # get top 10 malisous users and the virus number
	for row in data_top_user:
	    inputdata = '[{{"timestamp":"{0}","Username":"{1}","VirusNum":"{2}"}}]'.format(now,row[0],row[1]) # Set the output data format as JSON
	    print(row[0],row[1])
	    req = urllib2.Request(TOPtenAPI,inputdata) # Write data into Power BI streaming dataset
	    response = urllib2.urlopen(req)
	    print("Response: HTTP {0} {1}\n".format(response.getcode(),response.read()))
	    # time.sleep(1)
	GetTotalVirusNum = totalvirus(conn,topid) # Calling method to get total virus number
	print GetTotalVirusNum
	input_totalVirus = '[{{"timestamp":"{0}","TotalVirusNum":"{1}"}}]'.format(now,GetTotalVirusNum[0]) # Set output data format as JSON
	req_totalVirus = urllib2.Request(TotalVirusAPI,input_totalVirus)
        response_totalVirus = urllib2.urlopen(req_totalVirus)
	print("Response: HTTP {0} {1}\n".format(response_totalVirus.getcode(),response_totalVirus.read()))

	GetVirusAmount = virusamount(conn,topid) # Calling Method to get the amount of each virus
        for virus_amount in GetVirusAmount:
            virus_amount_input = '[{{"VirusName":"{0}","VirusNumber":"{1}"}}]'.format(virus_amount[0],virus_amount[1])
            print(virus_amount[0],virus_amount[1])
            virusamount_req = urllib2.Request(VirusAmountAPI,virus_amount_input)
	    virusamount_response = urllib2.urlopen(virusamount_req)
	    print("Response: HTTP {0} {1}\n".format(virusamount_response.getcode(),virusamount_response.read()))
     
    	Getvirusrecord = virusrecord(conn,topid)
	for virus_record in Getvirusrecord:
	    timedata = datetime.strftime(virus_record[1],"%Y-%m-%dT%H:%M:%S%Z")
	    virus_record_input = '[{{"Virus_Num":"{0}","Date":"{1}"}}]'.format(virus_record[0],timedata)
	    print(virus_record[0],virus_record[1])
	    req_virus_record = urllib2.Request(VirusRecordAPI,virus_record_input)
	    response_virusrecord = urllib2.urlopen(req_virus_record)
	    print("Response: HTTP {0} {1}\n".format(response_virusrecord.getcode(),response_virusrecord.read()))

	Getvirusdetail = virusdetail(conn,topid)
	for detail in Getvirusdetail:
	    virus_detail_input = '[{{"Virus_name":"{0}","UserName":"{1}","URL":"{2}"}}]'.format(detail[0],detail[1],detail[2])
	    print(detail[0],detail[1],detail[2])
	    req_virusdetail = urllib2.Request(VirusDetailAPI,virus_detail_input)
	    response_virusdetail = urllib2.urlopen(req_virusdetail)
	    print("Response: HTTP {0} {1}\n".format(response_virusdetail.getcode(),response_virusdetail.read()))

	print('waiting for the next loop')
	time.sleep(10)

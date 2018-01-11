import pymssql
import time 
import urllib2
from datetime import datetime

HOST = ''
USER = ''
PASS = ''
DBNAME = ''

datetimestr = '%Y-%m-%dT%H:%M:%S'

UserResourceAPI = ''
TotalBytesAPI = ''
UrlResourceAPI = ''
ResourceDetailAPI = ''

conn = pymssql.connect(host=HOST, user=USER, password=PASS, database=DBNAME)

def gettopId(conn): # Method to get the record id number at the begining of the month
    date_time_now = datetime.strftime(datetime.now(),datetimestr)
    month_time = int(time.mktime(time.strptime(date_time_now,datetimestr)))-2592000
    topid = None
    find_topid_sql = 'SELECT TOP 1 detail_record_id FROM scr_fct_exact_access WHERE seconds_since_epoch >= %s' % month_time
    with conn.cursor() as cursor:
	cursor.execute(find_topid_sql)
	topid = cursor.fetchone()
	cursor.close()
    return topid

def getuserinfo(conn, topid=None): # Method to get top 10 user used the most resources
    fetch_user_data = 'SELECT TOP 10 scr_dim_user.user_name, scr_fct_exact_access.bytes FROM scr_dim_user,scr_fct_exact_access WHERE scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.detail_record_id > %s GROUP BY scr_dim_user.user_name, scr_fct_exact_access.bytes ORDER BY scr_fct_exact_access.bytes DESC' % topid
    userdata = list()
    with conn.cursor() as cursor:
	print fetch_user_data
	cursor.execute(fetch_user_data)
	userdata = cursor.fetchall()
	print userdata
	cursor.close()
    return userdata

def totalbytes(conn,topid=None): # Method to get total bytes for a month
	fetch_total_bytes = 'SELECT SUM(scr_fct_exact_access.bytes) FROM scr_fct_exact_access WHERE scr_fct_exact_access.detail_record_id > %s' % topid
	with conn.cursor() as cursor:
            print fetch_total_bytes
	    cursor.execute(fetch_total_bytes)
	    totalbytes = cursor.fetchone()
	    print totalbytes
	    cursor.close()
	return totalbytes

def urlresource(conn,topid=None): # Method to get top 10 URL used the most resource
	fetch_url_resource = 'SELECT TOP 10 url, bytes FROM scr_fct_exact_access WHERE scr_fct_exact_access.detail_record_id > %s ORDER BY bytes DESC' % topid
	url_resource = list()
	with conn.cursor() as cursor:
	    print fetch_url_resource
	    cursor.execute(fetch_url_resource)
	    url_resource = cursor.fetchall()
	    print url_resource
	    cursor.close()
	return url_resource

def resourcedetail(conn,topid=None): # Method to get top 10 user used the most resource and its url and resoure taken from the server
	fetch_resource_detail = "SELECT TOP 10 scr_dim_user.user_name, scr_fct_exact_access.bytes, scr_fct_exact_access.url, scr_fct_exact_access.bytes_from_client, scr_fct_exact_access.bytes_from_server FROM scr_dim_user, scr_fct_exact_access WHERE scr_fct_exact_access.user_id = scr_dim_user.user_id AND scr_fct_exact_access.detail_record_id > %s GROUP BY scr_dim_user.user_name, scr_fct_exact_access.bytes, scr_fct_exact_access.url, scr_fct_exact_access.bytes_from_client, scr_fct_exact_access.bytes_from_server, scr_fct_exact_access.seconds_since_epoch ORDER BY scr_fct_exact_access.bytes DESC" % topid
	resource_detail = list()
	with conn.cursor() as cursor:
	    print fetch_resource_detail
	    cursor.execute(fetch_resource_detail)
	    resource_detail = cursor.fetchall()
	    print resource_detail
	    cursor.close()
	return resource_detail

if __name__ == '__main__':
    while True:
	topid = gettopId(conn)
	print('Top id: %s' % topid)
	data_totalbytes = totalbytes(conn,topid)
	totalbytes_TB = data_totalbytes[0]/1099511627776
	totalbytes_input = '[{{"Bytes":"{0}"}}]'.format(totalbytes_TB)
	print data_totalbytes
	req_bytesinfo = urllib2.Request(TotalBytesAPI,totalbytes_input)
	response_bytesinfo = urllib2.urlopen(req_bytesinfo)
	print("Response: HTTP {0} {1}\n".format(response_bytesinfo.getcode(),response_bytesinfo.read()))

	data_urlresource = urlresource(conn,topid)
	for row in data_urlresource:
	    url_byte_GB = row[1]/1073741824
	    urlresource_input = '[{{"URL":"{0}","Bytes":"{1}"}}]'.format(row[0],url_byte_GB)
            print(row[0],row[1])
	    req_urlresource = urllib2.Request(UrlResourceAPI,urlresource_input)
	    response_urlresource = urllib2.urlopen(req_urlresource)
	    print("Response: HTTP {0} {1}\n".format(response_urlresource.getcode(),response_urlresource.read()))

	data_user_info = getuserinfo(conn,topid)
	for row in data_user_info:
	    user_byte_GB = row[1]/1073741824
	    user_info_input = '[{{"UserName":"{0}","Bytes":"{1}"}}]'.format(row[0],user_byte_GB)
	    print(row[0],row[1])
	    req_userinfo = urllib2.Request(UserResourceAPI,user_info_input)
	    response_userinfo = urllib2.urlopen(req_userinfo)
	    print("Response: HTTP {0} {1}\n".format(response_userinfo.getcode(),response_userinfo.read()))

	data_resource_detail = resourcedetail(conn,topid)
	for row in data_resource_detail:
	    bytes_GB = row[1]/1073741824
	    bytes_client_GB = row[3]/1073741824
	    bytes_server_GB = row[4]/1073741824
	    #date_time = datetime.strftime(row[5],"%Y-%m-%dT%H:%M:%S%Z")
	    resource_detail_input = '[{{"User_Name":"{0}","Bytes":"{1}","URL":"{2}","Bytes_Client":"{3}","Bytes_Server":"{4}"}}]'.format(row[0],bytes_GB,row[2],bytes_client_GB,bytes_server_GB)
	    print(row[0],row[1],row[2],row[3],row[4])
	    req_resourcedetail = urllib2.Request(ResourceDetailAPI,resource_detail_input)
	    response_resourcedetail = urllib2.urlopen(req_resourcedetail)
	    print("Response: HTTP {0} {1}\n".format(response_resourcedetail.getcode(),response_resourcedetail.read()))


	print('watiing for the next loop')
	time.sleep(10)

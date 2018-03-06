# PowerBI Realtime Security Dashboard
The Python program of this project is to gaining the data from Web Report Database (SQL Database) and write to PowerBI streaming dataset by calling PowerBI Straming data API and therefore build visulization.

## Getting Started
The project includes 4 Python files and 1 linux bash file as main program file. The running of the bash file can be able to run all the python files simlusiously to achieve data processing. 

### Prerequisites
1. Linux environment
2. Python environment
3. Python 3 
4. Python library include: pymssql and urllib2


### Installing
1. Install python environment by using 'sudo apt-get instll python'
2. Install Python library pymssql and urllib2 by either 'pip install pymssql', 'pip install urllib2' or 'easy-install pymssql', 'easy-install urllib2'


## Running the tests
1. Go to the directory of '/powerbi_project'
2. Run the bash file as './main.sh'

The program will automically run all the script simulusiously in the background and then process the data.
The python code will be able to firstly get current date and time and then translate to epoch time.
After that, connections to Web Report database will be created and then execute the SQL query.
The query will be able to gain the monthly data and current realtime data based on currrent date and time.
Finally, the data output by the using query will be sent to power BI streaming dataset by calling PowerBI Stremaing data API. 


### Break down into end to end tests
Testing of monthly virus analysis:
The testing should be run as 'python virusinfo.py'
The testing output of the monthly virus analysis include the id value return based on the time, result of all the malicious user and their virus count, total virus result, virus count of each virus name, all the virus detail information and the http response of the using API. 

Testing of monthly internet resource using analysis:
The testing should be run as 'python resourceinfo.py'
The testing output of the monthly resource information include the id value return based on the time, result of top 10 users used the most bytes and the byte data, total resource used within a month, top 10 URLs used the most resource, detail information of the using result and the http response of the using API.

Testing of virus monitor analysis:
The testing should be run as 'python realtimevirus.py' 
The testing output of the virus monitor analysis include current date and time, realtime resource of user's request count, virus name, user name, URL link and http response. 

Testing of real time resource monitoring:
The testing should be run as 'python realtimeresource.py'
The testing output of the real time resource monitor include current date and time, total using bytes, user using bytes, server using bytes and http response.


## Built With

*[pymssql](http://pymssql.org/en/stable/) - Python library for SQL server database connection
*[urllib2](https://docs.python.org/2/library/urllib2.html) - Python Library for API Web Request
*[PowerBI Streaming data API](https://powerbi.microsoft.com/en-us/documentation/powerbi-service-real-time-streaming/) - Power BI streaming Data API turorial


## Authors

* **Carl Zhao** - *Initial work* 




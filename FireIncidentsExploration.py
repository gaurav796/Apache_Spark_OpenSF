# Exploring the City of San Francisco public data with Apache Spark 2.0
# There are **2 APIs** for interacting with Spark:
# **DataFrames/SQL/Datasets:** general, higher level API for users of Spark
# **RDD:** a lower level API for spark internals and advanced programming
# Introduction to Fire Department Calls for Service
# Note, you can also access the 1.6 GB of data directly from sfgov.org via this link: https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3

# Below commands are in iterative manner which I have used using **Pyspark** session in local mode

#The entry point into all functionality in Spark 2.0 is the new SparkSession class:
# **Analysis with PySpark DataFrames API**
spark
<pyspark.sql.session.SparkSession object at 0x0000000005C0FC88>
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

#Using the SparkSession, create a DataFrame from the CSV file by inferring the schema:

df = spark.read.csv('/mnt/sf_open_data/fire_dept_calls_for_service/Fire_Department_Calls_for_Service.csv', header=True, inferSchema=True)
df
DataFrame[Call Number: int, Unit ID: string, Incident Number: int, Call Type: string, Call Date: string, Watch Date: string, Received DtTm: string, Entry DtTm: string, Dispatch DtTm: string, Response DtTm: string, On Scene DtTm: string, Transport DtTm: string, Hospital DtTm: string, Call Final Disposition: string, Available DtTm: string, Address: string, City: string, Zipcode of Incident: int, Battalion: string, Station Area: string, Box: string, Original Priority: string, Priority: string, Final Priority: int, ALS Unit: boolean, Call Type Group: string, Number of Alarms: int, Unit Type: string, Unit sequence in call dispatch: int, Fire Prevention District: string, Supervisor District: string, Neighborhooods - Analysis Boundaries: string, Location: string, RowID: string]

#Notice that no job is run this time because there was no action triggered

#Also Notice that the above cell takes ~15 seconds to run b/c it is inferring the schema by sampling the file and reading through it.

# Look at the first 5 records in the DataFrame:

display(df.limit(5))

#Print just the column names in the DataFrame:

df.columns

#Count how many rows total there are in DataFrame:

df.count()

# There are over 4 million rows in the DataFrame and it takes ~14 seconds to do a full read of it.

# Open the Apache Spark 2.0 early release documentation in new tabs, so you can easily reference the API guide:
# 1) Spark 2.0 preview docs: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/
# 2) DataFrame user documentation: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/sql-programming-guide.html 
# 3) PySpark API 2.0 docs: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/api/python/index.html

# DataFrames support two types of operations: *transformations* and *actions*.
# Transformations, like `select()` or `filter()` create a new DataFrame from an existing one. 
# Transformations contribute to a query plan,  but  nothing is executed until an action is called.
# Actions, like `show()` or `count()`, return a value with results to the user. Other actions like `save()` write the DataFrame to distributed storage (like S3 or HDFS).


# **Q-1) How many different types of calls were made to the Fire Department?**

# Use the .select() transformation to yank out just the 'Call Type' column, then call the show action
>>> df.select('Call Type').show(5)
+----------------+
|       Call Type|
+----------------+
|Medical Incident|
|Medical Incident|
|Medical Incident|
|          Alarms|
|Medical Incident|
+----------------+
only showing top 5 rows

#Now I need to know all call types over all 4 Million data
# Add the .distinct() transformation to keep only distinct rows
# The False below expands the ASCII column width to fit the full text in the output

df.select('Call Type').distinct().show(35, False)
>>> df.select('Call Type').distinct().show(35, False)
+--------------------------------------------+
|Call Type                                   |
+--------------------------------------------+
|Elevator / Escalator Rescue                 |
|Marine Fire                                 |
|Aircraft Emergency                          |
|Confined Space / Structure Collapse         |
|Administrative                              |
|Alarms                                      |
|Odor (Strange / Unknown)                    |
|Lightning Strike (Investigation)            |
|Citizen Assist / Service Call               |
|HazMat                                      |
|Watercraft in Distress                      |
|Explosion                                   |
|Oil Spill                                   |
|Vehicle Fire                                |
|Suspicious Package                          |
|Train / Rail Fire                           |
|Extrication / Entrapped (Machinery, Vehicle)|
|Other                                       |
|Outside Fire                                |
|Traffic Collision                           |
|Assist Police                               |
|Gas Leak (Natural and LP Gases)             |
|Water Rescue                                |
|Electrical Hazard                           |
|High Angle Rescue                           |
|Structure Fire                              |
|Industrial Accidents                        |
|Medical Incident                            |
|Mutual Aid / Assist Outside Agency          |
|Fuel Spill                                  |
|Smoke Investigation (Outside)               |
|Train / Rail Incident                       |
+--------------------------------------------+

# **Q-2) How many incidents of each call type were there?**
#Note that .count() is actually a transformation here
 df.select('Call Type').groupBy('Call Type').count().orderBy("count", ascending=False).show(100)
+--------------------+-------+
|           Call Type|  count|
+--------------------+-------+
|    Medical Incident|2901390|
|      Structure Fire| 598794|
|              Alarms| 478487|
|   Traffic Collision| 183284|
|               Other|  72516|
|Citizen Assist / ...|  67921|
|        Outside Fire|  52227|
|        Vehicle Fire|  22025|
|        Water Rescue|  21427|
|Gas Leak (Natural...|  16389|
|   Electrical Hazard|  12529|
|Odor (Strange / U...|  12225|
|Elevator / Escala...|  11701|
|Smoke Investigati...|   9813|
|          Fuel Spill|   5275|
|              HazMat|   3768|
|Industrial Accidents|   2776|
|           Explosion|   2513|
|  Aircraft Emergency|   1511|
|       Assist Police|   1299|
|   High Angle Rescue|   1147|
|Train / Rail Inci...|   1121|
|Watercraft in Dis...|    884|
|Extrication / Ent...|    665|
|           Oil Spill|    516|
|Confined Space / ...|    467|
|Mutual Aid / Assi...|    422|
|         Marine Fire|    356|
|  Suspicious Package|    295|
|      Administrative|    262|
|   Train / Rail Fire|     10|
|Lightning Strike ...|      9|
+--------------------+-------+
#Seems like the SF Fire department is called for medical incidents far more than any other type. Note that the above command took about 14 seconds to execute. In an upcoming section, we'll cache the data into memory for up to 100x speed increases.
       
       
# ***Doing Date/Time Analysis*** 
# **Q-3) How many years of Fire Service Calls is in the data file?**
  
# Notice that the date or time columns are currently being interpreted as strings, rather than date or time objects:      
>>> df.printSchema()
root
 |-- Call Number: integer (nullable = true)
 |-- Unit ID: string (nullable = true)
 |-- Incident Number: integer (nullable = true)
 |-- Call Type: string (nullable = true)
 |-- Call Date: string (nullable = true)
 |-- Watch Date: string (nullable = true)
 |-- Received DtTm: string (nullable = true)
 |-- Entry DtTm: string (nullable = true)
 |-- Dispatch DtTm: string (nullable = true)
 |-- Response DtTm: string (nullable = true)
 |-- On Scene DtTm: string (nullable = true)
 |-- Transport DtTm: string (nullable = true)
 |-- Hospital DtTm: string (nullable = true)
 |-- Call Final Disposition: string (nullable = true)
 |-- Available DtTm: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Zipcode of Incident: integer (nullable = true)
 |-- Battalion: string (nullable = true)
 |-- Station Area: string (nullable = true)
 |-- Box: string (nullable = true)
 |-- Original Priority: string (nullable = true)
 |-- Priority: string (nullable = true)
 |-- Final Priority: integer (nullable = true)
 |-- ALS Unit: boolean (nullable = true)
 |-- Call Type Group: string (nullable = true)
 |-- Number of Alarms: integer (nullable = true)
 |-- Unit Type: string (nullable = true)
 |-- Unit sequence in call dispatch: integer (nullable = true)
 |-- Fire Prevention District: string (nullable = true)
 |-- Supervisor District: string (nullable = true)
 |-- Neighborhooods - Analysis Boundaries: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- RowID: string (nullable = true)       


# Let's use the unix_timestamp() function to convert the string into a timestamp:
from pyspark.sql.functions import *
# Note that PySpark uses the Java Simple Date Format patterns

from_pattern1 = 'MM/dd/yyyy'
to_pattern1 = 'yyyy-MM-dd'

from_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'
to_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'


df = df \
  .withColumn('CallDateTS', unix_timestamp(df['Call Date'], from_pattern1).cast("timestamp")) \
  .drop('CallDate') \
  .withColumn('WatchDateTS', unix_timestamp(df['Watch Date'], from_pattern1).cast("timestamp")) \
  .drop('WatchDate') \
  .withColumn('ReceivedDtTmTS', unix_timestamp(df['ReceivedDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ReceivedDtTm') \
  .withColumn('EntryDtTmTS', unix_timestamp(df['EntryDtTm'], from_pattern2).cast("timestamp")) \
  .drop('EntryDtTm') \
  .withColumn('DispatchDtTmTS', unix_timestamp(df['DispatchDtTm'], from_pattern2).cast("timestamp")) \
  .drop('DispatchDtTm') \
  .withColumn('ResponseDtTmTS', unix_timestamp(df['ResponseDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ResponseDtTm') \
  .withColumn('OnSceneDtTmTS', unix_timestamp(df['OnSceneDtTm'], from_pattern2).cast("timestamp")) \
  .drop('OnSceneDtTm') \
  .withColumn('TransportDtTmTS', unix_timestamp(df['TransportDtTm'], from_pattern2).cast("timestamp")) \
  .drop('TransportDtTm') \
  .withColumn('HospitalDtTmTS', unix_timestamp(df['HospitalDtTm'], from_pattern2).cast("timestamp")) \
  .drop('HospitalDtTm') \
  .withColumn('AvailableDtTmTS', unix_timestamp(df['AvailableDtTm'], from_pattern2).cast("timestamp")) \
  .drop('AvailableDtTm')  

df.printSchema()

# Notice that the formatting of the timestamps is now different:
df.limit(5).show()

# Finally calculate how many distinct years of data is in the CSV file:
df.select(year('CallDateTS')).distinct().orderBy('year(CallDateTS)').show()

 +----------------+  
|year(CallDateTS)|  
+----------------+  
|            2000|  
|            2001|  
|            2002|  
|            2003|  
|            2004|  
|            2005|  
|            2006|  
|            2007|  
|            2008|  
|            2009|  
|            2010|  
|            2011|  
|            2012|  
|            2013|  
|            2014|  
|            2015|  
|            2016|  
|            2017|  
+----------------+  
       
# **Q-4) How many service calls were logged in the last 7 days?**

# suppose today,is the 360  day of the year. Filter the DF down to just 2016 and days of year greater than 360:
 
df.filter(year('CallDateTS') == '2016').filter(dayofyear('CallDateTS') >= 360).select(dayofyear('CallDateTS')).distinct().orderBy('dayofyear(CallDateTS)').show()
+---------------------+  
|dayofyear(CallDateTS)|  
+---------------------+  
|                  360|  
|                  361|  
|                  362|  
|                  363|  
|                  364|  
|                  365|  
|                  366|  
+---------------------+    
       
df.filter(year('CallDateTS') == '2016').filter(dayofyear('CallDateTS') >= 360).groupBy(dayofyear('CallDateTS')).count().orderBy('dayofyear(CallDateTS)').show()
+---------------------+-----+
|dayofyear(CallDateTS)|count|
+---------------------+-----+
|                  360|  719|
|                  361|  745|
|                  362|  767|
|                  363|  884|
|                  364|  829|
|                  365|  912|
|                  366|  852|
+---------------------+-----+
       
# %html <h1> ***Memory, Caching and write to Parquet*** </h1>



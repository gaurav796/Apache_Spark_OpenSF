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
df = spark.read.csv('C:\Users\Gaurav\Downloads\Fire_Department_Calls_for_Service.csv',header=True,inferSchema=True)
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
#Seems like the SF Fire department is called for medical incidents far more than any other type. Note that the above command took about 14 seconds to execute. 
#In an upcoming section, we'll cache the data into memory for up to 100x speed increases.
       
       
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
       
# ***Memory, Caching and write to Parquet*** 
# The DataFrame is currently comprised of 13 partitions:     
df.rdd.getNumPartitions()  
       
# We can reparttion to any other number for example to 6 instead of default 13, which 'll be optimal for processing 
#and create a temp view out of df
df.repartition(6).createOrReplaceTempView("dfVIEW");
spark.catalog.cacheTable("dfVIEW")
# cache() is lazy operation, so no Spark job will run
       
# Call .count() to materialize the cache
spark.table("dfVIEW").count()
# It will first read files from disk. Spark reads maximum of 128 MB at a partition, hence 1.6BB file is divided in 13 partitions.
       # as 1600*1024/ 128 = 12.8
# After cache(), file will be saved in memory in compressed format. Spark uses Tungston Binary formar and file is 
# converted to collumnar compress data in memory.
# for example 1.6 GB files will utilize only 620 MB in memory to store.

# *** to utilize cached data ***
# we will create a dataframe from cached table
dfcached= spark.table("dfVIEW")
dfcached.count()
# NOTE that the full scan + count in memory takes < 1 second!, previoulsy it took 20 seconds to read file from disk
# The 6 partitions are now cached in memory:  
# Use the Spark UI to see the 6 partitions in memory: under Storage section
# Now that our data has the correct date types for each column and it is correctly partitioned, let's write it down as a parquet file for future loading:
# Parquet are much faster than CSV and JSON to read data        
df.write.format('parquet').save('C:\Users\Gaurav\Downloads\fireServiceParquet\')
# Now the directory should contain 6 .gz compressed Parquet files (one for each partition):
                                
# Here's how you can easily read the parquet file from S3 in the future:
tempDF = spark.read.parquet('C:\Users\Gaurav\Downloads\fireServiceParquet\')
tempDF.count()
display(tempDF.limit(5))

                            
# ***SQL Queries ***
# because I am working on local laptop, hence I can't directly run SQL command, but I need to use with sparkSQL                        
spark.sql("SELECT count(*) FROM dfVIEW").show()  
                            
                            
#If you look at Spark UI then you will see 3 stages , 1 skipped so only 2 stages ran.
#2 stages have 7 tasks which were launched to run the count... 6 tasks to reach the data from each of the 6 partitions 
# and do a pre-aggregation on each partition, then a final task to aggregate the count from all 6 tasks: 
#Result of first stage is input to next stage , as those 6 task result in 354KB which is input for next task.
                            
      
                            
# **Q-5) Which neighborhood in SF generated the most calls last year?**
 
spark.sql("SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM dfVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15");
     
# note that last stage uses 200 partitions! This is by default which is non-optimal, given that we only have ~1.6 GB of data and 3 slots.
# Change the shuffle.partitions option to 6:
# this SETTING means that anytime a shuffle happens, no of partitions which should be in dataframe is 200 by default.  
                                                
spark.conf.get("spark.sql.shuffle.partitions")
#which is 200
spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.get("spark.sql.shuffle.partitions")
# now its is 6

# Re-run the same SQL query and notice the speed increase:

spark.sql("SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM dfVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15");

# SQL also has some handy commands like `DESC` (describe) to see the schema + data types for the table:

# COMMAND ----------

 spark.sql("DESC dfVIEW")
                          
# ***Spark Internals and SQL UI***
# Catalyst Optimizer: which will take SQL query, or dataframe / dataset and generates logical plan, flows through logical & physical optimization
# and finally generates RDD and runs code
                            
# Note that a SQL Query just returns back a DataFrame
spark.sql("SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM dfVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15")

# The `explain()` method can be called on a DataFrame to understand its logical + physical plans:

spark.sql("SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM dfVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15").explain(True)
# you can see same plan under sparkUI, SQL section    
                            
# ***DataFrame Joins                         
                            
# **Q-6) What was the primary non-medical reason most people called the fire department from the Tenderloin last year?**

# The "Fire Incidents" data (another dataset )includes a summary of each (non-medical) incident to which the SF Fire Department responded.
#  https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric
# Let's do a join to the Fire Incidents data on the "Incident Number" column:

#  Read the Fire Incidents CSV file into a DataFrame: and cache it 

incidentsDF = spark.read.csv('/mnt/sf_open_data/fire_incidents/Fire_Incidents.csv', header=True, inferSchema=True).withColumnRenamed('Incident Number', 'IncidentNumber').cache()
incidentsDF.printSchema()

# Materialize the cache
incidentsDF.count()
display(incidentsDF.limit(3))

# JOIN both dataset based on common keys

joinedDF = fireServiceDF.join(incidentsDF, fireServiceDF.IncidentNumber == incidentsDF.IncidentNumber)
display(joinedDF.limit(3))

#Note that the joined DF is only 1.1 million rows b/c we did an inner join (the original Fire Service Calls data had 4+ million rows)
joinedDF.count()
joinedDF.filter(year('CallDateTS') == '2015').filter(col('NeighborhoodDistrict') == 'Tenderloin').count()
joinedDF.filter(year('CallDateTS') == '2015').filter(col('NeighborhoodDistrict') == 'Pacific Heights').count()
display(joinedDF.filter(year('CallDateTS') == '2015').filter(col('NeighborhoodDistrict') == 'Tenderloin').groupBy('Primary Situation').count().orderBy(desc("count")).limit(10))
display(joinedDF.filter(year('CallDateTS') == '2015').filter(col('NeighborhoodDistrict') == 'Pacific Heights').groupBy('Primary Situation').count().orderBy(desc("count")).limit(10))

# Most of the calls were False Alarms!

# What do residents of Russian Hill call the fire department for?

display(joinedDF.filter(year('CallDateTS') == '2015').filter(col('NeighborhoodDistrict') == 'Russian Hill').groupBy('Primary Situation').count().orderBy(desc("count")).limit(10))

# ***Convert a Spark DataFrame to a Pandas DataFrame ***

import pandas as pd

pandas2016DF = joinedDF.filter(year('CallDateTS') == '2016').toPandas()
pandas2016DF.dtypes
pandas2016DF.head()
pandas2016DF.describe()                            
                            

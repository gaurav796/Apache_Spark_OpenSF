# Exploring the City of San Francisco public data with Apache Spark 2.0
# There are **2 APIs** for interacting with Spark:
# **DataFrames/SQL/Datasets:** general, higher level API for users of Spark
# **RDD:** a lower level API for spark internals and advanced programming
# Introduction to Fire Department Calls for Service
# Note, you can also access the 1.6 GB of data directly from sfgov.org via this link: https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3

# Below commands are in iterative manner which I have used using **Pyspark** session in local mode

#The entry point into all functionality in Spark 2.0 is the new SparkSession class:

spark
<pyspark.sql.session.SparkSession object at 0x0000000005C0FC88>


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









# OpenSF-Apache-Spark

### ![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png) + ![SF Open Data Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/logo_sfopendata.png)

# Exploring the City of San Francisco public data with Apache Spark 2.0

![Fireworks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/fireworks.png)

The SF OpenData project was launched in 2009 and contains hundreds of datasets from the city and county of San Francisco. 
Open government data has the potential to increase the quality of life for residents, create more efficient government services, 
better public decisions, and even new local businesses and services.

APACHE SPARK:

Spark is a unified processing engine that can analyze big data using SQL, machine learning, graph processing or real time stream analysis:

![Spark Engines](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_4engines.png)

I will mostly focus on Spark SQL and DataFrames in this exercise.
Spark can read from many different databases and file systems and run in various environments:

![Spark Goal](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_goal.png)

Although Spark supports four languages (Scala, Java, Python, R), tonight we will use Python. 
Broadly speaking, there are 2 APIs for interacting with Spark:

DataFrames/SQL/Datasets: general, higher level API for users of Spark
RDD: a lower level API for spark internals and advanced programming

A Spark cluster is made of one Driver and many Executor JVMs (java virtual machines):
![Spark_Architecture](https://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)

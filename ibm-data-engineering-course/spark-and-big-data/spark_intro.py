# Installing required packages
# !pip install pyspark
# !pip install findspark

import findspark
findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the spark context. 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# To work with dataframes we just need to verify that the spark session instance has been created.
if 'spark' in locals() and isinstance(spark, SparkSession):
    print("SparkSession is active and ready to use.")
else:
    print("SparkSession is not active. Please create a SparkSession.")

# In this exercise we work with Resilient Distributed Datasets (RDDs). 
# RDDs are Spark's primitive data abstraction and we use concepts from functional programming to create and manipulate RDDs.

data = range(1,30)
# print first element of iterator
print(data[0])
len(data)
xrangeRDD = sc.parallelize(data, 4)

# this will let us know that we created an RDD
xrangeRDD

# Sample output of the above
"""
1
range(1, 30)
PythonRDD[9] at RDD at PythonRDD.scala:53
"""

""" A transformation is an operation on an RDD that results in a new RDD.
The transformed RDD is generated rapidly because the new RDD is lazily evaluated, 
which means that the calculation is not carried out when the new RDD is generated. 
The RDD will contain a series of transformations, or computation instructions, 
that will only be carried out when an action is called. 
In this transformation, we reduce each element in the RDD by 1. 
Note the use of the lambda function. We also then filter the RDD to only contain elements <10. """
subRDD = xrangeRDD.map(lambda x: x-1) 
filteredRDD = subRDD.filter(lambda x : x<10)

# A transformation returns a result to the driver. 
# We now apply the collect() action to get the output from the transformation.
print(filteredRDD.collect())
filteredRDD.count()

# Sample output of the above
"""
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
10
"""

# This simple example shows how to create an RDD and cache it. 
# Notice the 10x speed improvement! If you wish to see the actual computation time, browse to the Spark UI...it's at host:4040. 
# You'll see that the second calculation took much less time!

import time 

test = sc.parallelize(range(1,50000),4)
test.cache()

t1 = time.time()
# first count will trigger evaluation of count *and* cache
count1 = test.count()
dt1 = time.time() - t1
print("dt1: ", dt1)


t2 = time.time()
# second count operates on cached data only
count2 = test.count()
dt2 = time.time() - t2
print("dt2: ", dt2)

#test.count()
# Sample output of the above:
"""
dt1:  1.1817021369934082
dt2:  0.3986999988555908
"""

# --- CREATE YOUR FIRST DATAFRAME DEMO --- 
# Download the data first into a local `people.json` file
# !curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json >> people.json

# Read the dataset into a spark dataframe using the `read.json()` function
df = spark.read.json("people.json").cache()
# Print the dataframe as well as the data schema
df.show()
df.printSchema()

# Register the DataFrame as a SQL temporary view
df.createTempView("people")

# Select and show basic data columns

df.select("name").show()
df.select(df["name"]).show()
spark.sql("SELECT name FROM people").show()

# Perform basic filtering

df.filter(df["age"] > 21).show()
spark.sql("SELECT age, name FROM people WHERE age > 21").show()

# Perfom basic aggregation of data

df.groupBy("age").count().show()
spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()

# starter code
numbers = range(1, 50)
numbers_RDD = sc.parallelize(numbers, 4)
even_numbers_RDD = numbers_RDD.map(lambda x: x * 2)

# starter code
# !curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/people2.json >> people2.json
df = spark.read.json("people2.json")
df.createTempView("people2")
result = spark.sql("SELECT AVG(age) from people2")
result.show()

# Close the spark session
spark.stop() 
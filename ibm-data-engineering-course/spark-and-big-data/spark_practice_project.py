# Installing required packages

#!pip install wget pyspark  findspark

import findspark

findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession

# Creating a SparkContext object

sc = SparkContext.getOrCreate()

# Creating a Spark Session

spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


import wget

link_to_data1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv'
wget.download(link_to_data1)

link_to_data2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv'
wget.download(link_to_data2)

#load the data into a pyspark dataframe
    
df1 = spark.read.csv("dataset1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("dataset2.csv", header=True, inferSchema=True)

#print the schema of df1 and df2
    
df1.printSchema()
df2.printSchema()

"""
root
 |-- customer_id: integer (nullable = true)
 |-- date_column: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- description: string (nullable = true)
 |-- location: string (nullable = true)

root
 |-- customer_id: integer (nullable = true)
 |-- transaction_date: string (nullable = true)
 |-- value: integer (nullable = true)
 |-- notes: string (nullable = true)
"""

from pyspark.sql.functions import year, quarter, to_date

#Add new column year to df1
df1 = df1.withColumn('year', year(to_date('date_column','dd/MM/yyyy')))

#Add new column quarter to df2    
df2 = df2.withColumn('quarter', quarter(to_date('transaction_date','dd/MM/yyyy')))

#Rename df1 column amount to transaction_amount
df1 = df1.withColumnRenamed('amount', 'transaction_amount')
    
#Rename df2 column value to transaction_value
df2 = df2.withColumnRenamed('value', 'transaction_value')

#Drop columns description and location from df1
df1 = df1.drop('description', 'location')
    
#Drop column notes from df2
df2 = df2.drop('notes')

#join df1 and df2 based on common column customer_id
joined_df = df1.join(df2, 'customer_id', 'inner')
#if column names does not match then: df1.join(df2, df1.id == df2.employee_id)

# filter the dataframe for transaction amount > 1000
filtered_df = joined_df.filter("transaction_amount > 1000") 

from pyspark.sql.functions import sum
   
# group by customer_id and aggregate the sum of transaction amount

total_amount_per_customer = filtered_df.groupBy('customer_id').agg(sum('transaction_amount').alias('total_amount'))

#display the result
total_amount_per_customer.show()

"""
+-----------+------------+
|customer_id|total_amount|
+-----------+------------+
|         31|        3200|
|         85|        1800|
|         78|        1500|
|         34|        1200|
|         81|        5500|
|         28|        2600|
|         76|        2600|
|         27|        4200|
|         91|        3200|
|         22|        1200|
|         93|        5500|
|          1|        5000|
|         52|        2600|
|         13|        4800|
|          6|        4500|
|         16|        2600|
|         40|        2600|
|         94|        1200|
|         57|        5500|
|         54|        1500|
+-----------+------------+
only showing top 20 rows
"""

# Write total_amount_per_customer to a Hive table named customer_totals
total_amount_per_customer.write.mode("overwrite").saveAsTable("customer_totals")

#Write filtered_df to HDFS in parquet format file filtered_data.parquet
filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")

from pyspark.sql.functions import when, lit

# Add new column with value indicating whether transaction amount is > 5000 or not
df1 = df1.withColumn("high_value", when(df1.transaction_amount > 5000, lit("Yes")).otherwise(lit("No")))

from pyspark.sql.functions import avg

#calculate the average transaction value for each quarter in df2
average_value_per_quarter = df2.groupBy('quarter').agg(avg("transaction_value").alias("avg_trans_val"))
    
#show the average transaction value for each quarter in df2    
average_value_per_quarter.show()

"""
+-------+------------------+
|quarter|     avg_trans_val|
+-------+------------------+
|      1| 1111.111111111111|
|      3|1958.3333333333333|
|      4| 816.6666666666666|
|      2|            1072.0|
+-------+------------------+
"""

#Write average_value_per_quarter to a Hive table named quarterly_averages

average_value_per_quarter.write.mode("overwrite").saveAsTable("quarterly_averages")

# calculate the total transaction value for each year in df1.
total_value_per_year = df1.groupBy('year').agg(sum("transaction_amount").alias("total_transaction_val"))

# show the total transaction value for each year in df1.
total_value_per_year.show()

"""
+----+---------------------+
|year|total_transaction_val|
+----+---------------------+
|2025|                25700|
|2027|                25700|
|2023|                28100|
|2022|                29800|
|2026|                25700|
|2029|                25700|
|2030|                 9500|
|2028|                25700|
|2024|                25700|
+----+---------------------+
"""

#Write total_value_per_year to HDFS in the CSV format

total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")
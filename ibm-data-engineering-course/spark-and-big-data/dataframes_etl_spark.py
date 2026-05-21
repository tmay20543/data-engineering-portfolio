# Installing required packages
# !pip install pyspark
# !pip install findspark
# !pip install pandas

import findspark
findspark.init()
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

# Preview a few records
mtcars.head()
# Output of line 23:
"""
	Unnamed: 0	mpg	cyl	disp	hp	drat	wt	qsec	vs	am	gear	carb
0	Mazda RX4	21.0	6	160.0	110	3.90	2.620	16.46	0	1	4	4
1	Mazda RX4 Wag	21.0	6	160.0	110	3.90	2.875	17.02	0	1	4	4
2	Datsun 710	22.8	4	108.0	93	3.85	2.320	18.61	1	1	4	1
3	Hornet 4 Drive	21.4	6	258.0	110	3.08	3.215	19.44	1	0	3	1
4	Hornet Sportabout	18.7	8	360.0	175	3.15	3.440	17.02	0	0	3	2
"""

# We use the `createDataFrame` function to load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars) 

# Let us look at the schema of the loaded spark dataframe
sdf.printSchema()
"""
root
 |-- Unnamed: 0: string (nullable = true)
 |-- mpg: double (nullable = true)
 |-- cyl: long (nullable = true)
 |-- disp: double (nullable = true)
 |-- hp: long (nullable = true)
 |-- drat: double (nullable = true)
 |-- wt: double (nullable = true)
 |-- qsec: double (nullable = true)
 |-- vs: long (nullable = true)
 |-- am: long (nullable = true)
 |-- gear: long (nullable = true)
 |-- carb: long (nullable = true)
 """

# We use the show() method for this. Here we preview the first 5 records. Compare it to a similar head() function in Pandas.
sdf.show(5)
# Output of line 56 (sdf.show(5)):
"""
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|       Unnamed: 0| mpg|cyl| disp| hp|drat|   wt| qsec| vs| am|gear|carb|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|        Mazda RX4|21.0|  6|160.0|110| 3.9| 2.62|16.46|  0|  1|   4|   4|
|    Mazda RX4 Wag|21.0|  6|160.0|110| 3.9|2.875|17.02|  0|  1|   4|   4|
|       Datsun 710|22.8|  4|108.0| 93|3.85| 2.32|18.61|  1|  1|   4|   1|
|   Hornet 4 Drive|21.4|  6|258.0|110|3.08|3.215|19.44|  1|  0|   3|   1|
|Hornet Sportabout|18.7|  8|360.0|175|3.15| 3.44|17.02|  0|  0|   3|   2|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
only showing top 5 rows
"""

# We use the select() function to select a particular column of data. Here we show the mpg column.
sdf.select('mpg').show(5)
"""
+----+
| mpg|
+----+
|21.0|
|21.0|
|22.8|
|21.4|
|18.7|
+----+
only showing top 5 rows
"""

# We first filter to only retain rows with mpg > 18. We use the filter() function for this.
sdf.filter(sdf['mpg'] < 18).show(5)
"""
+-----------+----+---+-----+---+----+----+-----+---+---+----+----+
| Unnamed: 0| mpg|cyl| disp| hp|drat|  wt| qsec| vs| am|gear|carb|
+-----------+----+---+-----+---+----+----+-----+---+---+----+----+
| Duster 360|14.3|  8|360.0|245|3.21|3.57|15.84|  0|  0|   3|   4|
|  Merc 280C|17.8|  6|167.6|123|3.92|3.44| 18.9|  1|  0|   4|   4|
| Merc 450SE|16.4|  8|275.8|180|3.07|4.07| 17.4|  0|  0|   3|   3|
| Merc 450SL|17.3|  8|275.8|180|3.07|3.73| 17.6|  0|  0|   3|   3|
|Merc 450SLC|15.2|  8|275.8|180|3.07|3.78| 18.0|  0|  0|   3|   3|
+-----------+----+---+-----+---+----+----+-----+---+---+----+----+
only showing top 5 rows
"""

# Spark also provides a number of functions that can be directly applied to columns for data processing and aggregation. 
# The example below shows the use of basic arithmetic functions to convert the weight values from lb to metric ton. 
# We create a new column called wtTon that has the weight from the wt column converted to metric tons.
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)
"""
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+-------+
|       Unnamed: 0| mpg|cyl| disp| hp|drat|   wt| qsec| vs| am|gear|carb|  wtTon|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+-------+
|        Mazda RX4|21.0|  6|160.0|110| 3.9| 2.62|16.46|  0|  1|   4|   4|  1.179|
|    Mazda RX4 Wag|21.0|  6|160.0|110| 3.9|2.875|17.02|  0|  1|   4|   4|1.29375|
|       Datsun 710|22.8|  4|108.0| 93|3.85| 2.32|18.61|  1|  1|   4|   1|  1.044|
|   Hornet 4 Drive|21.4|  6|258.0|110|3.08|3.215|19.44|  1|  0|   3|   1|1.44675|
|Hornet Sportabout|18.7|  8|360.0|175|3.15| 3.44|17.02|  0|  0|   3|   2|  1.548|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+-------+
only showing top 5 rows
"""

sdf_new = sdf.withColumnRenamed("vs", "versus")
# The execution of the above function doesn’t modify the original DataFrame "sdf"; instead, a new DataFrame "sdf_new" is created with the renamed column.


sdf.where(sdf['mpg'] < 18).show(3) 
"""
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
|Unnamed: 0| mpg|cyl| disp| hp|drat|  wt| qsec| vs| am|gear|carb|
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
|Duster 360|14.3|  8|360.0|245|3.21|3.57|15.84|  0|  0|   3|   4|
| Merc 280C|17.8|  6|167.6|123|3.92|3.44| 18.9|  1|  0|   4|   4|
|Merc 450SE|16.4|  8|275.8|180|3.07|4.07| 17.4|  0|  0|   3|   3|
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
only showing top 3 rows
"""

# Combining DataFrames based on a specific condition.
# The function "join()"combines the DataFrames based on a specific condition.

# define sample DataFrame 1 
data = [("A101", "John"), ("A102", "Peter"), ("A103", "Charlie")] 
columns = ["emp_id", "emp_name"] 
dataframe_1 = spark.createDataFrame(data, columns) 

# define sample DataFrame 2 
data = [("A101", 1000), ("A102", 2000), ("A103", 3000)]
columns = ["emp_id", "salary"]
dataframe_2 = spark.createDataFrame(data, columns)

# create a new DataFrame, "combined_df" by performing an inner join
combined_df = dataframe_1.join(dataframe_2, on="emp_id", how="inner")

# "fillna()" or "fill()" function fill the missing values with a specified value.
# define sample DataFrame 1

data = [("A101", 1000), ("A102", 2000), ("A103",None)]
columns = ["emp_id", "salary"]
dataframe_1 = spark.createDataFrame(data, columns)

# fill missing salary value with a specified value 

filled_df = dataframe_1.fillna({"salary": 3000}) 
filled_df.head(3)

"""
[Row(emp_id='A101', salary=1000),
 Row(emp_id='A102', salary=2000),
 Row(emp_id='A103', salary=3000)]
"""

# Spark DataFrames support a number of commonly used functions to aggregate data after grouping. In this example we compute the average weight of cars by their cylinders as shown below.
sdf.groupby(['cyl'])\
.agg({"wt": "AVG"})\
.show(5)

# We can also sort the output from the aggregation to get the most common cars.
car_counts = sdf.groupby(['cyl'])\
.agg({"wt": "count"})\
.sort("count(wt)", ascending=False)\
.show(5)

# Display the first 5 rows of all cars that have atleast 5 cylinders.
sdf.where(sdf['cyl'] >= 5).show(5)
"""
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|       Unnamed: 0| mpg|cyl| disp| hp|drat|   wt| qsec| vs| am|gear|carb|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|        Mazda RX4|21.0|  6|160.0|110| 3.9| 2.62|16.46|  0|  1|   4|   4|
|    Mazda RX4 Wag|21.0|  6|160.0|110| 3.9|2.875|17.02|  0|  1|   4|   4|
|   Hornet 4 Drive|21.4|  6|258.0|110|3.08|3.215|19.44|  1|  0|   3|   1|
|Hornet Sportabout|18.7|  8|360.0|175|3.15| 3.44|17.02|  0|  0|   3|   2|
|          Valiant|18.1|  6|225.0|105|2.76| 3.46|20.22|  1|  0|   3|   1|
+-----------------+----+---+-----+---+----+-----+-----+---+---+----+----+
only showing top 5 rows
"""

# mean weight = sum of weight/number of cars 
sdf.agg({"wt": "count"})
('wtTon', sdf['wt'] * 0.45)

sdf.withColumn("wt_metric_tons", sdf["wt"] * 0.453592 / 1000).agg({"wt_metric_tons": "AVG"}).show()


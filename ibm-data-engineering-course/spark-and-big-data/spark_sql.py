# Installing required packages
# !pip install pyspark
# !pip install findspark
# !pip install pyarrow==0.14.1 
# !pip install pandas
# !pip install numpy==1.19.5
import findspark
findspark.init()
import pandas as pd
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

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')
# Preview a few records
mtcars.head()
mtcars.rename( columns={'Unnamed: 0':'name'}, inplace=True )

sdf = spark.createDataFrame(mtcars) 

sdf.printSchema()
# Rename the existing column name "vs" to "versus" and assign the new result DataFrame to a variable, "sdf_new".
# The function withColumnRenamed() is renames the existing column names.

sdf_new = sdf.withColumnRenamed("vs", "versus")
# The execution of the above function doesn’t modify the original DataFrame sdf, instead, a new DataFrame sdf_new is created with the renamed column.

# View the new dataframe
sdf_new.head(5)
# Observe how vs has now been renamed to versus in this dataframe.

# Create a Table View
# Creating a table view in Spark SQL is required to run SQL queries programmatically on a DataFrame. A view is a temporary table to run SQL queries. A Temporary view provides local scope within the current Spark session. In this example we create a temporary view using the createTempView() function

sdf.createTempView("cars")

# Running SQL queries and aggregating data
# Once we have a table view, we can run queries similar to querying a SQL table. We perform similar operations to the ones in the DataFrames notebook. Note the difference here however is that we use the SQL queries directly.

# Showing the whole table
spark.sql("SELECT * FROM cars").show()
"""
+-------------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|               name| mpg|cyl| disp| hp|drat|   wt| qsec| vs| am|gear|carb|
+-------------------+----+---+-----+---+----+-----+-----+---+---+----+----+
|          Mazda RX4|21.0|  6|160.0|110| 3.9| 2.62|16.46|  0|  1|   4|   4|
|      Mazda RX4 Wag|21.0|  6|160.0|110| 3.9|2.875|17.02|  0|  1|   4|   4|
|         Datsun 710|22.8|  4|108.0| 93|3.85| 2.32|18.61|  1|  1|   4|   1|
|     Hornet 4 Drive|21.4|  6|258.0|110|3.08|3.215|19.44|  1|  0|   3|   1|
|  Hornet Sportabout|18.7|  8|360.0|175|3.15| 3.44|17.02|  0|  0|   3|   2|
|            Valiant|18.1|  6|225.0|105|2.76| 3.46|20.22|  1|  0|   3|   1|
|         Duster 360|14.3|  8|360.0|245|3.21| 3.57|15.84|  0|  0|   3|   4|
|          Merc 240D|24.4|  4|146.7| 62|3.69| 3.19| 20.0|  1|  0|   4|   2|
|           Merc 230|22.8|  4|140.8| 95|3.92| 3.15| 22.9|  1|  0|   4|   2|
|           Merc 280|19.2|  6|167.6|123|3.92| 3.44| 18.3|  1|  0|   4|   4|
|          Merc 280C|17.8|  6|167.6|123|3.92| 3.44| 18.9|  1|  0|   4|   4|
|         Merc 450SE|16.4|  8|275.8|180|3.07| 4.07| 17.4|  0|  0|   3|   3|
|         Merc 450SL|17.3|  8|275.8|180|3.07| 3.73| 17.6|  0|  0|   3|   3|
|        Merc 450SLC|15.2|  8|275.8|180|3.07| 3.78| 18.0|  0|  0|   3|   3|
| Cadillac Fleetwood|10.4|  8|472.0|205|2.93| 5.25|17.98|  0|  0|   3|   4|
|Lincoln Continental|10.4|  8|460.0|215| 3.0|5.424|17.82|  0|  0|   3|   4|
|  Chrysler Imperial|14.7|  8|440.0|230|3.23|5.345|17.42|  0|  0|   3|   4|
|           Fiat 128|32.4|  4| 78.7| 66|4.08|  2.2|19.47|  1|  1|   4|   1|
|        Honda Civic|30.4|  4| 75.7| 52|4.93|1.615|18.52|  1|  1|   4|   2|
|     Toyota Corolla|33.9|  4| 71.1| 65|4.22|1.835| 19.9|  1|  1|   4|   1|
+-------------------+----+---+-----+---+----+-----+-----+---+---+----+----+
only showing top 20 rows
"""
# Showing a specific column
spark.sql("SELECT mpg FROM cars").show(5)
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
# Basic filtering query to determine cars that have a high mileage and low cylinder count
spark.sql("SELECT * FROM cars where mpg>20 AND cyl < 6").show(5)
"""
+-----------+----+---+-----+---+----+-----+-----+---+---+----+----+
|       name| mpg|cyl| disp| hp|drat|   wt| qsec| vs| am|gear|carb|
+-----------+----+---+-----+---+----+-----+-----+---+---+----+----+
| Datsun 710|22.8|  4|108.0| 93|3.85| 2.32|18.61|  1|  1|   4|   1|
|  Merc 240D|24.4|  4|146.7| 62|3.69| 3.19| 20.0|  1|  0|   4|   2|
|   Merc 230|22.8|  4|140.8| 95|3.92| 3.15| 22.9|  1|  0|   4|   2|
|   Fiat 128|32.4|  4| 78.7| 66|4.08|  2.2|19.47|  1|  1|   4|   1|
|Honda Civic|30.4|  4| 75.7| 52|4.93|1.615|18.52|  1|  1|   4|   2|
+-----------+----+---+-----+---+----+-----+-----+---+---+----+----+
only showing top 5 rows
"""
# Use where method to get list of cars that have miles per gallon is less than 18
sdf.where(sdf['mpg'] < 18).show(3) 
"""
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
|      name| mpg|cyl| disp| hp|drat|  wt| qsec| vs| am|gear|carb|
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
|Duster 360|14.3|  8|360.0|245|3.21|3.57|15.84|  0|  0|   3|   4|
| Merc 280C|17.8|  6|167.6|123|3.92|3.44| 18.9|  1|  0|   4|   4|
|Merc 450SE|16.4|  8|275.8|180|3.07|4.07| 17.4|  0|  0|   3|   3|
+----------+----+---+-----+---+----+----+-----+---+---+----+----+
only showing top 3 rows
"""
# Aggregating data and grouping by cylinders
spark.sql("SELECT count(*), cyl from cars GROUP BY cyl").show()
"""
+--------+---+
|count(1)|cyl|
+--------+---+
|       7|  6|
|      14|  8|
|      11|  4|
+--------+---+
"""

# Create a Pandas UDF to apply a columnar operation
# Apache Spark has become the de-facto standard in processing big data. To enable data scientists to leverage the value of big data, Spark added a Python API in version 0.7, with support for user-defined functions (UDF). These user-defined functions operate one-row-at-a-time, and thus suffer from high serialization and invocation overhead. As a result, many data pipelines define UDFs in Java and Scala and then invoke them from Python.
# Pandas UDFs built on top of Apache Arrow bring you the _best of both worlds_—the ability to define low-overhead, high-performance UDFs entirely in Python. In this simple example, we will build a Scalar Pandas UDF to convert the wT column from imperial units (1000-lbs) to metric units (metric tons).
# In addition, UDFs can be registered and invoked in SQL out of the box by registering a regular python function using the @pandas_udf() decorator. We can then apply this UDF to our wt column.

# Importing libraries and registering a UDF
# import the Pandas UDF function 
from pyspark.sql.functions import pandas_udf, PandasUDFType
@pandas_udf("float")
def convert_wt(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.45

spark.udf.register("convert_weight", convert_wt)
# Applying the UDF to the tableview
# We can now apply the convert_weight user-defined-function to our wt column from the cars table view. This is done very simply using the SQL query shown below. In this example below we show both the original weight (in ton-lbs) and converted weight (in metric tons).

spark.sql("SELECT *, wt AS weight_imperial, convert_weight(wt) as weight_metric FROM cars").show()
# Combining DataFrames based on a specific condition.
# Understanding JOIN operation
# define sample DataFrame 1 

data = [("A101", "John"), ("A102", "Peter"), ("A103", "Charlie")] 

columns = ["emp_id", "emp_name"]

dataframe_1 = spark.createDataFrame(data, columns)
# define sample DataFrame 2

data = [("A101", 3250), ("A102", 6735), ("A103", 8650)] 

columns = ["emp_id", "salary"] 

dataframe_2 = spark.createDataFrame(data, columns) 
# create a new DataFrame, "combined_df" by performing an inner join 

combined_df = dataframe_1.join(dataframe_2, on="emp_id", how="inner") 
# Show the data in combined_df as a list of Row.

combined_df.collect()
"""
[Row(emp_id='A103', emp_name='Charlie', salary=8650),
 Row(emp_id='A102', emp_name='Peter', salary=6735),
 Row(emp_id='A101', emp_name='John', salary=3250)]
"""

# Filling the missing values
# define sample DataFrame 1 with some missing values

data = [("A101", 1000), ("A102", 2000), ("A103",None)]

columns = ["emp_id", "salary"]

dataframe_1 = spark.createDataFrame(data, columns)

dataframe_1.head(3)
#You will see that an error is thrown as the dataframe has null value.
#Note that the third record of the DataFrame "dataframe_1", the column “salary”, contains null("na") value. It can be filled with a value by using the function "fillna()".

# fill missing salary value with a specified value

filled_df = dataframe_1.fillna({"salary": 3000})
filled_df.head(3)


# Display all Mercedez car rows from the cars table view we created earlier. The Mercedez cars have the prefix "Merc" in the car name column.

spark.sql("SELECT * FROM cars where name like 'Merc%'").show()

# User Defined Functions
# In this notebook, we created a UDF to convert weight from imperial to metric units. Now for this exercise, please create a pandas UDF to convert the mpg column to kmpl (kilometers per liter). You can use the conversion factor of 0.425.

# Code block for learners to answer
from pyspark.sql.functions import pandas_udf

@pandas_udf("float")
def convert_mileage(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.425

spark.udf.register("convert_mileage", convert_mileage)

spark.sql("SELECT *, mpg AS mpg, convert_mileage(mpg) as kmpl FROM cars").show()
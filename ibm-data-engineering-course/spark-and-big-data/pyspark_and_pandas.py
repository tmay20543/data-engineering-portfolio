# !pip install pyspark
# !pip install findspark
# !pip install pandas

"""
findspark is used to locate the Spark installation.
pandas is imported for data manipulation.
"""

import findspark  # This helps us find and use Apache Spark
findspark.init()  # Initialize findspark to locate Spark
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType
import pandas as pd  
# Initialize a Spark Session
spark = SparkSession \
    .builder \
    .appName("COVID-19 Data Analysis") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Check if the Spark Session is active
if 'spark' in locals() and isinstance(spark, SparkSession):
    print("SparkSession is active and ready to use.")
else:
    print("SparkSession is not active. Please create a SparkSession.")

# Read the COVID-19 data from the provided URL
vaccination_data = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/KpHDlIzdtR63BdTofl1mOg/owid-covid-latest.csv')


"""
Let's define a list called columns_to_display, which contains the names of the columns as : ['continent', 'total_cases', 'total_deaths', 'total_vaccinations', 'population'].
By using vaccination_data[columns_to_display].head(), let's filter the DataFrame to only show the specified columns and again display the first five records of this subset.
The continent column is explicitly converted to string, while the numeric columns (total cases, total deaths, total vaccinations, population) are filled with zeros for NaN values and then converted to int64 (which is compatible with LongType in Spark).
The use of fillna(0) ensures that NaN values do not cause type issues during the Spark DataFrame creation.
"""
print("Displaying the first 5 records of the vaccination data:")
columns_to_display = ['continent', 'total_cases', 'total_deaths', 'total_vaccinations', 'population']
# Show the first 5 records
print(vaccination_data[columns_to_display].head())

# Output of the above:
"""
Displaying the first 5 records of the vaccination data:
  continent  total_cases  total_deaths  total_vaccinations    population
0      Asia     235214.0        7998.0                 NaN  4.112877e+07
1       NaN   13145380.0      259117.0                 NaN  1.426737e+09
2    Europe     335047.0        3605.0                 NaN  2.842318e+06
3    Africa     272139.0        6881.0                 NaN  4.490323e+07
4   Oceania       8359.0          34.0                 NaN  4.429500e+04
"""

# --- Converting the Pandas DataFrame to a Spark DataFrame ---

# Let's convert the Pandas DataFrame, which contains our COVID-19 vaccination data, into a Spark DataFrame. 
# This conversion is crucial as it allows us to utilize Spark's distributed computing capabilities, enabling us to handle larger datasets and perform operations in a more efficient manner.

"""
Defining the schema:
StructType:

A class that defines a structure for a DataFrame.
StructField:

Represents a single field in the schema.
Parameters:
Field name: The name of the field.
Data type: The type of data for the field.
Nullable: A boolean indicating whether null values are allowed.
Data types:

StringType(): Used for text fields.
LongType(): Used for numerical fields.
Data type conversion:
astype(str):

Used to convert the 'continent' column to string type.
fillna(0):

Replaces any NaN values with 0, ensuring that the numerical fields do not contain any missing data.
astype('int64'):

Converts the columns from potentially mixed types to 64-bit integers for consistent numerical representation.
"""

# Creating a Spark DataFrame:

"""
createDataFrame:
The createDataFrame method of the Spark session (spark) is called with vaccination_data (the Pandas DataFrame) as its argument.
Parameters:
It takes as input a subset of the pandas DataFrame that corresponds to the fields defined in the schema, accessed using schema.fieldNames().
This function automatically converts the Pandas DataFrame into a Spark DataFrame, which is designed to handle larger data sets across a distributed environment.
The resulting spark_df will have the defined schema, which ensures consistency and compatibility with Spark's data processing capabilities.
"""

# Convert to Spark DataFrame directly
# Define the schema
schema = StructType([
    StructField("continent", StringType(), True),
    StructField("total_cases", LongType(), True),
    StructField("total_deaths", LongType(), True),
    StructField("total_vaccinations", LongType(), True),
    StructField("population", LongType(), True)
])

# Convert the columns to the appropriate data types
vaccination_data['continent'] = vaccination_data['continent'].astype(str)  # Ensures continent is a string
vaccination_data['total_cases'] = vaccination_data['total_cases'].fillna(0).astype('int64')  # Fill NaNs and convert to int
vaccination_data['total_deaths'] = vaccination_data['total_deaths'].fillna(0).astype('int64')  # Fill NaNs and convert to int
vaccination_data['total_vaccinations'] = vaccination_data['total_vaccinations'].fillna(0).astype('int64')  # Fill NaNs and convert to int
vaccination_data['population'] = vaccination_data['population'].fillna(0).astype('int64')  # Fill NaNs and convert to int

spark_df = spark.createDataFrame(vaccination_data[schema.fieldNames()])  # Use only the specified fields
# Show the Spark DataFrame
spark_df.show()

print("Schema of the Spark DataFrame:")
spark_df.printSchema()
# Print the structure of the DataFrame (columns and types)

# List the names of the columns you want to display
columns_to_display = ['continent', 'total_cases', 'total_deaths', 'total_vaccinations', 'population']
# Display the first 5 records of the specified columns
spark_df.select(columns_to_display).show(5)

print("Displaying the 'continent' and 'total_cases' columns:")
# Show only the 'continent' and 'total_cases' columns
spark_df.select('continent', 'total_cases').show(5)

print("Filtering records where 'total_cases' is greater than 1,000,000:")
 # Show records with more than 1 million total cases
spark_df.filter(spark_df['total_cases'] > 1000000).show(5) 

from pyspark.sql import functions as F

spark_df_with_percentage = spark_df.withColumn(
    'death_percentage', 
    (spark_df['total_deaths'] / spark_df['population']) * 100
)
spark_df_with_percentage = spark_df_with_percentage.withColumn(
    'death_percentage',
    F.concat(
        # Format to 2 decimal places
        F.format_number(spark_df_with_percentage['death_percentage'], 2), 
        # Append the percentage symbol 
        F.lit('%')  
    )
)
columns_to_display = ['total_deaths', 'population', 'death_percentage', 'continent', 'total_vaccinations', 'total_cases']
spark_df_with_percentage.select(columns_to_display).show(5)

"""
+------------+----------+----------------+---------+------------------+-----------+
|total_deaths|population|death_percentage|continent|total_vaccinations|total_cases|
+------------+----------+----------------+---------+------------------+-----------+
|        7998|  41128772|           0.02%|     Asia|                 0|     235214|
|      259117|1426736614|           0.02%|      nan|                 0|   13145380|
|        3605|   2842318|           0.13%|   Europe|                 0|     335047|
|        6881|  44903228|           0.02%|   Africa|                 0|     272139|
|          34|     44295|           0.08%|  Oceania|                 0|       8359|
+------------+----------+----------------+---------+------------------+-----------+
only showing top 5 rows
"""

# Grouping and summarizing

print("Calculating the total deaths per continent:")
# Group by continent and sum total death rates
spark_df.groupby(['continent']).agg({"total_deaths": "SUM"}).show()

"""
+-------------+-----------------+
|    continent|sum(total_deaths)|
+-------------+-----------------+
|       Europe|          2102483|
|       Africa|           259117|
|          nan|         22430618|
|North America|          1671178|
|South America|          1354187|
|      Oceania|            32918|
|         Asia|          1637249|
+-------------+-----------------+
"""

# --- Exploring user-defined functions (UDFs) ---
"""
UDFs in PySpark allow us to create custom functions that can be applied to individual columns within a DataFrame. 
This feature provides increased flexibility and customization in data processing, 
enabling us to define specific transformations or calculations that are not available through built-in functions. 
In this section, let's define a UDF to convert total deaths in the dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
# Function definition
def convert_total_deaths(total_deaths):
    return total_deaths * 2
# Here you can define any transformation you want
# Register the UDF with Spark
""" 
The line spark.udf.register("convert_total_deaths", convert_total_deaths,
IntegerType()) registers the UDF with Spark indicating that the function returns an integer,
allowing us to use it in Spark SQL queries and DataFrame operations. """

spark.udf.register("convert_total_deaths", convert_total_deaths, IntegerType())

# Using Spark SQL
"""
Spark SQL enables us to execute SQL queries directly on DataFrames.
"""
# Drop the existing temporary view if it exists
spark.sql("DROP VIEW IF EXISTS data_v")

# Create a new temporary view
spark_df.createTempView('data_v')

# Execute the SQL query using the UDF
spark.sql('SELECT continent, total_deaths, convert_total_deaths(total_deaths) as converted_total_deaths FROM data_v').show()

# Displaying All Records
spark.sql('SELECT * FROM data_v').show()

print("Displaying continent with total vaccinated more than 1 million:")
# SQL filtering
spark.sql("SELECT continent FROM data_v WHERE total_vaccinations > 1000000").show()

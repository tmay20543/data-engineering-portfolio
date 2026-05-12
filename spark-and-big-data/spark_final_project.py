# Installing required packages  

# !pip install pyspark  findspark wget

import findspark

findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession

# Creating a SparkContext object  

sc = SparkContext.getOrCreate()

# Creating a SparkSession  

spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Download the CSV data first into a local `employees.csv` file
import wget

wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")

# Read data from the "emp" CSV file and import it into a DataFrame variable named "employees_df"  
employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Define a Schema for the input data and read the file using the user-defined Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, DateType

schema = StructType([
    StructField("emp_no", IntegerType(), False),
    StructField("emp_name", StringType(), False),
    StructField("salary", DoubleType(), False),
    StructField("age", IntegerType(), False),
    StructField("department", StringType(), False)
])

employees_df = spark.read.csv("employees.csv", header=True, schema=schema)

# Display all columns of the DataFrame, along with their respective data types
employees_df.printSchema()
"""
root
 |-- emp_no: integer (nullable = true)
 |-- emp_name: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- age: integer (nullable = true)
 |-- department: string (nullable = true)
"""

# Create a temporary view named employees for the employees_df DataFrame, enabling Spark SQL queries on the data.
employees_df.createTempView('employees')

# SQL query to fetch solely the records from the View where the age exceeds 30
spark.sql("SELECT * FROM employees WHERE age > 30").show()

"""
+------+-----------+-------+---+----------+
|emp_no|   emp_name| salary|age|department|
+------+-----------+-------+---+----------+
|   199|    Douglas| 2600.0| 34|     Sales|
|   200|   Jennifer| 4400.0| 36| Marketing|
|   201|    Michael|13000.0| 32|        IT|
|   202|        Pat| 6000.0| 39|        HR|
|   203|      Susan| 6500.0| 36| Marketing|
|   205|    Shelley|12008.0| 33|   Finance|
|   206|    William| 8300.0| 37|        IT|
|   100|     Steven|24000.0| 39|        IT|
|   102|        Lex|17000.0| 37| Marketing|
|   103|  Alexander| 9000.0| 39| Marketing|
|   104|      Bruce| 6000.0| 38|        IT|
|   105|      David| 4800.0| 39|        IT|
|   106|      Valli| 4800.0| 38|     Sales|
|   107|      Diana| 4200.0| 35|     Sales|
|   109|     Daniel| 9000.0| 35|        HR|
|   110|       John| 8200.0| 31| Marketing|
|   111|     Ismael| 7700.0| 32|        IT|
|   112|Jose Manuel| 7800.0| 34|        HR|
|   113|       Luis| 6900.0| 34|     Sales|
|   116|     Shelli| 2900.0| 37|   Finance|
+------+-----------+-------+---+----------+
only showing top 20 rows
"""
# SQL query to calculate the average salary of employees grouped by department
spark.sql("SELECT department, AVG(salary) FROM employees GROUP BY department").show()
"""
+----------+-----------------+
|department|      avg(salary)|
+----------+-----------------+
|     Sales|5492.923076923077|
|        HR|           5837.5|
|   Finance|           5730.8|
| Marketing|6633.333333333333|
|        IT|           7400.0|
+----------+-----------------+
"""


# Apply a filter to select records where the department is 'IT'
employees_df.filter("department = 'IT'").show() 
"""
+------+--------+-------+---+----------+
|emp_no|emp_name| salary|age|department|
+------+--------+-------+---+----------+
|   198|  Donald| 2600.0| 29|        IT|
|   201| Michael|13000.0| 32|        IT|
|   206| William| 8300.0| 37|        IT|
|   100|  Steven|24000.0| 39|        IT|
|   104|   Bruce| 6000.0| 38|        IT|
|   105|   David| 4800.0| 39|        IT|
|   111|  Ismael| 7700.0| 32|        IT|
|   129|   Laura| 3300.0| 38|        IT|
|   132|      TJ| 2100.0| 34|        IT|
|   136|   Hazel| 2200.0| 29|        IT|
+------+--------+-------+---+----------+
"""
from pyspark.sql.functions import col

# Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
employees_df = employees_df.withColumn('SalaryAfterBonus', (employees_df.salary * 0.10 + employees_df.salary))
employees_df.show()

"""
+------+---------+-------+---+----------+----------------+
|emp_no| emp_name| salary|age|department|SalaryAfterBonus|
+------+---------+-------+---+----------+----------------+
|   198|   Donald| 2600.0| 29|        IT|          2860.0|
|   199|  Douglas| 2600.0| 34|     Sales|          2860.0|
|   200| Jennifer| 4400.0| 36| Marketing|          4840.0|
|   201|  Michael|13000.0| 32|        IT|         14300.0|
|   202|      Pat| 6000.0| 39|        HR|          6600.0|
|   203|    Susan| 6500.0| 36| Marketing|          7150.0|
|   204|  Hermann|10000.0| 29|   Finance|         11000.0|
|   205|  Shelley|12008.0| 33|   Finance|         13208.8|
|   206|  William| 8300.0| 37|        IT|          9130.0|
|   100|   Steven|24000.0| 39|        IT|         26400.0|
|   101|    Neena|17000.0| 27|     Sales|         18700.0|
|   102|      Lex|17000.0| 37| Marketing|         18700.0|
|   103|Alexander| 9000.0| 39| Marketing|          9900.0|
|   104|    Bruce| 6000.0| 38|        IT|          6600.0|
|   105|    David| 4800.0| 39|        IT|          5280.0|
|   106|    Valli| 4800.0| 38|     Sales|          5280.0|
|   107|    Diana| 4200.0| 35|     Sales|          4620.0|
|   108|    Nancy|12008.0| 28|     Sales|         13208.8|
|   109|   Daniel| 9000.0| 35|        HR|          9900.0|
|   110|     John| 8200.0| 31| Marketing|          9020.0|
+------+---------+-------+---+----------+----------------+
only showing top 20 rows
"""

from pyspark.sql.functions import max

# Group data by age and calculate the maximum salary for each age group
employees_df.groupBy('age').agg(max('salary'))

"""
+---+-----------+
|age|max(salary)|
+---+-----------+
| 31|     8200.0|
| 34|     7800.0|
| 28|    12008.0|
| 27|    17000.0|
| 26|     3600.0|
| 37|    17000.0|
| 35|     9000.0|
| 39|    24000.0|
| 38|     6000.0|
| 29|    10000.0|
| 32|    13000.0|
| 33|    12008.0|
| 30|     8000.0|
| 36|     7900.0|
+---+-----------+
"""

# Join the DataFrame with itself based on the "Emp_No" column
employees_df.join(employees_df, 'emp_no', 'inner').show()

"""
+------+---------+-------+---+----------+----------------+---------+-------+---+----------+----------------+
|emp_no| emp_name| salary|age|department|SalaryAfterBonus| emp_name| salary|age|department|SalaryAfterBonus|
+------+---------+-------+---+----------+----------------+---------+-------+---+----------+----------------+
|   198|   Donald| 2600.0| 29|        IT|          2860.0|   Donald| 2600.0| 29|        IT|          2860.0|
|   199|  Douglas| 2600.0| 34|     Sales|          2860.0|  Douglas| 2600.0| 34|     Sales|          2860.0|
|   200| Jennifer| 4400.0| 36| Marketing|          4840.0| Jennifer| 4400.0| 36| Marketing|          4840.0|
|   201|  Michael|13000.0| 32|        IT|         14300.0|  Michael|13000.0| 32|        IT|         14300.0|
|   202|      Pat| 6000.0| 39|        HR|          6600.0|      Pat| 6000.0| 39|        HR|          6600.0|
|   203|    Susan| 6500.0| 36| Marketing|          7150.0|    Susan| 6500.0| 36| Marketing|          7150.0|
|   204|  Hermann|10000.0| 29|   Finance|         11000.0|  Hermann|10000.0| 29|   Finance|         11000.0|
|   205|  Shelley|12008.0| 33|   Finance|         13208.8|  Shelley|12008.0| 33|   Finance|         13208.8|
|   206|  William| 8300.0| 37|        IT|          9130.0|  William| 8300.0| 37|        IT|          9130.0|
|   100|   Steven|24000.0| 39|        IT|         26400.0|   Steven|24000.0| 39|        IT|         26400.0|
|   101|    Neena|17000.0| 27|     Sales|         18700.0|    Neena|17000.0| 27|     Sales|         18700.0|
|   102|      Lex|17000.0| 37| Marketing|         18700.0|      Lex|17000.0| 37| Marketing|         18700.0|
|   103|Alexander| 9000.0| 39| Marketing|          9900.0|Alexander| 9000.0| 39| Marketing|          9900.0|
|   104|    Bruce| 6000.0| 38|        IT|          6600.0|    Bruce| 6000.0| 38|        IT|          6600.0|
|   105|    David| 4800.0| 39|        IT|          5280.0|    David| 4800.0| 39|        IT|          5280.0|
|   106|    Valli| 4800.0| 38|     Sales|          5280.0|    Valli| 4800.0| 38|     Sales|          5280.0|
|   107|    Diana| 4200.0| 35|     Sales|          4620.0|    Diana| 4200.0| 35|     Sales|          4620.0|
|   108|    Nancy|12008.0| 28|     Sales|         13208.8|    Nancy|12008.0| 28|     Sales|         13208.8|
|   109|   Daniel| 9000.0| 35|        HR|          9900.0|   Daniel| 9000.0| 35|        HR|          9900.0|
|   110|     John| 8200.0| 31| Marketing|          9020.0|     John| 8200.0| 31| Marketing|          9020.0|
+------+---------+-------+---+----------+----------------+---------+-------+---+----------+----------------+
only showing top 20 rows

"""

# Calculate the average age of employees
from pyspark.sql.functions import avg 

employees_df.agg(avg('age')).show()

"""
+--------+
|avg(age)|
+--------+
|   33.56|
+--------+
"""

# Calculate the total salary for each department. Hint - User GroupBy and Aggregate functions
from pyspark.sql.functions import sum 

employees_df.groupBy('department').agg(sum('salary')).show()

"""
+----------+-----------+
|department|sum(salary)|
+----------+-----------+
|     Sales|    71408.0|
|        HR|    46700.0|
|   Finance|    57308.0|
| Marketing|    59700.0|
|        IT|    74000.0|
+----------+-----------+
"""
# Sort the DataFrame by age in ascending order and then by salary in descending order
employees_df.sort("age", ascending=True).sort("salary", ascending=False).show()

"""
+------+-----------+-------+---+----------+----------------+
|emp_no|   emp_name| salary|age|department|SalaryAfterBonus|
+------+-----------+-------+---+----------+----------------+
|   100|     Steven|24000.0| 39|        IT|         26400.0|
|   102|        Lex|17000.0| 37| Marketing|         18700.0|
|   101|      Neena|17000.0| 27|     Sales|         18700.0|
|   201|    Michael|13000.0| 32|        IT|         14300.0|
|   205|    Shelley|12008.0| 33|   Finance|         13208.8|
|   108|      Nancy|12008.0| 28|     Sales|         13208.8|
|   114|        Den|11000.0| 27|   Finance|         12100.0|
|   204|    Hermann|10000.0| 29|   Finance|         11000.0|
|   103|  Alexander| 9000.0| 39| Marketing|          9900.0|
|   109|     Daniel| 9000.0| 35|        HR|          9900.0|
|   206|    William| 8300.0| 37|        IT|          9130.0|
|   110|       John| 8200.0| 31| Marketing|          9020.0|
|   121|       Adam| 8200.0| 39|        HR|          9020.0|
|   120|    Matthew| 8000.0| 30|        HR|          8800.0|
|   122|      Payam| 7900.0| 36|   Finance|          8690.0|
|   112|Jose Manuel| 7800.0| 34|        HR|          8580.0|
|   111|     Ismael| 7700.0| 32|        IT|          8470.0|
|   113|       Luis| 6900.0| 34|     Sales|          7590.0|
|   203|      Susan| 6500.0| 36| Marketing|          7150.0|
|   123|     Shanta| 6500.0| 35|     Sales|          7150.0|
+------+-----------+-------+---+----------+----------------+
only showing top 20 rows
"""

from pyspark.sql.functions import count

# Calculate the number of employees in each department

employees_df.groupBy('department').agg(count('emp_no')).show()

"""
+----------+-------------+
|department|count(emp_no)|
+----------+-------------+
|     Sales|           13|
|        HR|            8|
|   Finance|           10|
| Marketing|            9|
|        IT|           10|
+----------+-------------+
"""

# Apply a filter to select records where the employee's name contains the letter 'o'
employees_df.filter("emp_name like '%o%' ").show()

"""
+------+-----------+------+---+----------+----------------+
|emp_no|   emp_name|salary|age|department|SalaryAfterBonus|
+------+-----------+------+---+----------+----------------+
|   198|     Donald|2600.0| 29|        IT|          2860.0|
|   199|    Douglas|2600.0| 34|     Sales|          2860.0|
|   110|       John|8200.0| 31| Marketing|          9020.0|
|   112|Jose Manuel|7800.0| 34|        HR|          8580.0|
|   130|      Mozhe|2800.0| 28| Marketing|          3080.0|
|   133|      Jason|3300.0| 38|     Sales|          3630.0|
|   139|       John|2700.0| 36|     Sales|          2970.0|
|   140|     Joshua|2500.0| 29|   Finance|          2750.0|
+------+-----------+------+---+----------+----------------+
"""

employees_df.filter(col("emp_name").like("%o%")).show()
"""
+------+-----------+------+---+----------+----------------+
|emp_no|   emp_name|salary|age|department|SalaryAfterBonus|
+------+-----------+------+---+----------+----------------+
|   198|     Donald|2600.0| 29|        IT|          2860.0|
|   199|    Douglas|2600.0| 34|     Sales|          2860.0|
|   110|       John|8200.0| 31| Marketing|          9020.0|
|   112|Jose Manuel|7800.0| 34|        HR|          8580.0|
|   130|      Mozhe|2800.0| 28| Marketing|          3080.0|
|   133|      Jason|3300.0| 38|     Sales|          3630.0|
|   139|       John|2700.0| 36|     Sales|          2970.0|
|   140|     Joshua|2500.0| 29|   Finance|          2750.0|
+------+-----------+------+---+----------+----------------+
"""
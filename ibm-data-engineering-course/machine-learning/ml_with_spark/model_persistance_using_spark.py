"""
Model Persistence using PySpark

This practical demonstrates how to train, save, load, and reuse a Spark ML model:
- Loading a CSV dataset
- Preparing feature vectors with VectorAssembler
- Training a Linear Regression model
- Persisting the model to disk
- Loading the saved model
- Making predictions with the loaded model
"""

# -----------------------------------------------------------------------------
# Environment setup
# -----------------------------------------------------------------------------

def warn(*args, **kwargs):
    pass

import warnings
warnings.warn = warn
warnings.filterwarnings("ignore")

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.pipeline import PipelineModel


spark = (
    SparkSession.builder
    .appName("Model Persistence using Spark")
    .getOrCreate()
)


# -----------------------------------------------------------------------------
# 1. Load the MPG dataset
# -----------------------------------------------------------------------------
# The dataset contains vehicle information such as cylinders, engine displacement,
# horsepower, weight, acceleration, year, origin, and MPG.
#
# header=True tells Spark that the first row contains column names.
# inferSchema=True tells Spark to automatically detect column data types.

mpg_data = spark.read.csv("mpg.csv", header=True, inferSchema=True)

mpg_data.printSchema()
mpg_data.show(5)


# -----------------------------------------------------------------------------
# 2. Prepare the feature vector
# -----------------------------------------------------------------------------
# Spark ML models expect the input features to be stored in a single vector column.
# VectorAssembler combines multiple numeric columns into one "features" column.

feature_columns = [
    "Cylinders",
    "Engine Disp",
    "Horsepower",
    "Weight",
    "Accelerate",
    "Year"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

mpg_transformed_data = assembler.transform(mpg_data)

mpg_transformed_data.select("features", "MPG").show(truncate=False)


# -----------------------------------------------------------------------------
# 3. Split the dataset
# -----------------------------------------------------------------------------
# The training set is used to train the model.
# The testing set is kept aside so that predictions can be made on unseen data.

training_data, testing_data = mpg_transformed_data.randomSplit([0.7, 0.3], seed=42)


# -----------------------------------------------------------------------------
# 4. Train a Linear Regression model
# -----------------------------------------------------------------------------
# LinearRegression learns the relationship between the vehicle features and MPG.
# A Pipeline is used so the model can be managed and persisted as a single object.

lr = LinearRegression(
    labelCol="MPG",
    featuresCol="features"
)

pipeline = Pipeline(stages=[lr])
model = pipeline.fit(training_data)


# -----------------------------------------------------------------------------
# 5. Save the trained model
# -----------------------------------------------------------------------------
# overwrite() allows the model folder to be replaced if it already exists.
# save() writes the fitted PipelineModel to disk.

model_path = "./model_storage/"

model.write().overwrite().save(model_path)


# -----------------------------------------------------------------------------
# 6. Load the saved model
# -----------------------------------------------------------------------------
# PipelineModel.load() restores the persisted model so it can be reused later
# without retraining.

loaded_model = PipelineModel.load(model_path)


# -----------------------------------------------------------------------------
# 7. Make predictions with the loaded model
# -----------------------------------------------------------------------------
# transform() applies the loaded model to the testing data and adds a prediction
# column to the resulting DataFrame.

predictions = loaded_model.transform(testing_data)

predictions.select("MPG", "prediction").show(5)


# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------

spark.stop()

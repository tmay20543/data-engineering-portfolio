"""
Feature Extraction and Transformation using PySpark

This practical demonstrates common feature engineering steps used in Spark ML:
- Tokenization
- CountVectorizer
- HashingTF and IDF / TF-IDF
- StopWordsRemover
- StringIndexer
- StandardScaler
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
from pyspark.ml.feature import (
    Tokenizer,
    CountVectorizer,
    HashingTF,
    IDF,
    StopWordsRemover,
    StringIndexer,
    StandardScaler
)
from pyspark.ml.linalg import Vectors


spark = (
    SparkSession.builder
    .appName("Feature Extraction and Transformation using Spark")
    .getOrCreate()
)


# -----------------------------------------------------------------------------
# 1. Tokenization
# -----------------------------------------------------------------------------
# Tokenization splits text into individual words/tokens so that text can be
# processed by machine learning algorithms.

sentenceDataFrame = spark.createDataFrame([
    (1, "Spark is a distributed computing system."),
    (2, "It provides interfaces for multiple languages"),
    (3, "Spark is built on top of Hadoop")
], ["id", "sentence"])

sentenceDataFrame.show(truncate=False)

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
token_df = tokenizer.transform(sentenceDataFrame)

token_df.show(truncate=False)


# -----------------------------------------------------------------------------
# 2. CountVectorizer
# -----------------------------------------------------------------------------
# CountVectorizer converts tokenized text into numerical vectors by counting how
# often each word appears in each document.

textdata = [
    (1, "I love Spark Spark provides Python API ".split()),
    (2, "I love Python Spark supports Python".split()),
    (3, "Spark solves the big problem of big data".split())
]

textdata = spark.createDataFrame(textdata, ["id", "words"])
textdata.show(truncate=False)

cv = CountVectorizer(inputCol="words", outputCol="features")

model = cv.fit(textdata)
result = model.transform(textdata)

result.show(truncate=False)


# -----------------------------------------------------------------------------
# 3. TF-IDF using HashingTF and IDF
# -----------------------------------------------------------------------------
# HashingTF converts words into fixed numeric feature positions.
# IDF reduces the weight of very common words and gives more weight to rarer,
# more meaningful words.

sentenceData = spark.createDataFrame([
    (1, "Spark supports python"),
    (2, "Spark is fast"),
    (3, "Spark is easy")
], ["id", "sentence"])

sentenceData.show(truncate=False)

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)

wordsData.show(truncate=False)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10)
featurizedData = hashingTF.transform(wordsData)

featurizedData.show(truncate=False)

idf = IDF(inputCol="rawFeatures", outputCol="features")

idfModel = idf.fit(featurizedData)
tfidfData = idfModel.transform(featurizedData)

tfidfData.select("sentence", "features").show(truncate=False)


# -----------------------------------------------------------------------------
# 4. StopWordsRemover
# -----------------------------------------------------------------------------
# StopWordsRemover removes common words such as "is", "an", "has", and "for",
# which often add little meaning to text-based machine learning models.

textData = spark.createDataFrame([
    (1, ["Spark", "is", "an", "open-source", "distributed", "computing", "system"]),
    (2, ["IT", "has", "interfaces", "for", "multiple", "languages"]),
    (3, ["It", "has", "a", "wide", "range", "of", "libraries", "and", "APIs"])
], ["id", "sentence"])

textData.show(truncate=False)

remover = StopWordsRemover(inputCol="sentence", outputCol="filtered_sentence")
textData = remover.transform(textData)

textData.show(truncate=False)


# -----------------------------------------------------------------------------
# 5. StringIndexer
# -----------------------------------------------------------------------------
# StringIndexer converts categorical text values into numeric indexes.

colors = spark.createDataFrame([
    (0, "red"),
    (1, "red"),
    (2, "blue"),
    (3, "yellow"),
    (4, "yellow"),
    (5, "yellow")
], ["id", "color"])

colors.show()

indexer = StringIndexer(inputCol="color", outputCol="colorIndex")
indexed = indexer.fit(colors).transform(colors)

indexed.show()


# -----------------------------------------------------------------------------
# 6. StandardScaler
# -----------------------------------------------------------------------------
# StandardScaler scales numeric features so that values with larger ranges do not
# dominate the model.

data = [
    (1, Vectors.dense([70, 170, 17])),
    (2, Vectors.dense([80, 165, 25])),
    (3, Vectors.dense([65, 150, 135]))
]

df = spark.createDataFrame(data, ["id", "features"])
df.show()

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures",
    withStd=True,
    withMean=True
)

scalerModel = scaler.fit(df)
scaledData = scalerModel.transform(df)

scaledData.show(truncate=False)


# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------

spark.stop()

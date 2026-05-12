def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

import pandas as pd
from sklearn.linear_model import LinearRegression

# the data set is available at the url below.
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0231EN-SkillsNetwork/datasets/mpg.csv"

# using the read_csv function in the pandas library, we load the data into a dataframe.

df = pd.read_csv(URL)

# show 5 random rows from the dataset
df.sample(5)

df.shape

print(df.plot.scatter(x = "Horsepower", y = "MPG"))

# First we identify the target. Target is the value that our machine learning model needs to predict
target = df["MPG"]

# We identify the features next. Features are the values our machine learning model learns from
features = df[["Horsepower","Weight"]]

# Create a LR model
lr = LinearRegression()

# Train/Fit the model
lr.fit(features,target)

# Your model is now trained. Time to evaluate the model.

#Higher the score, better the model.
lr.score(features,target)

# Make predictions. Let us predict the mileage for a car with HorsePower = 100 and Weight = 2000
lr.predict([[100,2000]])
print(lr.predict([[100,2000]]))
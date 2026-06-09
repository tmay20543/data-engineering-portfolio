def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

import pandas as pd
from sklearn.linear_model import LinearRegression

#import functions for train test split

from sklearn.model_selection import train_test_split

# import functions for metrics
from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from math import sqrt

# the data set is available at the url below.
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0231EN-SkillsNetwork/datasets/mpg.csv"

# using the read_csv function in the pandas library, we load the data into a dataframe.

df = pd.read_csv(URL)

# show 5 random rows from the dataset
df.sample(5)
df.shape

df.plot.scatter(x = "Weight", y = "MPG")

y = df["MPG"] # y is the target

X = df[["Horsepower","Weight"]] # X is the set of features

# We split the data set in the ratio of 70:30. 70% training data, 30% testing data.
# The random_state variable controls the shuffling applied to the data before applying the split. Pass the same integer for reproducible output across multiple function calls
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)

lr = LinearRegression()

# Train/Fit the model using the training data set
lr.fit(X_train,y_train)

# --- Evaluate the model ---
# Your model is now trained. We use the testing data to evaluate the model.
lr.score(X_test, y_test)

# To compute the detailed metrics we need two values, the original mileage and the predicted mileage.
original_values = y_test
predicted_values = lr.predict(X_test)

r2_score(original_values, predicted_values) # Higher the value the better the model

mean_squared_error(original_values, predicted_values) # Lower the value the better the model

sqrt(mean_squared_error(original_values, predicted_values)) # Lower the value the better the model

mean_absolute_error(original_values, predicted_values) # Lower the value the better the model
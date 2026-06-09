def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

import pandas as pd
from sklearn.linear_model import LinearRegression


URL2 = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0231EN-SkillsNetwork/datasets/diamonds.csv"

df2 = pd.read_csv(URL2)

target = df2["price"]
features = df2[["carat","depth"]]

lr2 = LinearRegression()
lr2.fit(features,target)

lr2.score(features,target)

lr2.predict([[0.3, 60]])
print(lr2.predict([[0.3, 60]]))
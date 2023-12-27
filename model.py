from sklearn import linear_model
from sklearn.metrics import mean_squared_error , r2_score
from math import sqrt
import pandas as pd
from sklearn.model_selection import train_test_split
from players import *
from scipy.stats import poisson

def RunLinearModel(trainx,trainy,testx,testy):
    lm = linear_model.LinearRegression()
    lm.fit(trainx,trainy)
    yhat = lm.predict(testx)
    print("RMSE = ",sqrt(mean_squared_error(testy,yhat)))
    print("R^2= ",r2_score(testy,yhat))
    return lm, yhat

def build_TrainTest(df,column):
    Y = df.pop(column)
    X = df
    x_train,x_test,y_train,y_test = train_test_split(X,Y, test_size =.25, random_state =1)
    return x_train,x_test,y_train,y_test

def poisson_dist(test_value, average):
    print(f"probability of {test_value} point", poisson.pmf(k=test_value,mu=average))
    print(f"probability of less than {test_value} point", poisson.cdf(k=test_value,mu=average))
    print(f"probability of greater than {test_value} point", 1-poisson.cdf(k=test_value,mu=average))

# a_edwards = Player('Anthony Edwards','Minnesota')
# min_df = a_edwards.player_minutes()
# x_train,x_test,y_train,y_test = build_TrainTest(min_df)
# model,y_pred = RunLinearModel(x_train,y_train,x_test,y_test)
# print('mins for ant edwards = ', y_pred)
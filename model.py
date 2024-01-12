from sklearn.linear_model import Ridge, LinearRegression
from sklearn.metrics import mean_squared_error , r2_score
from math import sqrt
from sklearn.model_selection import train_test_split
from scipy.stats import poisson
import pandas as pd
import matplotlib.pyplot as plt 

def RunLinearModel(trainx,trainy,testx,testy):
    lm = LinearRegression()
    lm.fit(trainx,trainy)
    yhat = lm.predict(testx)
    print("RMSE = ",sqrt(mean_squared_error(testy,yhat)))
    print("R^2= ",r2_score(testy,yhat))
    return lm, yhat

def run_ridge_model(stats_df, year : int, predictors : list, stat_column: str):
    train = stats_df[stats_df['SEASON_ID'] < year]
    test = stats_df[stats_df['SEASON_ID'] >- year]
    reg = Ridge(alpha=0.1)
    reg.fit(train[predictors], train)

def build_TrainTest(df,column):
    Y = df.pop(column)
    X = df
    x_train,x_test,y_train,y_test = train_test_split(X,Y, test_size =.25, random_state =1)
    return x_train,x_test,y_train,y_test

def poisson_dist(test_value, average):
    print(f"probability of {test_value} point", poisson.pmf(k=test_value,mu=average))
    print(f"probability of less than {test_value} point", poisson.cdf(k=test_value,mu=average))
    print(f"probability of greater than {test_value} point", 1-poisson.cdf(k=test_value,mu=average))

def linear_regression(csv_name : str, stat : str):
    stats = pd.read_csv(csv_name)
    player_df= stats.drop(columns=['SEASON_ID','OPPONENT','GAME_DATE','LOCATION'])
    x_train,x_test,y_train,y_test = build_TrainTest(player_df,stat)
    model,y_pred = RunLinearModel(x_train,y_train,x_test,y_test)
    plt.scatter(y_pred ,y_test)
    plt.xlabel('minutes')
    plt.ylabel('predicted points')
    plt.show()

    return model
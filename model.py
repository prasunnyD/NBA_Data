from sklearn.linear_model import Ridge, LinearRegression
from sklearn.metrics import mean_squared_error , r2_score
from math import sqrt
from sklearn.model_selection import train_test_split
from scipy.stats import poisson
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import boto3

def RunLinearModel(trainx,trainy,testx,testy):
    lm = LinearRegression()
    lm.fit(trainx,trainy)
    yhat = lm.predict(testx)
    print("RMSE = ",sqrt(mean_squared_error(testy,yhat)))
    print("R^2= ",r2_score(testy,yhat))
    return lm, yhat

def run_ridge_model(stats_df, year : int, predictors : list, stat_column: str, model_filename: str):
    """
    Parameters:
        stats_df (dataframe): dataframe of the player's stats that will be trained on
        year (integer): model will train on years less than defined year 
        predictors (list): features the model will train on
        stat_column (string): stat that is being predicted
        model_filename (string): 

    """
    train = stats_df[stats_df['SEASON_ID'] < year]
    train = train.drop(columns=['SEASON_ID','OPPONENT','GAME_DATE','LOCATION'])
    test = stats_df[stats_df['SEASON_ID'] >= year]
    reg = Ridge(alpha=0.1)
    reg.fit(train[predictors], train)
    predictions = reg.predict(test[predictors])
    predictions = pd.DataFrame(predictions,columns=['predictions','a','b','c','d','e'])
    comparison = pd.concat([test[['GAME_DATE','OPPONENT',stat_column]],predictions],axis=1)
    save_model_upload_s3(reg,model_filename)
    return comparison

def build_TrainTest(df,column):
    Y = df.pop(column)
    X = df
    x_train,x_test,y_train,y_test = train_test_split(X,Y, test_size =.25, random_state =1)
    return x_train,x_test,y_train,y_test

def poisson_dist(test_value : float, average : int):
    """
    Gives a probiliity for the oddsmaker line against the predicted value
    Parameters:
        test_value (float): oddsmaker line
        average (int): predicted value
    """
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

def save_model_upload_s3(model, model_filename : str):
    """
    Uploads model to s3 bucket
    Parameters:
        model
        model_filename
    """
    joblib.dump(model,model_filename)
    s3 = boto3.client("s3")
    s3.upload_file(model_filename,'prasun-nba-model',model_filename)
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.metrics import mean_squared_error , r2_score
from math import sqrt
from sklearn.model_selection import train_test_split
from scipy.stats import poisson
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import boto3
import polars as pl
import logging


def RunLinearModel(trainx,trainy,testx,testy):
    lm = LinearRegression()
    lm.fit(trainx,trainy)
    yhat = lm.predict(testx)
    print("RMSE = ",sqrt(mean_squared_error(testy,yhat)))
    print("R^2= ",r2_score(testy,yhat))
    return lm, yhat


def run_ridge_model(stats_df: pl.DataFrame, year: str, predictors: list, stat_column: str, model_filename: str) -> pl.DataFrame:
    """
    Runs a Ridge regression model on player stats data and returns prediction comparisons.

    Parameters:
        stats_df (pl.DataFrame): DataFrame containing player game stats with columns:
            - SEASON_ID: Season identifier (e.g. '22023')
            - OPPONENT: Opponent team abbreviation
            - GAME_DATE: Date of game
            - LOCATION: Home/Away
            - [predictors]: Feature columns used for training
            - [stat_column]: Target stat being predicted
        year (str): Season ID threshold - model trains on seasons before this year
        predictors (list): List of feature column names to use for training
        stat_column (str): Name of target stat column to predict
        model_filename (str): Filename to save trained model under

    Returns:
        pl.DataFrame: Comparison DataFrame containing:
            - GAME_DATE: Date of game
            - OPPONENT: Opponent team
            - [stat_column]: Actual stat value
            - PREDICTED: Model's predicted value

    The function:
    1. Splits data into train (seasons < year) and test (seasons >= year) sets
    2. Trains a Ridge regression model on the training data
    3. Makes predictions on the test set
    4. Creates a comparison DataFrame of actual vs predicted values
    5. Saves the trained model to S3 bucket

    Example:
        >>> df = pl.DataFrame({
                'SEASON_ID': ['22022', '22023'],
                'OPPONENT': ['MIN', 'LAL'],
                'GAME_DATE': ['2022-10-01', '2023-01-15'],
                'LOCATION': ['Home', 'Away'],
                'MIN': [35.2, 32.1],
                'PTS': [28, 25]
            })
        >>> results = run_ridge_model(
                stats_df=df,
                year='22023',
                predictors=['MIN'],
                stat_column='PTS',
                model_filename='player_points_model.sav'
            )
    """
    # Split into train and test sets
    train = (stats_df
             .filter(pl.col('SEASON_ID') < year)
             .drop(['SEASON_ID', 'OPPONENT', 'GAME_DATE', 'LOCATION']))
    
    test = stats_df.filter(pl.col('SEASON_ID') >= year)
    
    # Prepare X and y for training
    X_train = train.select(predictors).to_numpy()
    y_train = train.select(stat_column).to_numpy().ravel()
    
    # Train model
    reg = Ridge(alpha=0.1)
    reg.fit(X_train, y_train)
    
    # Make predictions
    X_test = test.select(predictors).to_numpy()
    predictions = reg.predict(X_test)
    
    # Create comparison DataFrame
    comparison = (test
                 .select(['GAME_DATE', 'OPPONENT', stat_column])
                 .with_columns(pl.Series(name='PREDICTED', values=predictions)))
    
    # Save model
    save_model_upload_s3(reg, model_filename)
    
    return comparison

def build_TrainTest(df : pl.DataFrame, column : str):
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
    less_than = poisson.cdf(k=test_value,mu=average)
    greater_than = 1-less_than
    return less_than, greater_than

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

def save_model_upload_s3(model, model_filename: str):
    """
    Uploads model to s3 bucket
    Parameters:
        model: trained sklearn model
        model_filename (str): name for saving the model
    """
    logging.info(f"Saving model to {model_filename}")
    joblib.dump(model, model_filename)
    
    logging.info(f"Uploading model to S3")
    s3 = boto3.client("s3")
    s3.upload_file(model_filename, 'prasun-nba-model', model_filename)
    logging.info(f"Model upload complete")
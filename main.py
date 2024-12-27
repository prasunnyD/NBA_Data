from players import Player
from teams import Team, abrv_team_dict
from model import *
from io import BytesIO

import pandas as pd
import time
import joblib
import boto3
import numpy as np
import polars as pl
import logging
from util import Database
import duckdb
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Set the format of log messages
    handlers=[
        logging.StreamHandler()  # Output to terminal
    ]
)


def opp_data_polars(df : pl.DataFrame) -> pl.DataFrame:
    """
    This function processes a Polars DataFrame containing game data and enriches it with opponent team statistics.
    It handles API rate limiting and retries through a retry mechanism.

    Parameters:
        df (pl.DataFrame): Input DataFrame containing game data with columns:
            - OPPONENT: Team abbreviation (e.g. 'MIN', 'LAL')
            - SEASON_ID: Season identifier (e.g. '22023')

    Returns:
        pl.DataFrame: Original DataFrame augmented with opponent team statistics:
            - OPP_EFG_PCT: Opponent effective field goal percentage
            - OPP_FTA_RATE: Opponent free throw attempt rate  
            - OPP_OREB_PCT: Opponent offensive rebound percentage
            - PACE: Game pace
            - DEF_RATING: Defensive rating

    The function:
    1. Filters data to seasons >= 2018-19
    2. Creates a lookup dictionary of team stats to avoid redundant API calls
    3. Implements retry logic for API requests with exponential backoff
    4. Uses vectorized operations to efficiently add opponent stats columns
    
    Example:
        df = pl.DataFrame({
            'OPPONENT': ['MIN', 'LAL'],
            'SEASON_ID': ['22023', '22023']
        })
        enriched_df = opp_data_polars(df)
    """
    team_dict = {}
    df = df.filter(pl.col('SEASON_ID') >= '22018')
    unique_combos = df.select(['OPPONENT', 'SEASON_ID']).unique()
    logging.info(f"Creating team dictionary...")

    def get_team_stats_with_retry(city, season_id, max_retries=3, wait_time=1):
        for attempt in range(max_retries):
            try:
                team = Team(city)
                team_df = team.get_team_opp_efga(season_id)  #TODO: This can come from the team_boxscores table
                adv_stats_df = team.get_team_adv_stats(season_id) #TODO: This can come from the team_boxscores table
                return {
                    'OPP_EFG_PCT': team_df['OPP_EFG_PCT'][0],
                    'OPP_FTA_RATE': team_df['OPP_FTA_RATE'][0],
                    'OPP_OREB_PCT': team_df['OPP_OREB_PCT'][0],
                    'PACE': adv_stats_df['PACE'][0],
                    'DEF_RATING': adv_stats_df['DEF_RATING'][0]
                }
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for {city}: {str(e)}")
                if attempt < max_retries - 1:
                    logging.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"All retries failed for {city}")
                    raise

    # Build the lookup dictionary with retry logic
    team_dict = {}
    for opp, season_id in unique_combos.iter_rows():
        city = abrv_team_dict(opp)
        logging.info(f"Processing {city} for season {season_id}")
        team_dict[(opp, season_id)] = get_team_stats_with_retry(city, season_id)

    logging.info(f"Team dictionary created...")

    def lookup_stat(opp, season, stat):
        return team_dict.get((opp, season), {})[stat]

    # Add columns using vectorized operations
    df = df.with_columns([
        pl.struct(['OPPONENT', 'SEASON_ID']).map_elements(
            lambda x: lookup_stat(x['OPPONENT'], x['SEASON_ID'], 'OPP_EFG_PCT')
        ).alias('OPP_EFG_PCT'),
        pl.struct(['OPPONENT', 'SEASON_ID']).map_elements(
            lambda x: lookup_stat(x['OPPONENT'], x['SEASON_ID'], 'OPP_FTA_RATE')
        ).alias('OPP_FTA_RATE'),
        pl.struct(['OPPONENT', 'SEASON_ID']).map_elements(
            lambda x: lookup_stat(x['OPPONENT'], x['SEASON_ID'], 'OPP_OREB_PCT')
        ).alias('OPP_OREB_PCT'),
        pl.struct(['OPPONENT', 'SEASON_ID']).map_elements(
            lambda x: lookup_stat(x['OPPONENT'], x['SEASON_ID'], 'PACE')
        ).alias('PACE'),
        pl.struct(['OPPONENT', 'SEASON_ID']).map_elements(
            lambda x: lookup_stat(x['OPPONENT'], x['SEASON_ID'], 'DEF_RATING')
        ).alias('DEF_RATING')
    ])

    return df

def create_model(year: int, stats : pl.DataFrame, model_filename : str, stat : str, predictors: list):
    """
    TODO: Upload csv files to aws or make a database of players from the csv files
    """    
    results = run_ridge_model(stats_df=stats,year= year, predictors=predictors, model_filename=model_filename,stat_column=stat,)
    results.write_csv(f"{model_filename}.csv")


def predict_result_polars(model_filename : str, city: str, minutes: float):
    """
    Get model prediction for a player's performance against a specific opponent.

    Parameters:
        model_filename (str): Name of the saved model file in S3 bucket to use for prediction
        city (str): City name of the opponent team (e.g. 'Atlanta', 'Los Angeles Lakers')
        minutes (float): Projected minutes to be played by the player

    Returns:
        numpy.ndarray: Array containing the predicted stat value based on the model

    The function:
    1. Creates Team object for opponent team
    2. Gets opponent's last 5 games stats (Four Factors and Advanced)
    3. Combines stats into features DataFrame with projected minutes
    4. Loads model from S3 bucket
    5. Makes prediction using loaded model and features

    Example:
        >>> result = predict_result_polars('anthony_edwards_points_model.sav', 'Atlanta', 37.8)
        >>> print(result)
        array([[24.5]])
    """
    opp_team = Team(city)
    opp_df = opp_team.get_team_opp_efga('22024','5') #TODO: This can come from the team_boxscores table
    adv_stats_df = opp_team.get_team_adv_stats('22024','5') #TODO: This can come from the team_boxscores table  
    
    opp_df = (opp_df
        .with_columns([
            pl.lit(adv_stats_df.select('PACE').row(0)[0]).alias('PACE'),
            pl.lit(minutes).alias('MIN')
        ])
        .select(['OPP_EFG_PCT', 'OPP_FTA_RATE', 'OPP_OREB_PCT', 'PACE', 'MIN'])
    )
    
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket='prasun-nba-model',Key=model_filename)
    bytes_stream = BytesIO(response['Body'].read())
    loaded_model = joblib.load(bytes_stream)
    result = loaded_model.predict(opp_df)
    return result


if __name__ == "__main__":
    antman = Player('Anthony Edwards')
    # predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MIN']
    # query = f"SELECT GAME_DATE,PTS,MIN FROM player_boxscores WHERE Player_ID = '{antman.id}' LIMIT 10"
    # conn = duckdb.connect("player_boxscores.db")
    # pts_df = conn.sql(query).pl()
    # create_model(year='22022', stats=pts_df, predictors=predictors,stat='PTS',model_filename="anthony_edwards_points_model.sav")
    # results = predict_result_polars('anthony_edwards_points_model.sav','Atlanta',37.8)
    # print(results)



    query = f"SELECT GAME_DATE,PTS, AST, REB, MIN FROM player_boxscores WHERE Player_ID = '{antman.id}' ORDER BY game_id DESC LIMIT 30"
    conn = duckdb.connect("player_boxscores.db")
    player_game_logs = conn.sql(query).pl()
    # # response = player_game_logs.rows_by_key(key='GAME_DATE')
    # for key, value in response.items():
    response = {}
    for row in player_game_logs.iter_rows(named=True):
        game_date = row['GAME_DATE']
        response[game_date] = {
            'points': float(row['PTS']),
            'assists': float(row['AST']),
            'rebounds': float(row['REB']),
            'minutes': float(row['MIN'])
        }

    twolves = Team('Boston')
    roster = twolves.get_team_roster()['PLAYER'].to_list()
    try:
        for name in roster:
            player = Player(name)
            player.create_player_boxscore_table(conn)
    except Exception as e:
        print(f"Error creating player boxscore table for {name}: {e}")

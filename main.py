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

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Set the format of log messages
    handlers=[
        logging.StreamHandler()  # Output to terminal
    ]
)

def make_player_csv(player : Player, csv_name : str, stat : str) -> pd.DataFrame:
   """
   Creates a csv file of season id, game played, opponent, desired stat, rolling average minutes
   Parameters:
        player (obj): player object that you want to get stats for
        csv_name (string): csv file name
        stat (string): desired stats to model. Usually in abreviated format like 'PTS'
   """
   points_df = player.player_stat(stat)
   stats_df = opp_data(points_df)
   minutes_df =  player.player_minutes()
   stats_df['MINUTES'] = minutes_df['prev_3_avg']
   stats_df.dropna(inplace=True)
   stats_df.to_csv(csv_name, index = False)
   s3 = boto3.client("s3")
   s3.upload_file(csv_name,'s3://prasun-nba-model/csv_files/',csv_name)
   return stats_df

def make_player_csv_polars(player : Player, csv_name : str, stat : str) -> pl.DataFrame:
    points_df = player.player_stat_polars(stat)
    stats_df = opp_data_polars(points_df)
    stats_df.write_csv(csv_name)
    # minutes_df = player.player_minutes_polars()
    # stats_df = (stats_df
    #             .with_columns(minutes_df.select('prev_3_avg').rename({'prev_3_avg': 'MINUTES'}))
    #             .drop_nulls()
    #             .write_csv(csv_name))
    
    return stats_df


def opp_data(df : pd.DataFrame) -> pd.DataFrame:
    """
    Gets opponent Four Factor stats for each individual game to train against.
    TODO: Takes some time to get this data. Figure out how to make it more efficient
    """
    opponents_efga_list = []
    opponents_free_throw_rate_list = []
    opponents_orebounding_pct_list = []
    pace_list = []
    def_rtg_list= []
    team_dict = {}
    df = df.drop(df[df['SEASON_ID'] < '22018'].index)
    for opp, season_id in zip(df['OPPONENT'],df['SEASON_ID']):
        if opp in team_dict:
            team_df,adv_stats_df = team_dict[opp]
        else:
            city = abrv_team_dict(opp)
            team = Team(city)
            team_df = team.get_team_opp_efga(season_id)
            adv_stats_df = team.get_team_adv_stats(season_id)
            team_dict[opp] = (team_df,adv_stats_df)
        opponents_efga_list.append(team_df['OPP_EFG_PCT'][0])
        opponents_free_throw_rate_list.append(team_df['OPP_FTA_RATE'][0])
        opponents_orebounding_pct_list.append(team_df['OPP_OREB_PCT'][0])
        pace_list.append(adv_stats_df['PACE'][0])
        def_rtg_list.append(adv_stats_df['DEF_RTG'][0])
        time.sleep(1)

    df["OPP_EFG_PCT"]= np.array(opponents_efga_list)
    df["OPP_FTA_RATE"]= np.array(opponents_free_throw_rate_list)
    df["OPP_OREB_PCT"]= np.array(opponents_orebounding_pct_list)
    df['PACE'] = np.array(pace_list)

    return df

def opp_data_polars(df : pl.DataFrame) -> pl.DataFrame:
    """
    Gets opponent Four Factor stats for each individual game using Polars DataFrame operations.
    """
    team_dict = {}
    df = df.filter(pl.col('SEASON_ID') >= '22018')
    unique_combos = df.select(['OPPONENT', 'SEASON_ID']).unique()
    logging.info(f"Creating team dictionary...")

    def get_team_stats_with_retry(city, season_id, max_retries=3, wait_time=1):
        for attempt in range(max_retries):
            try:
                team = Team(city)
                team_df = team.get_team_opp_efga(season_id)
                adv_stats_df = team.get_team_adv_stats(season_id)
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

def dataset_creator(player:Player, team: Team) -> pd.DataFrame:
    player_stats = player_pts(player,'2023-24')
    team_stats = team_stat(team,'2023-24')
    team_stats = team_stats.drop(columns=['LOCATION',"OPPONENT"])
    df = player_stats.merge(team_stats,on=['GAME_ID','SEASON_YEAR','GAME_DATE'])
    return df

def create_model(year: int, stats : pl.DataFrame, model_filename : str, stat : str, predictors: list):
    """
    TODO: Upload csv files to aws or make a database of players from the csv files
    """
    # make_player_csv(player, csv_name, stat)
    # stats = pd.read_csv(csv_name)
    
    results = run_ridge_model_polars(stats_df=stats,year= year, predictors=predictors, model_filename=model_filename,stat_column=stat,)
    results.write_csv(f"{model_filename}.csv")

def predict_result(model_filename : str, city: str, minutes: float):
    """
    Get model prediction
    Parameters:
        model_filename (string): Desired model to predict results
        city (string): opponent team city
        minutes (float): projected minutes played by player
    """
    opp_team = Team(city)
    opp_df = opp_team.get_team_opp_efga('22024','5')
    adv_stats_df = opp_team.get_team_adv_stats('22024','5')
    opp_df['PACE'] = adv_stats_df['PACE'][0]
    opp_df['MINUTES']= minutes
    opp_df = opp_df[['OPP_EFG_PCT',"OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MINUTES']]
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket='prasun-nba-model',Key=model_filename)
    bytes_stream = BytesIO(response['Body'].read())
    loaded_model = joblib.load(bytes_stream)
    result = loaded_model.predict(opp_df)
    return result

def predict_result_polars(model_filename : str, city: str, minutes: float):
    """
    Get model prediction
    Parameters:
        model_filename (string): Desired model to predict results
        city (string): opponent team city
        minutes (float): projected minutes played by player
    """
    opp_team = Team(city)
    opp_df = opp_team.get_team_opp_efga('22024','5')
    adv_stats_df = opp_team.get_team_adv_stats('22024','5')
    
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

def team_stat(team : Team, season : str) -> pd.DataFrame:
    '''
    Creates a dataframe for training purposes
    TODO get opponents stats, home and away. Potentially get rid of stat?
    '''
    boxscore_df = team.get_team_game_log(season=season)
    home_list = ["Away" if '@' in x else "Home" for x in boxscore_df['MATCHUP']]
    opp_team = [x.split()[2] for x in boxscore_df['MATCHUP']]
    pts_df=boxscore_df[['SEASON_YEAR','GAME_ID','GAME_DATE','OPP_EFG_PCT','OPP_FTA_RATE','OPP_OREB_PCT','DEF_RATING','PACE']].copy()
    pts_df['LOCATION'] = home_list
    pts_df['OPPONENT'] = opp_team
    pts_df.dropna(inplace=True)
    return pts_df

def player_pts(player : Player, season: str) -> pd.DataFrame:
    boxscore_df = player.player_boxscores(season=season)
    home_list = ["Away" if '@' in x else "Home" for x in boxscore_df['MATCHUP']]
    opp_team = [x.split()[2] for x in boxscore_df['MATCHUP']]
    pts_df=boxscore_df[['SEASON_YEAR','GAME_ID','GAME_DATE','PTS','TS_PCT','MIN']].copy()
    pts_df['LOCATION'] = home_list
    pts_df['OPPONENT'] = opp_team
    pts_df.dropna(inplace=True)
    return pts_df


if __name__ == "__main__":
    antman = Player('Anthony Edwards','Minnesota')
    # pts_df = make_player_csv_polars(player=antman,csv_name='anthony_edwards_pts.csv',stat='PTS')
    # predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MIN']
    # create_model(year='22022', stats=pts_df, predictors=predictors,stat='PTS',model_filename="anthony_edwards_points_model.sav")
    # results = predict_result_polars('anthony_edwards_points_model.sav','Atlanta',37.8)
    # print(results)
    conn = duckdb.connect("player_boxscores.db")
    # antman.create_player_boxscore_table(conn)
    player_table = conn.query("SELECT * FROM player_boxscores").pl()
    print(player_table.columns)


    query = f"SELECT GAME_DATE,PTS,MIN FROM player_boxscores WHERE Player_ID = '{antman.id}' LIMIT 10"
    conn = duckdb.connect("player_boxscores.db")
    player_game_logs = conn.sql(query).pl()
    response = player_game_logs.rows_by_key(key='GAME_DATE')

    twolves = Team('Minnesota')
    twolves.create_team_table(conn, season='2020-21')
    print(conn.sql("SELECT * FROM team_boxscores"))
    # a_edwards = Player('Anthony Edwards', 'Minnesota')
    # twolves = Team('Minnesota')
    # df = dataset_creator(a_edwards,twolves)
    # print(df.columns)
    # df.to_csv("testing_file.csv")
    # bx23_df = twolves.get_team_game_log()
    # bx22_df = twolves.get_team_game_log(season='2022-23')
    # bx23_df = pd.concat([bx23_df,bx22_df], ignore_index=True)
    # bx23_df.to_csv('testing_file2.csv')

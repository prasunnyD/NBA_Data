from players import Player
from teams import Team, abrv_team_dict
from model import *
from io import BytesIO

import pandas as pd
import time
import joblib
import boto3



def make_player_csv(player, csv_name : str, stat : str):
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


def opp_data(df):
    """
    Gets opponent Four Factor stats for each individual game to train against.
    TODO: Takes some time to get this data. Figure out how to make it more efficient
    """
    opponents_efga_list = list()
    opponents_free_throw_rate_list = list()
    opponents_orebounding_pct_list = list()
    pace_list = list()
    team_dict = dict()
    for opp, season_id in zip(df['OPPONENT'],df['SEASON_ID']):
        if opp in team_dict:
            team_df = team_dict[opp]
        else:
            city = abrv_team_dict(opp)
            team = Team(city)
            team_df = team.get_team_opp_efga(season_id)
            adv_stats_df  = team.get_team_adv_stats(season_id)
            team_dict.update({opp:team_df})
        time.sleep(1)
        opponents_efga_list.append(team_df['OPP_EFG_PCT'][0])
        opponents_free_throw_rate_list.append(team_df['OPP_FTA_RATE'][0])
        opponents_orebounding_pct_list.append(team_df['OPP_OREB_PCT'][0])
        pace_list.append(adv_stats_df['PACE'][0])
        print("SEASON_ID",season_id)

    df["OPP_EFG_PCT"]= opponents_efga_list
    df["OPP_FTA_RATE"]= opponents_free_throw_rate_list
    df["OPP_OREB_PCT"]= opponents_orebounding_pct_list
    df['PACE'] = pace_list

    return df

def create_model_from_scratch(player, year: int, csv_name : str, model_filename : str, stat : str, predictors: list):
    """
    TODO: Upload csv files to aws or make a database of players from the csv files
    """
    make_player_csv(player, csv_name, stat)
    stats = pd.read_csv(csv_name)
    results = run_ridge_model(stats_df=stats,year= year, predictors=predictors, model_filename=model_filename,stat_column=stat,)
    results.to_csv("test_results.csv", index = False)

def predict_result(model_filename : str, X_test: list):
    """
    Get model prediction
    Parameters:
        model_filename (string): Desired model to predict results
        X_test (list): List of variables that want to predict against
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket='prasun-nba-model',Key=model_filename)
    bytes_stream = BytesIO(response['Body'].read())
    loaded_model = joblib.load(bytes_stream)
    result = loaded_model.predict(X_test)
    return result


if __name__ == "__main__":
    edwards = Player('Anthony Edwards','Minnesota')
    timberwolves = Team('Minnesota')
    predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MINUTES']
    #create_model_from_scratch(player=edwards,csv_name="anthony_edwards_pts.csv",year=22022,predictors=predictors,stat='PTS',model_filename="anthony_edwards_points_model.sav")
    min_df = edwards.player_minutes().iloc[0]
    X_test = [[0.553,0.257,0.282,97.08,min_df['prev_3_avg']]]
    input = pd.DataFrame(X_test, columns=predictors)
    prediction = predict_result("anthony_edwards_points_model.sav", X_test=input)
    print("Projected Points: ",prediction[0][0])
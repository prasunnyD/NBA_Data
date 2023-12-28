from players import Player
from teams import Team, abrv_team_dict
from model import *
import matplotlib.pyplot as plt 
import pandas as pd
import time
import joblib

edwards = Player('Anthony Edwards','Minnesota')
timberwolves = Team('Minnesota')
min_df = edwards.player_minutes().iloc[0]
X_test = [[0.534,0.238,0.29,99.02,min_df['prev_3_avg']]]


def make_player_csv(player, csv_name : str, stat : str):
   points_df = player.player_stat(stat)
   stats_df = opp_data(points_df)
   minutes_df =  player.player_minutes()
   stats_df['MINUTES'] = minutes_df['prev_3_avg']
   stats_df.dropna(inplace=True)
   stats_df.to_csv(csv_name, index = False)

def linear_regression(csv_name : str, stat : str):
    stats = pd.read_csv(csv_name)
    print(stats)
    player_df= stats.drop(columns=['SEASON_ID','OPPONENT','GAME_DATE','LOCATION'])
    x_train,x_test,y_train,y_test = build_TrainTest(player_df,stat)
    model,y_pred = RunLinearModel(x_train,y_train,x_test,y_test)
    plt.scatter(y_pred ,y_test)
    plt.xlabel('minutes')
    plt.ylabel('predicted points')
    plt.show()

    return model

def opp_data(df):
    opponents_efga_list = list()
    opponents_free_throw_rate_list = list()
    opponents_orebounding_pct_list = list()
    pace_list = list()
    team_dict = dict()
    for opp, season_id in zip(df['OPPONENT'],df['SEASON_ID']):
        if opp in team_dict:
            team_df = team_dict[opp]
        else:
            team = abrv_team_dict(opp)
            team_df = team.get_team_opp_efga(season_id)
            adv_stats_df  = team.get_team_adv_stats(season_id)
            team_dict.update({opp:team_df})
        time.sleep(1)
        opponents_efga_list.append(team_df['OPP_EFG_PCT'][0])
        opponents_free_throw_rate_list.append(team_df['OPP_FTA_RATE'][0])
        opponents_orebounding_pct_list.append(team_df['OPP_OREB_PCT'][0])
        pace_list.append(adv_stats_df['PACE'][0])

    df["OPP_EFG_PCT"]= opponents_efga_list
    df["OPP_FTA_RATE"]= opponents_free_throw_rate_list
    df["OPP_OREB_PCT"]= opponents_orebounding_pct_list
    df['PACE'] = pace_list

    return df

def create_model_from_scratch(player, csv_name : str, model_filename : str, stat : str):
    make_player_csv(player, csv_name, stat)
    model =linear_regression(csv_name, stat)
    joblib.dump(model,model_filename)

def predict_result(model_filename : str, X_test: list):
    loaded_model = joblib.load(model_filename)
    result = loaded_model.predict(X_test)
    print(result)

# create_model_from_scratch(giannis,"giannis_pts.csv", "giannis_points_model.sav", "PTS")
predict_result("edwards_points_model.sav", X_test)
poisson_dist(31.5,32.47)
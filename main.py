from players import Player
from teams import Team, abrv_team_dict
from model import *
import matplotlib.pyplot as plt 
import pandas as pd
import time
import joblib

a_edwards = Player('Anthony Edwards','Minnesota')
timberwolves = Team('Minnesota')
# advance_stats = timberwolves.get_team_pace()
# advance_stats.to_csv("adv_team_stats.csv")
X_test = [[0.559,0.272,0.263,100.63,39]]


def make_player_csv(player, csv_name : str, stat : str):
   points_df = player.player_stat(stat)
   stats_df = opp_data(points_df)
   minutes_df =  player.player_minutes()
   stats_df['MINUTES'] = minutes_df['prev_3_avg']
   stats_df.dropna(inplace=True)
   stats_df.to_csv(csv_name, index = False)

def linear_regression(csv_name : str):
    stats = pd.read_csv(csv_name)
    print(stats)
    player_df= stats.drop(columns=['SEASON_ID','OPPONENT','GAME_DATE','LOCATION'])
    x_train,x_test,y_train,y_test = build_TrainTest(player_df,'PTS')
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
    opps = df['OPPONENT'] 
    for opp in opps:
        if opp in team_dict:
            team_df = team_dict[opp]
        else:
            team = abrv_team_dict(opp)
            team_df = team.get_team_opp_efga()
            adv_stats_df  = team.get_team_adv_stats()
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

def create_model_from_scratch(csv_name : str, model_filename : str, stat : str):
    make_player_csv(a_edwards, csv_name, stat)
    model =linear_regression(csv_name)
    joblib.dump(model,model_filename)

def predict_result(model_filename : str, X_test: list):
    loaded_model = joblib.load(model_filename)
    result = loaded_model.predict(X_test)
    print(result)

#create_model_from_scratch("edwards_pts.csv", "edwards_model.sav", "PTS")
predict_result("edwards_model.sav", X_test)
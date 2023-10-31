from players import Player
from teams import Team, abrv_team_dict
from model import *
import matplotlib.pyplot as plt 
import pandas as pd
import time

a_edwards = Player('Anthony Edwards','Minnesota')
timberwolves = Team('Minnesota')
def_stats = timberwolves.get_team_opp_efga()
def_stats.to_csv("def_team_stats.csv")
# a_edwards_df = a_edwards.player_career_boxscore()
# print(a_edwards_df)
# a_edwards_df.to_csv('ant_edwards.csv')
pts_df = a_edwards.player_points('PTS')
opps = pts_df['OPPONENT'] 
opponents_efga_list = list()
opponents_free_throw_rate_list = list()
opponents_orebounding_pct_list = list()
team_dict = dict()
for opp in opps:
    if opp in team_dict:
        team_df = team_dict[opp]
    else:
        team = abrv_team_dict(opp)
        team_df = team.get_team_opp_efga()
        team_dict.update({opp:team_df})
    time.sleep(1)
    opponents_efga_list.append(team_df['OPP_EFG_PCT'])
    opponents_free_throw_rate_list.append(team_df['OPP_FTA_RATE'])
    opponents_orebounding_pct_list.append(team_df['OPP_OREB_PCT'])
                               
      
# opponents_efga_list = [abrv_team_dict(opp).get_team_opp_efga() for opp in opps]
pts_df["OPP_EFG_PCT"]= opponents_efga_list
pts_df["OPP_FTA_RATE"]= opponents_free_throw_rate_list
pts_df["OPP_OREB_PCT"]= opponents_orebounding_pct_list
pts_df.to_csv("edwards_pts.csv")
stats = pd.read_csv("edwards_pts.csv")
edwards_df= pts_df.drop(columns=['SEASON_ID','OPPONENT','GAME_DATE','LOCATION'])
# home_away = pd.get_dummies(edwards_df, columns=['LOCATION'], dtype=int)
# print(home_away)
x_train,x_test,y_train,y_test = build_TrainTest(edwards_df,'PTS')
model,y_pred = RunLinearModel(x_train,y_train,x_test,y_test)
plt.scatter(y_pred ,y_test)
plt.xlabel('OPP_EFG_PCT')
plt.ylabel('Predicted points')
plt.show()

from prefect import flow, task
from teams import Team,abrv_team_dict, ABRV_TEAM_DICT

@task
def populate_team_data(conn, season : str):
    cities = ABRV_TEAM_DICT.values()
    for city in cities:
        team = Team(city)
        team.create_team_table(conn, season=season)

@task
def populate_player_data():

@flow
def populate_data():
    

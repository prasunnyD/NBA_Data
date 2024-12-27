from prefect import flow, task, get_run_logger
import duckdb
from teams import Team, ABRV_TEAM_DICT
from players import Player
from time import sleep
@task
def populate_team_data(conn, team: Team, season : str):
    team.create_team_table(conn, season=season)

@task
def populate_player_data(team: Team):
    logger = get_run_logger()
    roster = team.get_team_roster()['PLAYER'].to_list()
    logger.info(f"Populating player data for {team.city}")
    
    with duckdb.connect("player_boxscores.db") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                sleep(2)
                player.create_player_boxscore_table(conn)
            except Exception as e:
                logger.error(f"Error creating player boxscore table for {name}: {e}")

@flow()
def populate_data():
    cities = ABRV_TEAM_DICT.values()
    for city in cities:
        team = Team(city)
        # populate_team_data(conn, team, season='2024-25')
        populate_player_data(team)

if __name__ == "__main__":
    populate_data.serve(name='player_boxscores', cron='0 8 * * *')

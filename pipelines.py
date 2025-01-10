from prefect import flow, task, get_run_logger
import duckdb
from teams import Team, ABRV_TEAM_DICT, team_stats
from players import Player
from time import sleep
import os

MOTHERDUCK_TOKEN = os.environ.get('motherduck_token')


@task
def populate_team_data(conn, team: Team, season : str):
    team.create_team_table(conn, season=season)

@task
def populate_player_data(team: Team):
    logger = get_run_logger()
    roster = team.get_team_roster()
    logger.info(f"Populating player data for {team.city}")
    
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                sleep(2)
                player.create_player_boxscore_table(conn)
            except Exception as e:
                logger.error(f"Error creating player boxscore table for {name}: {e}")

@task
def populate_team_stats():
    logger = get_run_logger()
    logger.info("Getting teams stats...")
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        team_stats(conn, 'Opponent', 'teams_opponent_stats')
        team_stats(conn, 'Defense', 'teams_defense_stats')
        team_stats(conn, 'Four Factors', 'teams_four_factors_stats')
        team_stats(conn, 'Advanced', 'teams_advanced_stats')
    logger.info("Successfully populated teams stats...")

@task
def populate_player_shooting_splits(team : Team):
    logger = get_run_logger()
    roster = team.get_team_roster()
    logger.info(f"Populating player data for {team.city}")
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                player.create_player_shooting_splits_table(conn, team.id, "player_shooting_splits")
                sleep(2) 
            except Exception as e:
                logger.error(f"Error creating player boxscore table for {name}: {e}")

@flow()
def populate_data():
    populate_team_stats()
    
    cities = ABRV_TEAM_DICT.values()
    for city in cities:
        team = Team(city)
        # populate_team_data(conn, team, season='2024-25')
        populate_player_data(team)

if __name__ == "__main__":
    populate_data.serve(name='player_boxscores', cron='0 8 * * *')

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
def populate_player_boxscores(team: Team):
    logger = get_run_logger()
    roster_df = team.get_team_roster()
    roster= roster_df.select('PLAYER').to_series().to_list()
    logger.info(f"Populating player data for {team.city}")
    
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                # Add exponential backoff retry logic
                max_retries = 3
                base_wait = 2  # Base wait time in seconds
                
                for attempt in range(max_retries):
                    try:
                        player.create_player_boxscore_table(conn)
                        # If successful, wait standard rate limit time and break
                        sleep(2)
                        break
                    except Exception as e:
                        wait_time = base_wait ** (attempt + 1)  # Exponential backoff
                        if attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt + 1} failed for {name}. Retrying in {wait_time} seconds... Error: {e}")
                            sleep(wait_time)
                        else:
                            raise Exception(f"All retries failed for {name}")
                            
            except Exception as e:
                logger.error(f"Error creating player boxscore table for {name}: {e}")
                continue 

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
    roster_df = team.get_team_roster()
    roster= roster_df.select('PLAYER').to_series().to_list()
    logger.info(f"Populating player data for {team.city}")
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                max_retries = 3
                base_wait = 2
                for attempt in range(max_retries):
                    try:
                        player.create_player_shooting_splits_table(conn, team.id, "player_shooting_splits")
                        sleep(2) 
                        break
                    except Exception as e:
                        wait_time = base_wait ** (attempt + 1)  # Exponential backoff
                        if attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt + 1} failed for {name}. Retrying in {wait_time} seconds... Error: {e}")
                            sleep(wait_time)
                        else:
                            raise Exception(f"All {max_retries} attempts failed for {name}. Error: {e}")
            except Exception as e:
                logger.error(f"Error creating player shooting splits table for {name}: {e}")
                continue

@task
def populate_player_headline_stats(team : Team):
    logger = get_run_logger()
    roster_df = team.get_team_roster()
    roster= roster_df.select('PLAYER').to_series().to_list()
    logger.info(f"Populating player data for {team.city}")
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        for name in roster:
            try:
                logger.info(f"Populating {name} data")
                player = Player(name)
                max_retries = 3
                base_wait = 2
                for attempt in range(max_retries):
                    try:
                        player.create_player_headline_stats_table(conn, "player_headline_stats")
                        sleep(2)
                        break
                    except Exception as e:
                        wait_time = base_wait ** (attempt + 1)  # Exponential backoff
                        if attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt + 1} failed for {name}. Retrying in {wait_time} seconds... Error: {e}")
                            sleep(wait_time)
                        else:
                            raise Exception(f"All {max_retries} attempts failed for {name}. Error: {e}")
            except Exception as e:
                logger.error(f"Error creating player headline stats table for {name}: {e}")
                continue

@flow()
def populate_data():
    populate_team_stats()
    
    cities = ABRV_TEAM_DICT.values()
    for city in cities:
        team = Team(city)
        # populate_team_data(conn, team, season='2024-25')
        populate_player_boxscores(team)
        populate_player_shooting_splits(team)
        populate_player_headline_stats(team)
if __name__ == "__main__":
    populate_data.serve(name='player_boxscores', cron='0 8 * * *')
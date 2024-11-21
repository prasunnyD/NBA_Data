from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamPlayerDashboard, PlayerDashboardByGameSplits, LeagueGameLog, LeagueDashTeamStats, LeagueDashPtTeamDefend, TeamGameLogs
from nba_api.stats.library.parameters import Season
import polars as pl
import pandas as pd
from players import Player
from functools import lru_cache
import logging

class Team:
    def __init__(self, city : str) -> None:
        self.city = city
        # self.lineup = lineup
        self.id = self.get_team_id()

    def get_team_id(self) -> int:
        try:
            team_id = (teams.find_teams_by_city(self.city)[0])['id']
        except:
            team_id = (teams.find_teams_by_full_name(self.city)[0])['id']
        return team_id
    
    def get_team_stats(self) -> pd.DataFrame:
        """
        Not sure if keeping this function
        """
        team_stats =TeamPlayerDashboard(team_id=self.id).get_data_frames()[0]
        return team_stats
    

    # def get_team_stats2(self, season : str = None, season_type : str = "Regular Season", season_segment : str = None) -> pd.DataFrame:
    #     """
    #     Combines box score and advanced stats of team and returns dataframe of multiple seasons.
    #         Parameters:
    #             season (str)
    #     TODO: Make so can get single season from this to
    #     """
    #     regular_stats_columns = ['GROUP_VALUE','TEAM','GP','MIN','FGM','FGA','FG_PCT','FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']
    #     adv_stats_columns = ['GROUP_VALUE','TEAM','W_PCT','OFF_RATING','DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_RATIO', 'EFG_PCT', 'TS_PCT', 'POSS', 'PIE', 'PACE']
    #     stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
    #     stats['TEAM'] = self.city
    #     team_career_stats = stats[regular_stats_columns]
    #     adv_stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame",measure_type_detailed_defense="Advanced", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
    #     adv_stats['TEAM'] = self.city
    #     team_career_adv_stats = adv_stats[adv_stats_columns]
    #     final = mergeTables(team_career_stats,team_career_adv_stats)
    #     return final
    
    def get_team_lineup_stats(self, lineup : list[Player]) -> pd.DataFrame:
        return pd.concat([player.get_current_season_stats() for player in lineup])

    def team_scores(self):
        boxscore = LeagueGameLog().get_data_frames()[0]
        return boxscore
    
    # def team_rebounding_stats(self, season : str = None, season_type : str = "Regular Season", season_segment : str = None) -> pd.DataFrame:
    #     regular_stats_columns = ['GROUP_VALUE','TEAM','REB', 'OREB', 'DREB', 'OREB_RANK', 'DREB_RANK', 'REB_RANK']
    #     adv_stats_columns = ['GROUP_VALUE','TEAM','DEF_RATING', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'OREB_PCT_RANK', 'DREB_PCT_RANK', 'REB_PCT_RANK', 'POSS', 'PIE', 'E_PACE', 'PACE']
    #     stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
    #     stats['TEAM'] = self.city
    #     team_career_stats = stats[regular_stats_columns]
    #     adv_stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame",measure_type_detailed_defense="Advanced", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
    #     adv_stats['TEAM'] = self.city
    #     team_career_adv_stats = adv_stats[adv_stats_columns]
    #     final = mergeTables(team_career_stats,team_career_adv_stats)
    #     return final

    @staticmethod
    def get_season(season_id : str) -> str:
        year = season_id[-2:]
        year_dict={'23':'2023-24','22':'2022-23','21':'2021-22','20':'2020-21','19':'2019-20','18':'2018-2019','17':'2017-2018'}
        return year_dict.get(year)

    
    def get_team_opp_efga(self, season_id: str, last_number_games : str = "0") -> pl.DataFrame:
        """
        Parameters:
            season_id(string)
            last_number_games (string): Returns stats of last number of games
        Returns:
            stats (dataframe)
        """
        logging.info(f"Getting team opp efga for {self.city}...")
        season_year = self.get_season(season_id)
        stats = LeagueDashTeamStats(team_id_nullable=self.id,measure_type_detailed_defense='Four Factors',season=season_year, last_n_games=last_number_games, timeout=100).get_dict()
        stats_df = pl.DataFrame(stats['resultSets'][0]['rowSet'], schema=stats['resultSets'][0]['headers'])
        logging.info(f"Returning team opp efga for {self.city}...")
        return stats_df
    
    def get_team_adv_stats(self, season_id: str, last_number_games : str = "0") -> pl.DataFrame:
        """
        Parameters:
            season_id(string)
            last_number_games (string): Returns stats of last number of games
        Returns:
            stats (dataframe)
        """
        logging.info(f"Getting team adv stats for {self.city}...")
        season_year = self.get_season(season_id)
        stats = LeagueDashTeamStats(team_id_nullable=self.id,measure_type_detailed_defense='Advanced',season=season_year, last_n_games=last_number_games,timeout=100).get_dict()
        stats_df = pl.DataFrame(stats['resultSets'][0]['rowSet'], schema=stats['resultSets'][0]['headers'])
        logging.info(f"Returning team adv stats for {self.city}...")
        return stats_df
        
    def get_team_def(self):
        df = LeagueDashPtTeamDefend(team_id_nullable=self.id).get_data_frames()[0]

    def get_team_game_log(self, season=Season.current_season) -> pl.DataFrame:
        """
        Gets team game logs and merges four factors, advanced stats, and regular stats into a single polars DataFrame.

        Parameters:
            season (str): NBA season in format '2023-24'. Defaults to current season.

        Returns:
            pl.DataFrame: Combined DataFrame containing:
                - Four Factors stats (opponent data)
                - Advanced stats (team data) 
                - Regular box score stats
                - Location ('Home'/'Away') and opponent team columns

        The function:
        1. Gets Four Factors stats and drops unnecessary columns
        2. Gets Advanced stats and drops redundant columns
        3. Joins Four Factors and Advanced stats on common columns
        4. Gets regular box score stats
        5. Adds location and opponent columns derived from matchup info
        6. Joins all stats together into final DataFrame

        Example:
            team = Team('Minnesota')
            df = team.get_team_game_log_polars('2023-24')
        """
        # Four Factors - opponent data
        ff_df = TeamGameLogs(team_id_nullable=self.id, measure_type_player_game_logs_nullable='Four Factors',season_nullable=season).get_dict()
        ff_df = pl.DataFrame(ff_df['resultSets'][0]['rowSet'], schema=ff_df['resultSets'][0]['headers'])
        ff_df = ff_df.drop(["AVAILABLE_FLAG","MIN","GP_RANK","W_RANK","L_RANK","W_PCT_RANK","MIN_RANK"])
        # Advanced stats - team data
        adv_stats = TeamGameLogs(team_id_nullable=self.id, measure_type_player_game_logs_nullable='Advanced',season_nullable=season).get_dict()
        adv_df = pl.DataFrame(adv_stats['resultSets'][0]['rowSet'], schema=adv_stats['resultSets'][0]['headers'])
        adv_df = adv_df.drop(["AVAILABLE_FLAG","MIN","GP_RANK","W_RANK","L_RANK","W_PCT_RANK","MIN_RANK","OREB_PCT","EFG_PCT","TM_TOV_PCT","OREB_PCT_RANK","TM_TOV_PCT_RANK","EFG_PCT_RANK"])

        df =ff_df.join(adv_df,on=['GAME_ID','SEASON_YEAR','TEAM_ID','GAME_DATE','MATCHUP', 'TEAM_ABBREVIATION', 'WL','TEAM_NAME'],how="inner")

        reg_stats = TeamGameLogs(team_id_nullable=self.id,season_nullable=season).get_dict()
        reg_df = pl.DataFrame(reg_stats['resultSets'][0]['rowSet'], schema=reg_stats['resultSets'][0]['headers'])
        reg_df = reg_df.drop(["AVAILABLE_FLAG","MIN","GP_RANK","W_RANK","L_RANK","W_PCT_RANK","MIN_RANK"])
        home_list = ["Away" if '@' in x else "Home" for x in reg_df['MATCHUP']]
        opp_team = [x.split()[2] for x in reg_df['MATCHUP']]
        reg_df = reg_df.with_columns([
            pl.Series(name="LOCATION", values=home_list),
            pl.Series(name="OPPONENT", values=opp_team)
        ])

        team_df = df.join(reg_df,on=['GAME_ID','SEASON_YEAR','TEAM_ID','GAME_DATE','MATCHUP', 'TEAM_ABBREVIATION', 'WL','TEAM_NAME'],how="inner")
        return team_df
        
    
    def create_team_table(self, conn, season=Season.current_season):
        """
        Creates/updates a DuckDB table with team boxscore data.
        
        Parameters:
            conn: DuckDB connection object
            season: NBA season to get data for (defaults to current season)
        """
        logging.info(f"Getting boxscores for {self.city}...")
        
        # Get team data
        team_df = self.get_team_game_log(season)
        
        # Check if table exists
        table_exists = conn.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'team_boxscores'
            )
        """).fetchone()[0]
        
        if not table_exists:
            logging.info("Creating team_boxscores table...")
            # Create table with schema from team_df
            conn.execute("CREATE TABLE team_boxscores AS SELECT * FROM team_df WHERE 1=0")
            
        # Register the polars dataframe as a view
        conn.register('team_boxscores_view', team_df)
        
        # Insert new records that don't already exist
        conn.execute("""
            INSERT INTO team_boxscores 
            SELECT * FROM team_boxscores_view 
            WHERE NOT EXISTS (
                SELECT 1 FROM team_boxscores t 
                WHERE t.GAME_ID = team_boxscores_view.GAME_ID
                AND t.TEAM_ID = team_boxscores_view.TEAM_ID
            )
        """)

        logging.info(f"Successfully updated boxscores for {self.city} in team_boxscores table")
    
    #TODO TeamAndPlayersVsPlayers CAN BE USED FOR LINEUP COMPARISON

@lru_cache(maxsize=None)
def abrv_team_dict(team : str):
    """
    Dictionary of all the city with their abbreviations
    """    
    abrv_team_dict = {'ATL': "Atlanta", 'BKN': 'Brooklyn', 'BOS': 'Boston', 'CHA': 'Charlotte', 'CHI': 'Chicago', 'CLE': 'Cleveland', 'DAL': 'Dallas', 'DEN': 'Denver', 'DET': 'Detroit', 'GSW': 'Golden State',
                      'HOU': "Houston", 'IND': 'Indiana', 'MEM': 'Memphis', 'MIA': 'Miami', 'MIL': 'Milwaukee', 'MIN': 'Minnesota', 'NOP': 'New Orleans', 'NYK': 'New York', 'LAC': 'Los Angeles Clippers', 'LAL': 'Los Angeles Lakers', 
                      'OKC': 'Oklahoma City', 'ORL': 'Orlando', 'PHI': 'Philadelphia', 'PHX': 'Phoenix', 'POR': 'Portland', 'SAC': 'Sacramento', 'SAS': 'San Antonio', 'TOR': 'Toronto', 'UTA': 'Utah', 'WAS': 'Washington'}

    return abrv_team_dict.get(team)





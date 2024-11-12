from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamPlayerDashboard, PlayerDashboardByGameSplits, LeagueGameLog, LeagueDashTeamStats, LeagueDashPtTeamDefend, TeamGameLogs
from nba_api.stats.library.parameters import Season
from util import mergeTables
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

    def get_team_opp_efga(self, season_id: str, last_number_games : str = "0") -> pd.DataFrame:
        """
        Parameters:
            season_id(string)
            last_number_games (string): Returns stats of last number of games
        Returns:
            stats (dataframe)
        """
        season_year = self.get_season(season_id)
        stats = LeagueDashTeamStats(team_id_nullable=self.id,measure_type_detailed_defense='Four Factors',season=season_year, last_n_games=last_number_games, timeout=100).get_data_frames()[0]
        return stats
    
    def get_team_opp_efga_polars(self, season_id: str, last_number_games : str = "0") -> pl.DataFrame:
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

    def get_team_adv_stats(self, season_id: str, last_number_games : str = "0") -> pd.DataFrame:
        """
        Parameters:
            season_id(string)
            last_number_games (string): Returns stats of last number of games
        Returns:
            stats (dataframe)
        """
        season_year = self.get_season(season_id)
        stats = LeagueDashTeamStats(team_id_nullable=self.id,measure_type_detailed_defense='Advanced',season=season_year, last_n_games=last_number_games,timeout=100).get_data_frames()[0]
        return stats
    
    def get_team_adv_stats_polars(self, season_id: str, last_number_games : str = "0") -> pl.DataFrame:
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

    def get_team_game_log(self, season=Season.current_season) -> pd.DataFrame:
        """
        Merges the team four factor and advance box scores into one dataframe, that can used for training.
        Only returns one season at a time. 
        Parameters:
            season (string): format 2023-24
        Returns:
            dataframe
        """
        ff_df = TeamGameLogs(team_id_nullable=self.id, measure_type_player_game_logs_nullable='Four Factors',season_nullable=season).get_data_frames()[0]
        ff_df.drop(list(ff_df.filter(regex='RANK')), axis=1, inplace=True)
        ff_df = ff_df.drop(columns=['TEAM_NAME',"AVAILABLE_FLAG","MIN"])
        adv_df = TeamGameLogs(team_id_nullable=self.id, measure_type_player_game_logs_nullable='Advanced',season_nullable=season).get_data_frames()[0]
        adv_df.drop(list(adv_df.filter(regex='RANK')), axis=1, inplace=True)
        adv_df = adv_df.drop(columns=['TEAM_NAME','EFG_PCT','TM_TOV_PCT','OREB_PCT',"AVAILABLE_FLAG","MIN"])
        df = ff_df.merge(adv_df,on=['GAME_ID','SEASON_YEAR','TEAM_ID','GAME_DATE','MATCHUP', 'TEAM_ABBREVIATION', 'WL'])
        return df
    
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





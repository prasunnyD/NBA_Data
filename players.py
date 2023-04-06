from nba_api.stats.endpoints import playercareerstats, PlayerDashboardByGameSplits, LeagueDashPlayerStats, PlayerDashboardByYearOverYear
from nba_api.stats.static import players
import pandas as pd


class Player:
    def __init__(self, name : str, team : str) -> None:
        self.name = name
        self.team = team
        self.id = self.get_player_id()

    def get_player_id(self) -> int:
        player_id = (players.find_players_by_full_name(self.name)[0])['id']
        return player_id
    
    def get_career_stats(self, season : str = None) -> pd.DataFrame:
        """
        TODO: Might removed double check value of this function
        """
        if season == None:
            stats = playercareerstats.PlayerCareerStats(player_id=self.id,per_mode36="PerGame").get_data_frames()[0]
        else:
            full_stats = playercareerstats.PlayerCareerStats(player_id=self.id,per_mode36="PerGame").get_data_frames()[0]
            stats = full_stats.loc[full_stats['SEASON_ID']== season]
        return stats
    
    def get_current_season_stats(self, per_mode : str = "PerGame") -> pd.DataFrame:
        """
        Combines box score stats and advanced stats to return current season overall stats of player
        
            Parameters:
                    per_mode (str): string for if user wants (Totals,PerGame,MinutesPer,Per48,Per40,Per36,PerMinute,PerPossession,PerPlay,Per100Possesions,Per100Plays)

            Returns:
                    final (Datafram): pandas dataframe of player's current season stats
        """
        regular_stats_columns = ['NAME','GP','MIN','FGM','FGA','FG_PCT','FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']
        adv_stats_columns = ['NAME','W_PCT','OFF_RATING','DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_RATIO', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'POSS', 'PIE', 'PACE']
        adv_stats = PlayerDashboardByGameSplits(player_id=self.id,per_mode_detailed=per_mode,measure_type_detailed='Advanced').get_data_frames()[0]
        adv_stats['NAME'] = self.name
        adv_player_stats = adv_stats[adv_stats_columns]
        stats = PlayerDashboardByGameSplits(player_id=self.id,per_mode_detailed=per_mode).get_data_frames()[0]
        stats['NAME'] = self.name
        player_stats = stats[regular_stats_columns]
        final = mergeTables(player_stats, adv_player_stats)
        return final
    
    def get_career_stats(self, season : str = None, season_type : str = "Regular Season", season_segment : str = None) -> pd.DataFrame:
        """
        Combines box score stats and advanced stats to return career stats of player
        
            Parameters:
                    per_mode (str): string for if user wants (Totals,PerGame,MinutesPer,Per48,Per40,Per36,PerMinute,PerPossession,PerPlay,Per100Possesions,Per100Plays)

            Returns:
                    final (Datafram): pandas dataframe of player's current season stats


        TODO: Make so can get single season from this to
        """
        regular_stats_columns = ['GROUP_VALUE','NAME','GP','MIN','FGM','FGA','FG_PCT','FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']
        adv_stats_columns = ['GROUP_VALUE','NAME','W_PCT','OFF_RATING','DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_RATIO', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'POSS', 'PIE', 'PACE', '']
        stats = PlayerDashboardByYearOverYear(player_id=self.id,per_mode_detailed="PerGame", season=season, season_segment_nullable = season_segment, season_type_playoffs= season_type).get_data_frames()[1]
        stats['NAME'] = self.name
        player_career_stats = stats[regular_stats_columns]
        adv_stats = PlayerDashboardByYearOverYear(player_id=self.id,per_mode_detailed="PerGame",measure_type_detailed="Advanced", season=season, season_segment_nullable = season_segment, season_type_playoffs= season_type).get_data_frames()[1]
        adv_stats['NAME'] = self.name
        player_career_adv_stats = adv_stats[adv_stats_columns]
        final = mergeTables(player_career_stats,player_career_adv_stats)
        return final
    
        
def mergeTables(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes into one
    """
    result = pd.merge(df1, df2)
    return result
    

# a_edwards = Player('Anthony Edwards','Minnesota')
# print(a_edwards.get_career_stats(season='2022-23'))




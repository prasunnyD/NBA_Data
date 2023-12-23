from nba_api.stats.endpoints import playercareerstats, PlayerDashboardByGameSplits, LeagueDashPlayerStats, PlayerDashboardByYearOverYear, CumeStatsPlayerGames, WinProbabilityPBP, PlayerGameLogs, PlayerGameLog
from nba_api.stats.static import players
from nba_api.stats.library.parameters import SeasonAll
import pandas as pd
from util import mergeTables
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
    
    def get_career_stats2(self, season : str = None, season_type : str = "Regular Season", season_segment : str = None) -> pd.DataFrame:
        """
        Combines box score stats and advanced stats to return career stats of player
        
            Parameters:
                    per_mode (str): string for if user wants (Totals,PerGame,MinutesPer,Per48,Per40,Per36,PerMinute,PerPossession,PerPlay,Per100Possesions,Per100Plays)

            Returns:
                    final (Dataframe): pandas dataframe of player's current season stats


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
    
    def get_games(self):
        '''
        Gives out game ids. Could use game ids in boxscore to create dataframe of box score for season
        '''
        stats = CumeStatsPlayerGames(player_id=self.id,season_type_all_star='Regular Season').get_data_frames()[0]
        # stats['MATCHUP'] = [re.sub('Timberwolves', '', x) for x in stats['MATCHUP']]
        # stats['MATCHUP'] = [re.sub('at', '', x) for x in stats['MATCHUP']]
        # stats[['DATE', 'OPPONENT','OPPONENT SUFFIX']] = stats["MATCHUP"].apply(lambda x: pd.Series(str(x).split()))
        # #stats['OPPONENT'] = stats[['OPPONENT', 'OPPONENT SUFFIX']].apply('-'.join,axis=1)
        # stats['TUTTI'] = stats['OPPONENT'].astype(str)+stats['OPPONENT SUFFIX']
        # #print(stats.MATCHUP.str.split(expand=True))
        return stats

    #TODO PlayerDashboardByGameSplits might be useful to get player performance by halves and quarter

    def test_func(self):
        '''Play by play see what can be done with this, move to team.py'''
        pbp = WinProbabilityPBP('0022201225').get_data_frames()[0]
        print(pbp)

    def player_career_boxscore(self):
        gamelog_df = PlayerGameLog(player_id=self.id,season=SeasonAll.all).get_data_frames()[0]
        return gamelog_df
            
    def player_minutes(self):
        '''
        Returns df for minutes projection. Not sure if needed. Could be useful for injuries.
        '''
        boxscore_df = self.player_career_boxscore()
        min_df = pd.DataFrame()
        min_df['MIN'] = boxscore_df['MIN']
        min_df['prev_3_avg'] = boxscore_df['MIN'].rolling(3).mean()
        #min_df['prev_5_avg'] = boxscore_df['MIN'].rolling(5).mean()
        min_df['prev_3_median'] = boxscore_df['MIN'].rolling(3).median()
        #min_df['prev_5_median'] = boxscore_df['MIN'].rolling(5).median()
        min_df['prev_3_std'] = boxscore_df['MIN'].rolling(3).std()
        #min_df['prev_5_std'] = boxscore_df['MIN'].rolling(5).std()
        min_df.dropna(inplace=True)
        return min_df

    def player_stat(self, stat):
        '''
        TODO get opponents stats, home and away
        '''
        boxscore_df = self.player_career_boxscore()
        pts_df = pd.DataFrame()
        home_list = ["Away" if '@' in x else "Home" for x in boxscore_df['MATCHUP']]
        opp_team = []
        # TODO convert to list comprehension
        for x in boxscore_df['MATCHUP']:
            dummy = x.split()
            opponent = dummy[2]
            opp_team.append(opponent)
        pts_df['SEASON_ID'] = boxscore_df['SEASON_ID']
        pts_df['GAME_DATE'] = boxscore_df['GAME_DATE']
        pts_df[stat] = boxscore_df[stat]
        pts_df['LOCATION'] = home_list
        pts_df['OPPONENT'] = opp_team
        pts_df.dropna(inplace=True)
        return pts_df
        





from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamPlayerDashboard, PlayerDashboardByGameSplits, LeagueGameLog, LeagueDashTeamStats
from util import mergeTables
import pandas as pd
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
    
    def get_team_lineup_stats(self, lineup):
        lineup_stats = pd.DataFrame()
        for player in lineup:
            lineup_stats = pd.concat([lineup_stats, player.get_current_season_stats()])
        return lineup_stats

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

    def get_team_opp_efga(self):
        stats = LeagueDashTeamStats(team_id_nullable=self.id,measure_type_detailed_defense='Four Factors',season="2022-23", timeout=100).get_data_frames()[0]
        return stats
        # return stats["OPP_EFG_PCT"][0]
        
    #TODO TeamAndPlayersVsPlayers CAN BE USED FOR LINEUP COMPARISON


def abrv_team_dict(team : str):
    ATL = Team('Atlanta')
    BKN = Team('Brooklyn')
    BOS = Team('Boston')
    CHA = Team('Charlotte')
    CHI = Team('Chicago')
    CLE = Team('Cleveland')
    DAL = Team('Dallas')
    DEN = Team('Denver')
    DET = Team('Detroit')
    GSW = Team('Golden State')
    HOU = Team('Houston')
    IND = Team('Indiana')
    MEM = Team('Memphis')
    MIA = Team('Miami')
    MIL = Team('Milwaukee')
    MIN = Team('Minnesota')
    NOP = Team('New Orleans')
    NYK = Team('New York')
    LAC = Team('Los Angeles Clippers')
    LAL = Team('Los Angeles Lakers')
    OKC = Team('Oklahoma City')
    ORL = Team('Orlando')
    PHI = Team('Philadelphia')
    PHX = Team('Phoenix')
    POR = Team('Portland')
    SAC = Team('Sacramento')
    SAS = Team('San Antonio')
    TOR = Team('Toronto')
    UTA = Team('Utah')
    WAS = Team('Washington')

    abrv_team_dict = {'ATL': ATL, 'BKN': BKN, 'BOS': BOS, 'CHA': CHA, 'CHI': CHI, 'CLE': CLE, 'DAL': DAL, 'DEN': DEN, 'DET': DET, 'GSW': GSW,
                      'HOU': HOU, 'IND': IND, 'MEM': MEM, 'MIA': MIA, 'MIL': MIL, 'MIN': MIN, 'NOP': NOP, 'NYK': NYK, 'LAC': LAC, 'LAL': LAL, 
                      'OKC': OKC, 'ORL': ORL, 'PHI': PHI, 'PHX': PHX, 'POR': POR, 'SAC': SAC, 'SAS': SAS, 'TOR': TOR, 'UTA': UTA, 'WAS': WAS}
    
    return abrv_team_dict[team]


# timberwolves = Team('Minnesota')
# print(timberwolves.team_rebounding_stats())
abrv_list = ['ATL', 'BKN', 'BOS', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 'HOU', 'IND', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
              'LAC', 'LAL', 'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']

# for abrv in abrv_list:
#     print(teams.find_team_by_abbreviation(abrv))

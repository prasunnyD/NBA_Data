from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamPlayerDashboard, PlayerDashboardByGameSplits, TeamDashboardByYearOverYear, LeagueGameLog
from players import *

class Team:
    def __init__(self, city : str, lineup) -> None:
        self.city = city
        self.lineup = lineup
        self.id = self.get_team_id()

    def get_team_id(self) -> int:
        team_id = (teams.find_teams_by_city(self.city)[0])['id']
        return team_id

    def get_team_stats(self) -> pd.DataFrame:
        """
        Not sure if keeping this function
        """
        team_stats =TeamPlayerDashboard(team_id=self.id).get_data_frames()[0]
        return team_stats
    

    def get_team_stats2(self, season : str = None, season_type : str = "Regular Season", season_segment : str = None) -> pd.DataFrame:
        """
        Combines box score and advanced stats of team and returns dataframe of multiple seasons.
            Parameters:
                season (str)
        TODO: Make so can get single season from this to
        """
        regular_stats_columns = ['GROUP_VALUE','TEAM','GP','MIN','FGM','FGA','FG_PCT','FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']
        adv_stats_columns = ['GROUP_VALUE','TEAM','W_PCT','OFF_RATING','DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_RATIO', 'EFG_PCT', 'TS_PCT', 'POSS', 'PIE', 'PACE']
        stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
        stats['TEAM'] = self.city
        team_career_stats = stats[regular_stats_columns]
        adv_stats = TeamDashboardByYearOverYear(team_id=self.id,per_mode_detailed="PerGame",measure_type_detailed_defense="Advanced", season=season, season_segment_nullable = season_segment, season_type_all_star= season_type).get_data_frames()[1]
        adv_stats['TEAM'] = self.city
        team_career_adv_stats = adv_stats[adv_stats_columns]
        final = mergeTables(team_career_stats,team_career_adv_stats)
        return final
    
    def get_team_lineup_stats(self):
        lineup_stats = pd.DataFrame()
        for player in self.lineup:
            lineup_stats = pd.concat([lineup_stats, player.get_current_season_stats()])
        return lineup_stats

    def team_scores(self):
        boxscore = LeagueGameLog().get_data_frames()[0]
        return boxscore
    #TODO TeamAndPlayersVsPlayers CAN BE USED FOR LINEUP COMPARISON

a_edwards = Player('Anthony Edwards','Minnesota')
r_gobert = Player('Rudy Gobert', 'Minnesota')
j_mcdaniels = Player('Jaden McDaniels', 'Minnesota')
k_towns = Player('Karl-Anthony Towns', 'Minnesota')
m_conley = Player('Mike Conley', 'Minnesota')

minn_lineup = [a_edwards, r_gobert, j_mcdaniels, k_towns, m_conley]
timberwolves = Team('Minnesota', minn_lineup)
print(timberwolves.team_scores())
from nba_api.stats.endpoints import playercareerstats, PlayerDashboardByGameSplits, LeagueDashPlayerStats, PlayerDashboardByYearOverYear, CumeStatsPlayerGames, WinProbabilityPBP, PlayerGameLogs, PlayerGameLog, PlayerDashPtShots
from nba_api.stats.static import players
from nba_api.stats.library.parameters import SeasonAll, SeasonNullable
import pandas as pd
import polars as pl
import logging
from util import mergeTables
class Player:
    def __init__(self, name : str) -> None:
        self.name = name
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
        adv_stats_columns = ['GROUP_VALUE','NAME','W_PCT','OFF_RATING','DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_RATIO', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'POSS', 'PIE', 'PACE']
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

    def player_career_boxscore(self,measure_type='Base') -> pd.DataFrame:
        """
        Returns player boxscore per game for entire career
        """
        # gamelog_df = PlayerGameLog(player_id=self.id,season=SeasonAll.all,measure_type='Advanced').get_data_frames()[0]
        gamelog_df = PlayerGameLogs(player_id_nullable=self.id,season_nullable=SeasonNullable.current_season,measure_type_player_game_logs_nullable='Advanced').get_data_frames()[0]
        return gamelog_df
    
    def player_boxscores(self, season) -> pd.DataFrame:
        adv_stats_df = PlayerGameLogs(player_id_nullable=self.id,measure_type_player_game_logs_nullable='Advanced', season_nullable=season).get_data_frames()[0]
        adv_stats_df = adv_stats_df.drop(columns=['NICKNAME','TEAM_NAME','TEAM_ID','TEAM_ABBREVIATION','GAME_DATE','MATCHUP','WL','MIN','FGM','FGA','FG_PCT','AVAILABLE_FLAG'])
        adv_stats_df.drop(list(adv_stats_df.filter(regex='RANK')), axis=1, inplace=True)
        stats_df = PlayerGameLogs(player_id_nullable=self.id,season_nullable=season).get_data_frames()[0]
        stats_df.drop(list(stats_df.filter(regex='RANK')), axis=1, inplace=True)
        stats_df = stats_df.drop(columns=['NICKNAME','WL','AVAILABLE_FLAG','NBA_FANTASY_PTS','DD2','TD3','WNBA_FANTASY_PTS'])
        gamelog_df = stats_df.merge(adv_stats_df,on=['PLAYER_ID','SEASON_YEAR','PLAYER_NAME','GAME_ID'])
        return gamelog_df
    
    
    def player_minutes_polars(self) -> pl.DataFrame:
        boxscores = PlayerGameLog(player_id=self.id,season=SeasonAll.all).get_dict()
        boxscores_df = pl.DataFrame(boxscores['resultSets'][0]['rowSet'], schema=boxscores['resultSets'][0]['headers'])
        return (pl.DataFrame(boxscores_df)
                .select(
                    pl.col('MIN'),
                    pl.col('MIN').rolling_mean(window_size=3).alias('prev_3_avg'),
                    pl.col('MIN').rolling_median(window_size=3).alias('prev_3_median'),
                    pl.col('MIN').rolling_std(window_size=3).alias('prev_3_std')
                )
        )

    def player_stat(self) -> pl.DataFrame:
        """
        Creates a polars DataFrame for training purposes with game stats and location info.
        
        Parameters:
            stat (str): The statistical column to extract (e.g. 'PTS', 'AST', 'REB')
            
        Returns:
            pl.DataFrame: DataFrame containing:
                - SEASON_ID: Season identifier
                - GAME_DATE: Date of game
                - stat: The requested statistical column
                - MIN: Minutes played
                - LOCATION: 'Home' or 'Away' 
                - OPPONENT: Opponent team abbreviation
                
        The function processes the player's game logs to extract the specified stat
        along with contextual information about each game including location and opponent.
        Null values are preserved in the returned DataFrame.
        """
        logging.info(f"Getting boxscores for {self.name}...")
        try:
            boxscores = PlayerGameLog(player_id=self.id, timeout=10).get_dict()
            boxscores_df = pl.DataFrame(boxscores['resultSets'][0]['rowSet'], schema=boxscores['resultSets'][0]['headers'], orient='row')
            home_list = ["Away" if '@' in x else "Home" for x in boxscores_df['MATCHUP']]
            opp_team = [x.split()[2] for x in boxscores_df['MATCHUP']]
            boxscores_df = boxscores_df.with_columns([
                pl.Series(name="LOCATION", values=home_list),
                pl.Series(name="OPPONENT", values=opp_team)
            ])
            logging.info(f"Returning boxscores for {self.name}...")
            return boxscores_df
        except Exception as e:
            logging.error(f"Error getting boxscores for {self.name}: {e}")
            raise
    
    # def player_stat_duckdb(self, stat: str) -> pd.DataFrame:
    #     logging.info(f"Getting boxscores for {self.name}...")
    #     boxscores = PlayerGameLog(player_id=self.id,season=SeasonAll.all).get_dict()
    #     boxscores_df = pl.DataFrame(boxscores['resultSets'][0]['rowSet'], schema=boxscores['resultSets'][0]['headers'])
    #     # Convert to DuckDB and process
    #     query = f"""
    #     SELECT 
    #         SEASON_ID,
    #         GAME_DATE,
    #         {stat},
    #         MIN,
    #         CASE 
    #             WHEN MATCHUP LIKE '%@%' THEN 'Away'
    #             ELSE 'Home'
    #         END as LOCATION,
    #         split_part(MATCHUP, ' ', 3) as OPPONENT
    #     FROM boxscores_df
    #     """
        
        # result_df = duckdb.query(query).pl()
        
        # logging.info(f"Returning processed boxscores for {self.name}...")
        # return result_df.drop_nulls()

    def player_minutes_polars(self) -> pl.DataFrame:
        '''
        Returns df for minutes projection. Not sure if needed. Could be useful for injuries.
        '''
        boxscores = PlayerGameLog(player_id=self.id,season=SeasonAll.all).get_dict()
        boxscores_df = pl.DataFrame(boxscores['resultSets'][0]['rowSet'], schema=boxscores['resultSets'][0]['headers'])
        return (pl.DataFrame(boxscores_df)
                .select(
                    pl.col('MIN'),
                    pl.col('MIN').rolling_mean(window_size=3).alias('prev_3_avg'),
                    pl.col('MIN').rolling_median(window_size=3).alias('prev_3_median'),
                    pl.col('MIN').rolling_std(window_size=3).alias('prev_3_std')
                )
                .drop_nulls()
        )
    
    def create_player_boxscore_table(self, conn):
        """
        Creates/updates a DuckDB table with player boxscore data.
        """

        boxscores_df = self.player_stat()


        # Check if table exists
        table_exists = conn.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'player_boxscores'
            )
        """).fetchone()[0]

        if not table_exists:
            logging.info("Creating new player_boxscores table...")
            conn.execute("""
                CREATE TABLE player_boxscores AS 
                SELECT * FROM boxscores_df
            """)
        else:
            logging.info("Updating existing player_boxscores table...")
            # Register the new data as a view
            conn.register('new_boxscores', boxscores_df)
            
            # Insert only new records
            conn.execute("""
                INSERT INTO player_boxscores
                SELECT n.* 
                FROM new_boxscores n
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM player_boxscores p
                    WHERE p.GAME_ID = n.GAME_ID 
                    AND p.Player_ID = n.Player_ID
                )
            """)

        conn.commit()

        logging.info(f"Successfully updated boxscores for {self.name} in player_boxscores table")


    def create_player_shooting_splits_table(self, conn, team_id : str, table_name : str):
        """
        Creates/updates a DuckDB table with player shooting splits data.
        try:
        """ 
        try:
            shots = PlayerDashPtShots(player_id=self.id, team_id=team_id, per_mode_simple="PerGame", timeout=10).get_dict()
            shots_df = pl.DataFrame(shots['resultSets'][0]['rowSet'], schema=shots['resultSets'][0]['headers'], orient='row')
            # Check if table exists
            table_exists = conn.execute(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name.lower()}'
                )
            """).fetchone()[0]

            if not table_exists:
                logging.info(f"Creating {table_name}...")
                conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM shots_df
                """)
            else:
                logging.info(f"Updating {table_name}...")
                conn.register('new_shots', shots_df)
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE PLAYER_ID = '{self.id}';
                    
                    INSERT INTO {table_name}
                    SELECT * FROM new_shots;
                """)
            conn.commit()
            logging.info(f"Successfully populated {self.name} shooting splits...")
        except Exception as e:
            logging.error(f"Error getting shooting splits for {self.name}: {e}")
            raise




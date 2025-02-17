import pandas as pd
import numpy as np
from players import Player

class NBAPointsProjector:
    def __init__(self, league_averages, player, team, opponent):
        # Team pace factors (already in possessions per game)
        self.team_pace = team.pace
        self.league_average_pace = league_averages.pace
        # League average shooting percentages (baseline)
        self.league_averages = {
            'two_point_percentage': 0.55,
            'three_point_percentage': 0.357,
            'fta_rate': 0.246,
            'pace': league_averages.pace
        }
        
        # Player-specific data for Anthony Edwards
        self.edwards_stats = {
            'minutes_per_game': 36.5,
            'usage_rate': 31.2,
            'points_per_game': 27.5,
            'field_goal_attempts_per_game': 20.8,
            'two_point_frequency': 0.512,
            'three_point_frequency': 0.488,
            'two_point_percentage': 0.463,
            'three_point_percentage': 0.414,
            'free_throw_attempts_per_game': 5.9,
            'free_throw_percentage': 0.843
        }
    
    def calculate_game_pace(self, team, opponent):
        """
        Calculate expected game pace based on both teams.
        Adjusts relative to league average pace.
        """
        # Calculate each team's pace relative to league average
        team_pace_factor = team / self.league_average_pace
        opponent_pace_factor = opponent / self.league_average_pace
        
        # Average the pace factors and apply to league average
        average_pace_factor = (team_pace_factor + opponent_pace_factor) / 2
        return self.league_average_pace * average_pace_factor
    
    def adjust_shooting_percentage(self, base_percentage, league_average, defense_percentage):
        """
        Adjust shooting percentage based on defensive matchup.
        Uses the differential between defensive FG% allowed and league average.
        """
        defensive_impact = (defense_percentage - league_average) / league_average
        return base_percentage * (1 + defensive_impact)
    
    def project_shot_attempts(self, projected_possessions, opp_fta_rate):
        """Project shot attempts based on usage and team possessions."""
        usage_factor = self.edwards_stats['usage_rate'] / 100
        
        # Calculate expected possessions used by player
        player_possessions = projected_possessions * usage_factor
        print(player_possessions)
        
        # Calculate historical FGA per possession
        baseline_possessions = self.league_average_pace * (self.edwards_stats['minutes_per_game'] / 48)  # Using team's normal pace for baseline
        fga_per_possession = self.edwards_stats['field_goal_attempts_per_game'] / baseline_possessions
        print(fga_per_possession)
        # Project total FGA
        projected_fga = player_possessions * fga_per_possession
        print(projected_fga)
        # Use shot frequencies to split into 2PA and 3PA
        return {
            'fga': player_possessions,
            '2pa': player_possessions * self.edwards_stats['two_point_frequency'],
            '3pa': player_possessions * self.edwards_stats['three_point_frequency'],
            'fta': player_possessions * opp_fta_rate # TODO: weight 2pt foul less than 3pt foul but 3pt fouls happen less often
        }
    
    def project_points(self, oppenent_pace, oppenent_defense_2pt, oppenent_defense_3pt, opp_fta_rate, player_stats=None):
        """Project points for a player against specific opponent."""
        if player_stats is None:
            player_stats = self.edwards_stats
        
        # Get projected possessions for the game
        game_pace = self.calculate_game_pace(self.team_pace, oppenent_pace)
        minutes_factor = player_stats['minutes_per_game'] / 48
        projected_possessions = game_pace * minutes_factor
        
        # Project shot attempts using usage rate
        shots = self.project_shot_attempts(projected_possessions, opp_fta_rate)
        
        # Adjust shooting percentages based on opponent defense
        adjusted_2pt_percentage = self.adjust_shooting_percentage(
            player_stats['two_point_percentage'],
            self.league_averages['two_point_percentage'],
            oppenent_defense_2pt
        )
        
        adjusted_3pt_percentage = self.adjust_shooting_percentage(
            player_stats['three_point_percentage'],
            self.league_averages['three_point_percentage'],
            oppenent_defense_3pt
        )
        
        # Calculate expected points using adjusted percentages
        expected_two_points = shots['2pa'] * adjusted_2pt_percentage * 2
        expected_three_points = shots['3pa'] * adjusted_3pt_percentage * 3
        expected_free_points = shots['fta'] * player_stats['free_throw_percentage']
        
        total_points = expected_two_points + expected_three_points + expected_free_points
        
        return {
            'projected_points': round(total_points, 1),
            'game_pace': round(game_pace, 1),
            'projected_possessions': round(projected_possessions, 1),
            'breakdown': {
                'two_points': round(expected_two_points, 1),
                'three_points': round(expected_three_points, 1),
                'free_throws': round(expected_free_points, 1),
                'shot_attempts': {
                    'two_pointers': round(shots['2pa'], 1),
                    'three_pointers': round(shots['3pa'], 1),
                    'free_throws': round(shots['fta'], 1)
                },
                'adjusted_percentages': {
                    'two_point': round(adjusted_2pt_percentage * 100, 1),
                    'three_point': round(adjusted_3pt_percentage * 100, 1),
                    'defensive_impact_2pt': round((adjusted_2pt_percentage - player_stats['two_point_percentage']) * 100, 1),
                    'defensive_impact_3pt': round((adjusted_3pt_percentage - player_stats['three_point_percentage']) * 100, 1)
                }
            }
        }
    
    # def project_ppg_second_method(self, opponent_pace, oppenent_defense_2pt, oppenent_defense_3pt,opp_fta_rate):
    #     """Project points for a player against specific opponent."""

    #     player_stats = self.edwards_stats

    #     # Calculate defensive adjustment factors
    #     two_point_adjustment = (oppenent_defense_2pt / self.league_averages['two_point_percentage'])
    #     three_point_adjustment = (oppenent_defense_3pt / self.league_averages['three_point_percentage'])
    #     fta_adjustment = (opp_fta_rate / self.league_averages['fta_rate'])
        
    #     # Calculate pace adjustment
    #     pace_adjustment = self.calculate_game_pace(self.team_pace, opponent_pace) / self.team_pace
        
    #     # Start with baseline points
    #     baseline_points = player_stats['points_per_game']
        
    #     # Estimate points breakdown from baseline (based on typical distribution)
    #     baseline_two_points = baseline_points * self.edwards_stats['two_point_frequency']  # Estimated 50% of points from 2s
    #     baseline_three_points = baseline_points * self.edwards_stats['three_point_frequency'] # Estimated 30% of points from 3s
        
    #     # Apply adjustments
    #     adjusted_two_points = baseline_two_points * two_point_adjustment * pace_adjustment
    #     adjusted_three_points = baseline_three_points * three_point_adjustment * pace_adjustment
    #     adjusted_free_throw_points = baseline_points * fta_adjustment * pace_adjustment
        
    #     total_points = adjusted_two_points + adjusted_three_points + adjusted_free_throw_points
        
    #     return {
    #         'baseline_points': baseline_points,
    #         'projected_points': round(total_points, 1),
    #         'game_pace': round(self.calculate_game_pace(self.team_pace, opponent_pace), 1),
    #         'adjustment_factors': {
    #             'two_point_defense': round(two_point_adjustment * 100 - 100, 1),
    #             'three_point_defense': round(three_point_adjustment * 100 - 100, 1),
    #             'fta_rate': round(fta_adjustment * 100 - 100, 1),
    #             'pace': round(pace_adjustment * 100 - 100, 1)
    #         },
    #         'breakdown': {
    #             'two_points': round(adjusted_two_points, 1),
    #             'three_points': round(adjusted_three_points, 1),
    #             'free_throws': round(adjusted_free_throw_points, 1),
    #         }
    #     }

# Example usage
player=Player()
projector = NBAPointsProjector(team_pace=98.4)
edwards_projection = projector.project_points(oppenent_pace=101.35, oppenent_defense_2pt=0.55, oppenent_defense_3pt=0.358, opp_fta_rate=0.246)
print(edwards_projection)
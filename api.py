from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from main import *
from pydantic import BaseModel
from util import Database
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import CommonTeamRoster
from nba_api.stats.static import teams
import os
from datetime import datetime

MOTHERDUCK_TOKEN = os.environ.get('motherduck_token')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite's default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PlayerModel(BaseModel):
    city:str
    opp_city : str
    minutes:float

@app.post("/points-prediction/{player_name}")
def points_prediction(player_name: str,item: PlayerModel):
    """
    Takes player name, opponent city, and minutes as inputs.
    Returns predicted points
    """
    prediction = predict_result(f'{player_name}_points_model.sav', item.opp_city, item.minutes)
    print(prediction)
    return {"projected_points": prediction[0][0]}

class PoissonDist(BaseModel):
    predictedPoints : float
    bookLine : float


@app.post("/poisson_dist")
def get_poisson_dist(poissondist : PoissonDist):
    """
    Convert possion percentages into odds
    """
    data = poissondist.model_dump()
    proj_points = round(data['predictedPoints'])
    less_than, greater_than = poisson_dist(data['bookLine'], proj_points)
    return {"less": less_than, "greater":greater_than }

@app.get("/opponent-team-stats/{city}/{number_of_days}")
def opponent_team_stats(city: str, number_of_days : str):
    """
    TODO Goal is to display this data in UI
    """
    team = Team(city)
    adv_stats_df = team.get_team_adv_stats('22023',last_number_games=number_of_days)
    opp_stats_df= team.get_team_opp_efga('22023')

@app.get("/team-last-10-games/{city}")
def get_team_last_ten_games(city : str) -> dict[str, float]:
    try:
        team = Team(city)
        query = f"SELECT GAME_DATE,PTS FROM team_boxscores WHERE TEAM_ID = '{team.id}' LIMIT 10"
        conn = duckdb.connect("team_boxscores.db")
        team_game_logs = conn.sql(query).pl()
        response = player_game_logs.rows_by_key(['GAME_DATE'])
        if not response:
            raise HTTPException(status_code=404, detail=f"No games found for team: {city}")
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid team city: {city}")
    return response


@app.get("/team-roster/{city}")
def get_team_roster(city : str):
    print("city selected: ", city)
    if "Los Angeles" not in city:
        city = city.split(' ')
        if len(city) == 2:
            city = city[0]
        else:
            city = city[0:2]
            city = ' '.join(city) 
    else:
        print(city)

    try:
        team = Team(city)
        response = team.get_team_roster()
        if not response:
            raise HTTPException(status_code=404, detail=f"No team members found for team: {city}")
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid team city: {city}")
    print("response: ", response)
    return response

    

class GameStats(BaseModel):
    points: float
    assists: float
    rebounds: float
    minutes: float

class PlayerGamesResponse(BaseModel):
    games: dict[str, GameStats]

@app.get("/player-last-{last_number_of_games}-games/{name}")
def get_player_last_x_games(name: str, last_number_of_games : int) -> dict[str, dict[str, float]]:
    try:
        player = Player(name)
        query = f"SELECT GAME_DATE,PTS,AST,REB,MIN FROM player_boxscores WHERE Player_ID = '{player.id}' Order by game_id DESC LIMIT {last_number_of_games}"
        with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
            player_game_logs = conn.sql(query).pl()
            response = {}
            for row in player_game_logs.iter_rows(named=True):
                game_date = row['GAME_DATE']
                response[game_date] = {
                    'points': float(row['PTS']),
                    'assists': float(row['AST']),
                    'rebounds': float(row['REB']),
                    'minutes': float(row['MIN'])
                }
        if not response:
            raise HTTPException(
                status_code=404,
                detail=f"No games found for {player.name}"
            )
            
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid Player: {name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    return response


@app.get("/scoreboard") # Cache until midnight
def get_scoreboard():
    games = scoreboard.ScoreBoard().games.get_dict()
    response = {}
    for game in games:
        response[game['gameId']] = {
            'home_team': game['homeTeam']['teamCity'] + ' ' + game['homeTeam']['teamName'],
            'away_team': game['awayTeam']['teamCity'] + ' ' + game['awayTeam']['teamName']
        }
    return response

@app.get("/{team_name}-defense-stats")
def get_team_defense_stats(team_name : str):
    
    team_id = (teams.find_teams_by_city(team_name)[0])['id']
    response = {}
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        opponent_query = f"SELECT * FROM teams_opponent_stats WHERE TEAM_ID = '{team_id}'"
        defense_query = f"SELECT * FROM teams_defense_stats WHERE TEAM_ID = '{team_id}'"
        four_factors_query = f"SELECT * FROM teams_four_factors_stats WHERE TEAM_ID = '{team_id}'"
        advanced_query = f"SELECT * FROM teams_advanced_stats WHERE TEAM_ID = '{team_id}'"
        opponent_stats = conn.sql(opponent_query).pl()
        defense_stats = conn.sql(defense_query).pl()
        four_factors_stats = conn.sql(four_factors_query).pl()
        advanced_stats = conn.sql(advanced_query).pl()
        response[team_name] = {
            "OPP_FGA_RANK": opponent_stats['OPP_FGA_RANK'][0],
            "OPP_FGA": opponent_stats['OPP_FGA'][0],
            "OPP_FG_PCT_RANK": opponent_stats['OPP_FG_PCT_RANK'][0],
            "OPP_FG_PCT": opponent_stats['OPP_FG_PCT'][0],
            "OPP_FTA_RANK": opponent_stats['OPP_FTA_RANK'][0],
            "OPP_FTA": opponent_stats['OPP_FTA'][0],
            "OPP_FT_PCT_RANK": opponent_stats['OPP_FT_PCT_RANK'][0],
            "OPP_FT_PCT": opponent_stats['OPP_FT_PCT'][0],
            "OPP_REB_RANK": opponent_stats['OPP_REB_RANK'][0],
            "OPP_REB": opponent_stats['OPP_REB'][0],
            "OPP_AST_RANK": opponent_stats['OPP_AST_RANK'][0],
            "OPP_AST": opponent_stats['OPP_AST'][0],
            "OPP_FG3A_RANK": opponent_stats['OPP_FG3A_RANK'][0],
            "OPP_FG3A": opponent_stats['OPP_FG3A'][0],
            "DEF_RATING_RANK": defense_stats['DEF_RATING_RANK'][0],
            "DEF_RATING": defense_stats['DEF_RATING'][0],
            "OPP_PTS_PAINT_RANK": defense_stats['OPP_PTS_PAINT_RANK'][0],
            "OPP_PTS_PAINT": defense_stats['OPP_PTS_PAINT'][0],
            "PACE_RANK": advanced_stats['PACE_RANK'][0],
            "PACE": advanced_stats['PACE'][0],
            "OPP_EFG_PCT_RANK": four_factors_stats['OPP_EFG_PCT_RANK'][0],
            "OPP_EFG_PCT": four_factors_stats['OPP_EFG_PCT'][0],
            "OPP_FTA_RATE_RANK": four_factors_stats['OPP_FTA_RATE_RANK'][0],
            "OPP_FTA_RATE": four_factors_stats['OPP_FTA_RATE'][0],
            "OPP_OREB_PCT_RANK": four_factors_stats['OPP_OREB_PCT_RANK'][0],
            "OPP_OREB_PCT": four_factors_stats['OPP_OREB_PCT'][0]
        }
    return response
                                
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from main import *
from pydantic import BaseModel
from nba_api.stats.static import teams, players
import os
from curl_cffi import requests
import duckdb
from contextlib import asynccontextmanager
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache

MOTHERDUCK_TOKEN = os.environ.get('motherduck_token')

@asynccontextmanager
async def lifespan(app: FastAPI):
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    yield

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite's default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
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
    try:
        team = Team(city)
        roster_df = team.get_team_roster()
        response = {
            team.city: roster_df.select(["PLAYER", "NUM", "POSITION"]).to_dicts()
        }
        if not response:
            raise HTTPException(status_code=404, detail=f"No team members found for team: {city}")
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid team city: {city}")
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
        query = f"SELECT strptime(GAME_DATE, '%b %d, %Y')::DATE AS GAME_DATE,PTS,AST,REB,FG3M,MIN FROM player_boxscores WHERE Player_ID = '{player.id}' Order by GAME_DATE desc limit {last_number_of_games}"
        with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
            player_game_logs = conn.sql(query).pl()
            response = {}
            for row in player_game_logs.iter_rows(named=True):
                game_date = row['GAME_DATE'].strftime('%Y-%m-%d')
                response[game_date] = {
                    'points': float(row['PTS']),
                    'assists': float(row['AST']),
                    'rebounds': float(row['REB']),
                    'threePointersMade': float(row['FG3M']),
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


@app.get("/scoreboard")
@cache(expire=3600)
def get_scoreboard():
    session = requests.Session(impersonate="chrome")
    url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
    response = session.get(url)
    games = response.json()['scoreboard']['games']
    response = {}
    for game in games:
        response[game['gameId']] = {
            'home_team': game['homeTeam']['teamCity'] + ' ' + game['homeTeam']['teamName'],
            'away_team': game['awayTeam']['teamCity'] + ' ' + game['awayTeam']['teamName']
        }
    return response

@app.get("/{team_name}-defense-stats")
def get_team_defense_stats(team_name : str):
    team_id = (teams.find_teams_by_full_name(team_name)[0])['id']
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
            "OPP_FG3_PCT": opponent_stats['OPP_FG3_PCT'][0],
            "OPP_FG3_PCT_RANK": opponent_stats['OPP_FG3_PCT_RANK'][0],
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

@app.get("/{player_name}-shooting-splits")
def get_player_shooting_splits(player_name : str):
    player_id = (players.find_players_by_full_name(player_name)[0])['id']
    response = {}
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        query = f"SELECT * FROM player_shooting_splits WHERE PLAYER_ID = '{player_id}'"
        shooting_splits = conn.sql(query).pl()
        response[player_name] = {
            "FG2A":shooting_splits['FG2A'][0],
            "FG2M":shooting_splits['FG2M'][0], 
            "FG2_PCT":shooting_splits['FG2_PCT'][0],
            "FG3A":shooting_splits['FG3A'][0],
            "FG3M":shooting_splits['FG3M'][0],
            "FG3_PCT":shooting_splits['FG3_PCT'][0],
            "FGA":shooting_splits['FGA'][0],
            "FGM":shooting_splits['FGM'][0],
            "FG_PCT":shooting_splits['FG_PCT'][0],
            "EFG_PCT":shooting_splits['EFG_PCT'][0],
            "FG2A_FREQUENCY":shooting_splits['FG2A_FREQUENCY'][0],
            "FG3A_FREQUENCY":shooting_splits['FG3A_FREQUENCY'][0]
        }
        return response
    
@app.get("/{player_name}-headline-stats")
def get_player_headline_stats(player_name : str):
    player_id = (players.find_players_by_full_name(player_name)[0])['id']
    response = {}
    with duckdb.connect(f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}") as conn:
        query = f"SELECT * FROM player_headline_stats WHERE PLAYER_ID = '{player_id}'"
        shooting_splits = conn.sql(query).pl()
        response[player_name] = {
            "PTS":shooting_splits['PTS'][0],
            "AST":shooting_splits['AST'][0], 
            "REB":shooting_splits['REB'][0]
        }
        return response

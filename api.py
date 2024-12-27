from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from main import *
from pydantic import BaseModel
from util import Database

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
        conn = duckdb.connect("player_boxscores.db")
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



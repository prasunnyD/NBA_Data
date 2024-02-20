from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from main import *
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

class Item(BaseModel):
    city:str
    minutes:float

@app.post("/points-prediction/{player_name}")
def points_prediction(player_name: str,item: Item):
    """
    Takes player name, opponent city, and minutes as inputs.
    Returns predicted points
    """
    prediction = predict_result(f'{player_name}_points_model.sav', item.city, item.minutes)
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

@app.get("/opponent-team-stats/{city}")
def opponent_team_stats(city: str):
    """
    TODO Goal is to display this data in UI
    """
    team = Team(city)
    adv_stats_df = team.get_team_adv_stats('22023')
    opp_stats_df= team.get_team_opp_efga('22023')

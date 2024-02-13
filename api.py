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
    OEFG:float
    OFTR:float
    OREB:float
    PACE:float
    minutes:float

@app.post("/points-prediction/{player_name}")
def points_prediction(player_name: str,item: Item):
    predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MINUTES']
    input = pd.DataFrame([item.model_dump().values()], columns=predictors)
    prediction = predict_result(f'{player_name}_points_model.sav',input)
    print(prediction)
    return {"projected_points": prediction[0][0]}

class PoissonDist(BaseModel):
    predictedPoints : float
    bookLine : float


@app.post("/poisson_dist")
def get_poisson_dist(poissondist : PoissonDist):
    proj_points = round(poissondist.model_dump().values())
    less_than, greater_than = poisson_dist(28.5, proj_points)
    return {"less": less_than, "greater":greater_than }

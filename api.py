from fastapi import FastAPI
from main import *
from pydantic import BaseModel
import json

app = FastAPI()

# class Item(BaseModel):
#     OEFG:float
#     OFTR:float
#     OREB:float
#     PACE:float
#     minutes:float

@app.post("/points-prediction")
def points_prediction(item):
    print(item)
    input = json.loads(item)
    print(input)
    predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MINUTES']
    input = pd.DataFrame([input.values()], columns=predictors)
    prediction = predict_result('edwards_points_model.sav',input)
    return {"projected_points": prediction[0][0]}

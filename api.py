from fastapi import FastAPI
from main import *
from pydantic import BaseModel

app = FastAPI()

# class Item(BaseModel):
#     OEFG:str
#     OFTR:str
#     OREB:str
#     PACE:str
#     minutes:str

@app.post("/points-prediction")
def points_prediction(item):
    print(item)
    # prediction = predict_result('edwards_points_model.sav')
    # return {"projected_points": prediction}

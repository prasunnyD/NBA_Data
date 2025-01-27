from pydantic import BaseModel

class PlayerModel(BaseModel):
    city: str
    opp_city: str
    minutes: float

class RegisterItem(BaseModel):
    full_name: str
    username: str
    password: str

class LoginItem(BaseModel):
    username: str
    password: str

class PoissonDist(BaseModel):
    predictedPoints: float
    bookLine: float

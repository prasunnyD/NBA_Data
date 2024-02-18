import pandas as pd
import requests
import json

# response = requests.get("https://api.betstamp.app/api/gameline/?game_id=166089&period=FT")
# print(json.dumps(response.json()))

action = requests.get("https://api.actionnetwork.com/web/v2/scoreboard/nhl?bookIds=15,30,1548,1005,1006,939,972,974,68,973,1902&date=20240218&periods=event")
print(action.text)
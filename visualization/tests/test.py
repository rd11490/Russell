import requests
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.pbpstats.com/possession-finder/nba?0Exactly5OnFloor=201143,202681,1627759,202330,1628369,202694,203935,1626179,203382,1628464&TeamId=1610612738&Season=2018-19&SeasonType=Regular%2BSeason&OffDef=Offense&StartType=All&Opponent=1610612754&OpponentExactly5OnFloor=201152,201936,202711,1627734,1626167,201954,203506,202709,203124,203926',
    'Origin': 'https://www.pbpstats.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    'Sec-Fetch-Mode': 'cors',
}

params = (
    ('0Exactly5OnFloor', '201143,202681,1627759,202330,1628369,202694,203935,1626179,203382,1628464'),
    ('TeamId', '1610612738'),
    ('Season', '2018-19'),
    ('SeasonType', 'Regular+Season'),
    ('OffDef', 'Offense'),
    ('StartType', 'All'),
    ('Opponent', '1610612754'),
    ('OpponentExactly5OnFloor', '201152,201936,202711,1627734,1626167,201954,203506,202709,203124,203926'),
)

response = requests.get('https://api.pbpstats.com/get-possessions/nba', headers=headers, params=params)

js = response.json()
print(js.keys())
print(js['player_results'])
print(js['possessions'])
print(js['team_results'])

df = pd.DataFrame(js['player_results'])
df.to_csv('player_results.csv')
print(df.head(10))


df = pd.DataFrame(js['possessions'])
df.to_csv('possessions.csv')
print(df.head(10))


df = pd.DataFrame([js['team_results']])
df.to_csv('team_results.csv')
print(df.head(10))
# #
#

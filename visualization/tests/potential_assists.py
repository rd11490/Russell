import requests
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/players/passing/?Season=2019-20&SeasonType=Regular%20Season&PerMode=Totals',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

params = (
    ('College', ''),
    ('Conference', ''),
    ('Country', ''),
    ('DateFrom', ''),
    ('DateTo', ''),
    ('Division', ''),
    ('DraftPick', ''),
    ('DraftYear', ''),
    ('GameScope', ''),
    ('Height', ''),
    ('LastNGames', '0'),
    ('LeagueID', '00'),
    ('Location', ''),
    ('Month', '0'),
    ('OpponentTeamID', '0'),
    ('Outcome', ''),
    ('PORound', '0'),
    ('PerMode', 'Totals'),
    ('PlayerExperience', ''),
    ('PlayerOrTeam', 'Player'),
    ('PlayerPosition', ''),
    ('PtMeasureType', 'Passing'),
    ('Season', '2019-20'),
    ('SeasonSegment', ''),
    ('SeasonType', 'Regular Season'),
    ('StarterBench', ''),
    ('TeamID', '0'),
    ('VsConference', ''),
    ('VsDivision', ''),
    ('Weight', ''),
)

response = requests.get('https://stats.nba.com/stats/leaguedashptstats', headers=headers, params=params)
results = response.json()['resultSets'][0]
frame = pd.DataFrame(results['rowSet'], columns=results['headers'])
print(frame.head(10))

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# response = requests.get('https://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=Totals&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=Passing&Season=2019-20&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=', headers=headers, cookies=cookies)

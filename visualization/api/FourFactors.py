import json

import pandas as pd
import urllib3
import MySqlDatabases.NBADatabase

from cred import MySQLConnector


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def build_url(season):
    return "https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Four+Factors&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=".format(season)

header_data = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Referer': 'stats.nba.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}

sql = MySQLConnector.MySQLConnector()
http = urllib3.PoolManager()

seasons = ["2018-19", "2017-18", "2016-17"]
teams = [i for i in range(1610612737, 1610612766)]

def extract_data(url, season, result):
    print(url)
    r = http.request('GET', url, headers=header_data)
    resp = json.loads(r.data)
    results = resp['resultSets'][result]
    headers = results['headers']
    headers.append("season")
    rows = results['rowSet']
    frame = pd.DataFrame(rows)
    frame["season"] = season
    frame.columns = headers
    return frame

for season in seasons:
    print("season: {}".format(season))
    url = build_url(season)
    data = extract_data(url, season, 0)
    data["primaryKey"] = data['TEAM_ID'].map(str) + '_' + data['season']
    sql.write(data, MySqlDatabases.NBADatabase.team_four_factors, MySqlDatabases.NBADatabase.NAME)













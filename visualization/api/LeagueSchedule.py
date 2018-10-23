import json

import pandas as pd
import urllib3

import MySQLConnector
import MySqlDatabases.NBADatabase

season = "2018"
def url(season):
    if season > "2015":
        return "http://data.nba.net/prod/v2/{}/schedule.json".format(season)
    else:
        return "http://data.nba.net/prod/v1/{}/schedule.json".format(season)

sql = MySQLConnector.MySQLConnector()
http = urllib3.PoolManager()

r = http.request('GET', url(season))
resp = json.loads(r.data)
games = pd.DataFrame()
for game in resp["league"]["standard"]:
    if game["seasonStageId"] == 2:
        teams = [int(game["hTeam"]["teamId"]), int(game["vTeam"]["teamId"]), int(game["hTeam"]["score"]>game["vTeam"]["score"]), season]
        games = games.append(pd.Series(teams), ignore_index=True)
columns = ["HomeTeam", "AwayTeam", "HomeWin", "Season"]

games.columns = columns

sql.write(games, MySqlDatabases.NBADatabase.league_results, MySqlDatabases.NBADatabase.NAME)







import json

import pandas as pd
import urllib3

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

seasons = [("2015", "2015-16"),("2016", "2016-17"),("2017", "2017-18"),("2018", "2018-19")]
def url(season):
    if season > "2015":
        return "http://data.nba.net/prod/v2/{}/schedule.json".format(season)
    else:
        return "http://data.nba.net/prod/v1/{}/schedule.json".format(season)

sql = MySQLConnector.MySQLConnector()
http = urllib3.PoolManager()

for (season, full_season) in seasons:
    print(season)
    print(full_season)
    r = http.request('GET', url(season))
    resp = json.loads(r.data)
    games = pd.DataFrame()
    for game in resp["league"]["standard"]:
        if game["seasonStageId"] == 2:
            teams = [game["gameId"], int(game["hTeam"]["teamId"]), int(game["vTeam"]["teamId"]), int(game["hTeam"]["score"]>game["vTeam"]["score"]), full_season]
            games = games.append(pd.Series(teams), ignore_index=True)
    columns = ["gameId", "homeTeam", "awayTeam", "homeWin", "season"]

    games.columns = columns

    #sql.truncate_table(MySqlDatabases.NBADatabase.league_results,MySqlDatabases.NBADatabase.NAME, "season = '{0}'".format(season))
    sql.write(games, MySqlDatabases.NBADatabase.league_results, MySqlDatabases.NBADatabase.NAME)







import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

season = "2018-19"
seasonType = "Regular Season"

stints = sql.runQuery("SELECT * FROM nba.luck_adjusted_one_way_possessions where season = '{}' and seasonType ='{}';".format(season, seasonType))
teams = sql.runQuery("select * from nba.team_info where season = '{0}'".format(season))

game_record = sql.runQuery("select * from nba.game_record where season = '{0}'".format(season))

team_stats = stints[["gameId", "offenseTeamId1", "points"]].groupby(by=["gameId", "offenseTeamId1"]).sum().reset_index()

team_stats = team_stats.merge(teams, left_on="offenseTeamId1", right_on="teamId")[["gameId","teamId", "teamName", "points"]]

game_scores = game_record[["gameId", "teamId", "points"]]


merged = team_stats.merge(game_scores, on=["teamId", "gameId"], suffixes=("_calculated", "_expected"))

merged["fail"] = merged["points_calculated"] != merged["points_expected"]

print(merged[merged["fail"] == True])




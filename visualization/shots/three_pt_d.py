import pandas as pd
import numpy as np

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"
season_type = "Regular Season"

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

query = "SELECT * FROM nba.team_scored_shots where season = '{}' and seasonType = '{}';".format(season, season_type)
teams_query = "select * from nba.team_info where season = '{0}'".format(season)
shots = sql.runQuery(query)
teams = sql.runQuery(teams_query)

shots_3 = shots[shots['shotValue'] == 3]

shots_3['points'] = shots_3['shotValue'] * shots_3['shotMade']

epps = shots_3.groupby(by='defenseTeamId')[['expectedPoints', 'points']].mean().reset_index()

epps = epps.merge(teams, left_on='defenseTeamId', right_on='teamId')[['teamName', 'expectedPoints', 'points']]
print(epps)
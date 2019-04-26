import numpy as np
import pandas as pd

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#
#
# regress combo of 4F + RAPM + LA_RAPM to which team wins
# run MCMC
#


sql = MySQLConnector.MySQLConnector()


# seasons = [("2012-15", "2015-16")]


seasons = ["2015-16", "2016-17", "2017-18"]

for season in seasons:
    advanced_box_score = sql.runQuery("SELECT * FROM nba.raw_team_box_score_advanced where season = '{}'".format(season))
    schedule = sql.runQuery("SELECT * FROM nba.league_results where season = '{}'".format(season))

    joined = schedule.merge(advanced_box_score, left_on=["gameId","homeTeam"], right_on=["gameId","teamId"], suffixes=('', '_home')).merge(advanced_box_score, left_on=["gameId","awayTeam"], right_on=["gameId","teamId"], suffixes=('', '_away'))
    print(joined)


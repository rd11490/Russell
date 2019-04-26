import numpy as np
import pandas as pd

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#
#
#

sql = MySQLConnector.MySQLConnector()

seasons = [("2012-15", "2012-13"), ("2013-16", "2013-14"), ("2014-17", "2014-15")]

rookie_rapm = pd.DataFrame()

columns = ["LA_RAPM", "LA_RAPM__Def", "LA_RAPM__Off", "RAPM", "RAPM__Def", "RAPM__Off", "RA_EFG", "RA_EFG__Def", "RA_EFG__Off", "RA_FTR", "RA_FTR__Def", "RA_FTR__Off", "RA_ORBD", "RA_ORBD__Def", "RA_ORBD__Off", "RA_TOV", "RA_TOV__Def", "RA_TOV__Off"]

for (season_rapm, season_draft) in seasons:
    rapm = sql.runQuery("SELECT * FROM nba.real_adjusted_four_factors_multi where season = '{}'".format(season_rapm))
    draft = sql.runQuery("SELECT * FROM nba.draft where season = '{}'".format(season_draft))

    joined = rapm.merge(draft, left_on="playerId", right_on="PERSON_ID", how="left").dropna()
    rookie_rapm=pd.concat([rookie_rapm, joined])

print(rookie_rapm[columns].mean())

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()


stints_query = "SELECT * FROM nba.luck_adjusted_one_way_stints;"
poss_query = "SELECT * FROM nba.luck_adjusted_stints;"
seconds_query = "SELECT * FROM nba.seconds_played;"

stints = sql.runQuery(stints_query)
sql.truncate_table(MySqlDatabases.NBADatabase.luck_adjusted_one_way_stints, MySqlDatabases.NBADatabase.NAME)
stints['primaryKey'] = stints['primaryKey'] + "_" + stints['seasonType']
sql.write(stints, MySqlDatabases.NBADatabase.luck_adjusted_one_way_stints, MySqlDatabases.NBADatabase.NAME)

stints = sql.runQuery(poss_query)
sql.truncate_table(MySqlDatabases.NBADatabase.luck_adjusted_stints, MySqlDatabases.NBADatabase.NAME)
stints['primaryKey'] = stints['primaryKey'] + "_" + stints['seasonType']
sql.write(stints, MySqlDatabases.NBADatabase.luck_adjusted_stints, MySqlDatabases.NBADatabase.NAME)

stints = sql.runQuery(seconds_query)
sql.truncate_table(MySqlDatabases.NBADatabase.seconds_played, MySqlDatabases.NBADatabase.NAME)
stints['primaryKey'] = stints['primaryKey'] + "_" + stints['seasonType']
sql.write(stints, MySqlDatabases.NBADatabase.seconds_played, MySqlDatabases.NBADatabase.NAME)
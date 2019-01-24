import pandas as pd

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()

pd.read_csv("results/Luck Adjusted RAPM 2017-18.csv")
stintsQuery = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{}';".format("2017-18")
stints = sql.runQuery(stintsQuery)

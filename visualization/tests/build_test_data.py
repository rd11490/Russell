import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

season = "2018-19"

events_query = "SELECT * FROM nba.luck_adjusted_one_way_possessions where season = '{0}';".format(season)

events = sql.runQuery(events_query)

new_events = events[['offensePlayer1Id', 'offensePlayer2Id', 'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                     'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id', 'defensePlayer4Id', 'defensePlayer5Id',
                     'points', 'possessions']]

new_events.to_csv('RAPM_Possessions.csv')



import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

season = '2018-19'
seasonType = 'Regular Season'
stints_query = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{0}' and seasonType ='{1}' and possessions > 0;".format(season, seasonType)
stints = sql.runQuery(stints_query)

stints['o_lineup'] = stints["offensePlayer1Id"].astype(str) + '-' + stints["offensePlayer2Id"].astype(str) + '-'+ stints["offensePlayer3Id"].astype(str) + '-' + stints["offensePlayer4Id"].astype(str) + '-' +stints["offensePlayer5Id"].astype(str)
stints['d_lineup'] = stints["defensePlayer1Id"].astype(str) + '-' + stints["defensePlayer2Id"].astype(str) + '-'+ stints["defensePlayer3Id"].astype(str) + '-' + stints["defensePlayer4Id"].astype(str) + '-' +stints["defensePlayer5Id"].astype(str)

stints_poss_limit = stints[stints['possessions'] > 15]

lineups = list(
        set(list(stints_poss_limit["o_lineup"]) + list(stints_poss_limit["d_lineup"]) + ['default']))
lineups.sort()

possessions = stints['possessions'].sum()

stints_rep = stints[(stints['o_lineup'].isin(lineups)) | (stints['d_lineup'].isin(lineups))]

possessions_rep = stints_rep['possessions'].sum()

print(possessions)
print(possessions_rep)
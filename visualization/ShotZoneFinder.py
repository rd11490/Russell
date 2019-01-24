import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2018-19"
seasonType = "Regular Season"

shots_query = "SELECT * FROM nba.lineup_shots where season = '{}' and seasonType ='{}';".format(season, seasonType)

shots = sql.runQuery(shots_query)

cnt = shots.count()

shots['PPS'] = shots['shotMadeFlag'] * shots['shotValue']
shots['Freq'] = 1/cnt

means = shots.groupby(by=['shotZone']).mean()['PPS']
stds = shots.groupby(by=['shotZone']).std()['PPS']





out = pd.concat([means, stds], axis=1)

out.columns = ['mean', 'std']

out['lower'] = out['mean']-out['std']
out['upper'] = out['mean']+out['std']

print(out)


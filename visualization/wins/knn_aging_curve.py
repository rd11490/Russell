import pandas as pd
import MySqlDatabases.NBADatabase
from cred import MySQLConnector
import numpy as np
from sklearn.cluster import KMeans
from sklearn import preprocessing



pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()


advanced_season = sql.runQuery('SELECT * FROM nba_data.player_season_totals_advanced;')
advanced_season = advanced_season[['PLAYER_ID', 'PLAYER_NAME', 'AGE', 'USG_PCT','POSS', 'SEASON']]


tracking_eff = sql.runQuery('SELECT * FROM nba_data.player_tracking_efficiency;')
tracking_eff = tracking_eff[['PLAYER_ID', 'POINTS', 'DRIVE_PTS', 'DRIVE_FG_PCT', 'CATCH_SHOOT_PTS', 'CATCH_SHOOT_FG_PCT', 'PULL_UP_PTS', 'PULL_UP_FG_PCT', 'PAINT_TOUCH_PTS', 'PAINT_TOUCH_FG_PCT', 'POST_TOUCH_PTS', 'POST_TOUCH_FG_PCT', 'ELBOW_TOUCH_PTS', 'ELBOW_TOUCH_FG_PCT', 'EFF_FG_PCT', 'SEASON']]
tracking_eff = tracking_eff.merge(advanced_season, on=['PLAYER_ID', 'SEASON'], how='inner')
tracking_eff['DRIVE_PTS_PCT'] = tracking_eff['DRIVE_PTS']/tracking_eff['POINTS']
tracking_eff['CATCH_SHOOT_PTS_PCT'] = tracking_eff['CATCH_SHOOT_PTS']/tracking_eff['POINTS']
tracking_eff['PULL_UP_PTS_PCT'] = tracking_eff['PULL_UP_PTS']/tracking_eff['POINTS']
tracking_eff['PAINT_TOUCH_PTS_PCT'] = tracking_eff['PAINT_TOUCH_PTS']/tracking_eff['POINTS']
tracking_eff['POST_TOUCH_PTS_PCT'] = tracking_eff['POST_TOUCH_PTS']/tracking_eff['POINTS']
tracking_eff['ELBOW_TOUCH_PTS_PCT'] = tracking_eff['ELBOW_TOUCH_PTS']/tracking_eff['POINTS']
tracking_eff=tracking_eff.replace([np.inf, -np.inf], np.nan)
tracking_eff = tracking_eff.fillna(0.0)

tracking_eff=tracking_eff.drop(['DRIVE_PTS', 'CATCH_SHOOT_PTS', 'PULL_UP_PTS', 'PAINT_TOUCH_PTS','POST_TOUCH_PTS','ELBOW_TOUCH_PTS'], axis=1)


model_data_input = tracking_eff[['POINTS','POSS', 'DRIVE_FG_PCT', 'CATCH_SHOOT_FG_PCT', 'PULL_UP_FG_PCT', 'PAINT_TOUCH_FG_PCT', 'POST_TOUCH_FG_PCT', 'ELBOW_TOUCH_FG_PCT', 'EFF_FG_PCT', 'DRIVE_PTS_PCT', 'CATCH_SHOOT_PTS_PCT', 'PULL_UP_PTS_PCT', 'PAINT_TOUCH_PTS_PCT', 'POST_TOUCH_PTS_PCT', 'ELBOW_TOUCH_PTS_PCT', 'USG_PCT']]



print(model_data_input.head(10))

x = model_data_input.values #returns a numpy array
min_max_scaler = preprocessing.MinMaxScaler()
x_scaled = min_max_scaler.fit_transform(x)
model_data_input = pd.DataFrame(x_scaled, columns=model_data_input.columns)


print(model_data_input.head(10))

kmeans = KMeans(n_clusters=12, n_init=50)

clusters = kmeans.fit_predict(X=model_data_input)
tracking_eff['cluster'] = clusters

print(tracking_eff)

tracking_eff.to_csv('player_clusters.csv', index=False)
print(tracking_eff.groupby('cluster').count())
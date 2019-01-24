import pandas as pd
from sklearn import linear_model
from sklearn import metrics

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

season = "2017"
season_full = "2017-18"


season_query = "SELECT * FROM nba.{0};".format(MySqlDatabases.NBADatabase.league_results, season)
net_rtg_query = "SELECT * FROM nba.{0};".format(MySqlDatabases.NBADatabase.league_net_rtg, season_full)

sql = MySQLConnector.MySQLConnector()

season_results = sql.runQuery(season_query)
net_rtg = sql.runQuery(net_rtg_query)[["TEAM_ID", "TEAM_NAME", "OFF_RATING", "DEF_RATING", "NET_RATING", "season"]]

print(season_results.head(10))
print(net_rtg.head(10))

season_results_with_home_rtg = pd.merge(season_results, net_rtg, left_on=['HomeTeam', "Season"], right_on=['TEAM_ID', "season"])[["HomeTeam", "TEAM_NAME", "AwayTeam",  "OFF_RATING", "DEF_RATING", "NET_RATING", "HomeWin", "Season"]]

season_results_with_home_rtg.columns = ["HomeTeam", "HomeTeamName", "AwayTeam",  "HomeTeamOffRTG", "HomeTeamDefRTG", "HomeTeamNetRTG", "HomeWin", "season"]

season_results_with_rtg = pd.merge(season_results_with_home_rtg, net_rtg, left_on=['AwayTeam', "season"], right_on=['TEAM_ID', "season"])[["HomeTeam", "HomeTeamName",  "HomeTeamOffRTG", "HomeTeamDefRTG", "HomeTeamNetRTG", "AwayTeam", "TEAM_NAME",  "OFF_RATING", "DEF_RATING", "NET_RATING", "HomeWin"]]
season_results_with_rtg.columns = ["HomeTeam", "HomeTeamName",  "HomeTeamOffRTG", "HomeTeamDefRTG", "HomeTeamNetRTG", "AwayTeam", "AwayTeamName",  "AwayTeamOffRTG", "AwayTeamDefRTG", "AwayTeamNetRTG", "HomeWin"]

training_data = season_results_with_rtg[["HomeTeamOffRTG", "HomeTeamDefRTG", "HomeTeamNetRTG", "AwayTeamOffRTG", "AwayTeamDefRTG", "AwayTeamNetRTG", "HomeWin"]]
training_data["Net"] = training_data["HomeTeamNetRTG"] - training_data["AwayTeamNetRTG"]

print(training_data[["HomeTeamOffRTG", "HomeTeamDefRTG", "AwayTeamOffRTG", "AwayTeamDefRTG", "HomeWin"]])
X = training_data.as_matrix(["Net"])
y = training_data.as_matrix(["HomeWin"])

clf = linear_model.LogisticRegression(C=100000.0, random_state=0, solver='liblinear', fit_intercept=False)
clf.fit(X, y)

print(clf.coef_)

pred = clf.predict_proba(X)[:,1]

training_data["Pred"] = pred

print(training_data[training_data["Net"] >10].head(100))
print("METRICS:")

print("max: {}".format(max(pred)))
print("min: {}".format(min(pred)))

print("METRICS:")

abs_error = metrics.mean_absolute_error(y, pred)
print("mean absolute error: {}".format(abs_error))

rms_error = metrics.mean_squared_error(y, pred)
print("mean squared error: {}".format(rms_error))

log_error = metrics.mean_squared_error(y, pred)
print("log squared error: {}".format(log_error))


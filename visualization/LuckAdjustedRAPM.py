import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV
import matplotlib.pyplot as plt

import MySQLConnector
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2017-18"

secondsQuery = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}';".format(season)
stintsQuery = "SELECT * FROM nba.luck_adjusted_stints where season = '{}';".format(season)
playerNamesQuery = "select playerId, playerName from nba.roster_player where season = '{}';".format(season)

stints = sql.runQuery(stintsQuery)
playerNames = sql.runQuery(playerNamesQuery).drop_duplicates()
secondsPlayed = sql.runQuery(secondsQuery)
secondsPlayedMap = secondsPlayed.set_index("playerId").to_dict()["secondsPlayed"]

players = list(
    set(list(stints["team1player1Id"]) + list(stints["team1player2Id"]) + list(stints["team1player3Id"]) + \
        list(stints["team1player4Id"]) + list(stints["team1player5Id"]) + list(stints["team2player1Id"]) + \
        list(stints["team2player2Id"]) + list(stints["team2player3Id"]) + list(stints["team2player4Id"]) + \
        list(stints["team2player5Id"])))
players.sort()

filteredPlayers = players  # [p for p in players if secondsPlayedMap[p] > shotCutOff]


def map_players(row_in):
    p1 = row_in[0]
    p2 = row_in[1]
    p3 = row_in[2]
    p4 = row_in[3]
    p5 = row_in[4]
    p6 = row_in[5]
    p7 = row_in[6]
    p8 = row_in[7]
    p9 = row_in[8]
    p10 = row_in[9]

    rowOut = np.zeros([len(filteredPlayers) * 2])

    rowOut[filteredPlayers.index(p1)] = 1
    rowOut[filteredPlayers.index(p2)] = 1
    rowOut[filteredPlayers.index(p3)] = 1
    rowOut[filteredPlayers.index(p4)] = 1
    rowOut[filteredPlayers.index(p5)] = 1

    rowOut[filteredPlayers.index(p6) + len(filteredPlayers)] = -1
    rowOut[filteredPlayers.index(p7) + len(filteredPlayers)] = -1
    rowOut[filteredPlayers.index(p8) + len(filteredPlayers)] = -1
    rowOut[filteredPlayers.index(p9) + len(filteredPlayers)] = -1
    rowOut[filteredPlayers.index(p10) + len(filteredPlayers)] = -1

    return rowOut

stints = stints[stints["team1Possessions"] > 0]
stints = stints[stints["team2Possessions"] > 0]

stints["team1PPP"] = 100 * stints["team1Points"] / stints["team1Possessions"]
stints["team2PPP"] = 100 * stints["team2Points"] / stints["team2Possessions"]

stints["team1AdjPPP"] = 100 * stints["team1ExpectedPoints"] / stints["team1Possessions"]
stints["team2AdjPPP"] = 100 * stints["team2ExpectedPoints"] / stints["team2Possessions"]

stints = stints.drop_duplicates()
stintsForReg = stints[["team1player1Id", "team1player2Id",
                       "team1player3Id", "team1player4Id", "team1player5Id",
                       "team2player1Id", "team2player2Id", "team2player3Id",
                       "team2player4Id", "team2player5Id", "team1PPP",
                       "team1AdjPPP", "team2PPP", "team2AdjPPP"]]

stintXBase = stintsForReg.as_matrix(columns=["team1player1Id", "team1player2Id",
                                         "team1player3Id", "team1player4Id", "team1player5Id",
                                         "team2player1Id", "team2player2Id", "team2player3Id",
                                         "team2player4Id", "team2player5Id"])

stintX = np.apply_along_axis(map_players, 1, stintXBase)
stintX2 = np.apply_along_axis(map_players, 1, stintXBase) * -1


stintX = np.concatenate((stintX, stintX2))


stintY = stintsForReg.as_matrix(["team1AdjPPP"])
stintY2 = stintsForReg.as_matrix(["team2AdjPPP"])

stintY = np.concatenate((stintY, stintY2))

alphas = np.array([0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0])
clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True)

weights = [secondsPlayedMap[p] for p in filteredPlayers]
weights = np.array(weights)
weights = np.concatenate((weights, weights))
print(weights)

clf.coef_ = weights
model = clf.fit(stintX, stintY)


playerArr = np.transpose(np.array(filteredPlayers).reshape(1, len(filteredPlayers)))
coefDArr = np.transpose(model.coef_[:, 0:len(filteredPlayers)])
coefOArr = np.transpose(model.coef_[:, len(filteredPlayers):])
playerIdWithCoef = np.concatenate([playerArr, coefOArr, coefDArr], axis=1)

playersCoef = pd.DataFrame(playerIdWithCoef)
playersCoef.columns = ["playerId", "Luck Adjusted ORAPM", "Luck Adjusted DRAPM"]

merged = playersCoef.merge(playerNames, how="inner", on="playerId")[
    ["playerName", "Luck Adjusted ORAPM", "Luck Adjusted DRAPM"]]

merged["Luck Adjusted RAPM"] = merged["Luck Adjusted ORAPM"] + merged["Luck Adjusted DRAPM"]

merged.to_csv("results/Luck Adjusted RAPM {}.csv".format(season))

mergedO = merged.sort_values(by="Luck Adjusted ORAPM", ascending=False)

print("Top 20 Offensive Players by Luck Adjusted ORAPM")
print(mergedO.head(20))

print("Bottom 20 Offensive Players by Luck Adjusted ORAPM")
print(mergedO.tail(20))

mergedD = merged.sort_values(by="Luck Adjusted DRAPM", ascending=False)

print("Top 20 Defensive Players by Luck Adjusted DRAPM")
print(mergedD.head(20))

print("Bottom 20 Defensive Players by Luck Adjusted DRAPM")
print(mergedD.tail(20))

mergedD = merged.sort_values(by="Luck Adjusted RAPM", ascending=False)

print("Top 20 Players by Luck Adjusted RAPM")
print(mergedD.head(20))

print("Bottom 20 Players by Luck Adjusted RAPM")
print(mergedD.tail(20))

print("r^2 value: {}".format(model.score(stintX, stintY)))
print(model.alpha_)
print(model.intercept_)

pred = model.predict(stintX)

err = pred - stintY

print("max: {}".format(max(err)))
print("min: {}".format(min(err)))

err = pred - stintY
print("METRICS:")

print("max: {}".format(max(err)))
print("min: {}".format(min(err)))

abs_error = metrics.mean_absolute_error(stintY, pred)
print("mean absolute error: {}".format(abs_error))

rms_error = metrics.mean_squared_error(stintY, pred)
print("mean squared error: {}".format(rms_error))

log_error = metrics.mean_squared_log_error(stintY, pred)
print("log squared error: {}".format(log_error))

r2 = metrics.r2_score(stintY, pred)
print("r2: {}".format(r2))

# stints["Prediction"] = pred
#
# stints["Error"] = stints["Prediction"] - stints[shot_frequency]
#
#
# def calculate_rmse(group):
#     group["RMSE"] = ((group["Error"]) ** 2).mean() ** .5
#     group["AvgError"] = abs(group["Error"]).mean()
#     group["Count"] = len(group) / 6
#
#     return group
#
#
# rmse = stints.groupby(by=true_attempts).apply(calculate_rmse)
#
# rmse_for_plot = rmse[[true_attempts, "Count", "RMSE", "AvgError"]].drop_duplicates().sort_values(by=true_attempts)
#
# print(rmse_for_plot)
#
# fig, ax = plt.subplots()
# rmse_for_plot.plot.scatter(ax=ax, x=true_attempts, y="RMSE", color="Red", )
# rmse_for_plot.plot.scatter(ax=ax, x=true_attempts, y="AvgError", color="Blue")
# plt.legend(["RMSE", "AvgError"])
# plt.xlabel("Attempts")
# plt.ylabel("Error")
# plt.title("In-Sample Error for Regularized 3Pt Frequency")
# plt.ylim([0, 50])
# plt.show()

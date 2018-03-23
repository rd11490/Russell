import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV
import matplotlib.pyplot as plt
from sklearn.preprocessing import normalize
from sklearn import preprocessing


import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2017-18"

secondsQuery = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}';".format(season)
stintsQuery = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{}';".format(season)
playerNamesQuery = "select playerId, playerName from nba.roster_player where season = '{}';".format(season)

stints = sql.runQuery(stintsQuery)
playerNames = sql.runQuery(playerNamesQuery).drop_duplicates()
secondsPlayed = sql.runQuery(secondsQuery)
secondsPlayedMap = secondsPlayed.set_index("playerId").to_dict()["secondsPlayed"]

players = list(
    set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
        list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
        list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
        list(stints["defensePlayer5Id"])))
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


stints = stints[stints["possessions"] > 0]

stints["PointsPerPossession"] = 100 * stints["points"] / stints["possessions"]

stints["ExpectedPointsPerPossession"] = 100 * stints["expectedPoints"] / stints["possessions"]

print(stints)

stints = stints.drop_duplicates()
stintsForReg = stints[["offensePlayer1Id", "offensePlayer2Id",
                       "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
                       "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
                       "defensePlayer4Id", "defensePlayer5Id", "PointsPerPossession",
                       "ExpectedPointsPerPossession"]]

stintXBase = stintsForReg.as_matrix(columns=["offensePlayer1Id", "offensePlayer2Id",
                                             "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
                                             "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
                                             "defensePlayer4Id", "defensePlayer5Id"])

stintX = np.apply_along_axis(map_players, 1, stintXBase)

stintYAdjusted = stintsForReg.as_matrix(["ExpectedPointsPerPossession"])
stintYRaw = stintsForReg.as_matrix(["PointsPerPossession"])


alphas = [1000, 2500, 5000, 7500, 10000, 12500, 15000, 20000, 50000]
clfAdjusted = RidgeCV(alphas=alphas, cv=10, fit_intercept=True, normalize=False)
clfRaw = RidgeCV(alphas=alphas, cv=10, fit_intercept=True, normalize=False)


weights = [secondsPlayedMap[p]/60 for p in filteredPlayers]
weights = np.array(weights)
weights = np.concatenate((weights, weights))
weights = weights - weights.mean()
print(weights)



clfAdjusted.coef_ = weights
clfRaw.coef_ = weights

sample_weights = stints["possessions"].values

print(clfAdjusted.coef_)

modelAdjusted = clfAdjusted.fit(stintX, stintYAdjusted, sample_weight=sample_weights)
modelRaw = clfRaw.fit(stintX, stintYRaw, sample_weight=sample_weights)


playerArr = np.transpose(np.array(filteredPlayers).reshape(1, len(filteredPlayers)))
coefOArrAdj = np.transpose(modelAdjusted.coef_[:, 0:len(filteredPlayers)])
coefDArrAdj = np.transpose(modelAdjusted.coef_[:, len(filteredPlayers):])

coefOArrRaw = np.transpose(modelRaw.coef_[:, 0:len(filteredPlayers)])
coefDArrRaw = np.transpose(modelRaw.coef_[:, len(filteredPlayers):])

playerIdWithCoef = np.concatenate([playerArr, coefOArrAdj, coefDArrAdj, coefOArrRaw, coefDArrRaw], axis=1)

playersCoef = pd.DataFrame(playerIdWithCoef)
playersCoef.columns = ["playerId", "Luck Adjusted ORAPM", "Luck Adjusted DRAPM", "ORAPM", "DRAPM"]

merged = playersCoef.merge(playerNames, how="inner", on="playerId")[
    ["playerName", "Luck Adjusted ORAPM", "Luck Adjusted DRAPM", "ORAPM", "DRAPM"]]

merged["Luck Adjusted RAPM"] = merged["Luck Adjusted ORAPM"] + merged["Luck Adjusted DRAPM"]
merged["RAPM"] = merged["ORAPM"] + merged["DRAPM"]

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

# # print("r^2 value: {}".format(model.score(stintX, stintY)))
# # print(model.alpha_)
# # print(model.intercept_)
# #
# # pred = model.predict(stintX)
# #
# # err = pred - stintY
#
# print("max: {}".format(max(err)))
# print("min: {}".format(min(err)))
#
# err = pred - stintY
# print("METRICS:")
#
# print("max: {}".format(max(err)))
# print("min: {}".format(min(err)))
#
# abs_error = metrics.mean_absolute_error(stintY, pred)
# print("mean absolute error: {}".format(abs_error))
#
# rms_error = metrics.mean_squared_error(stintY, pred)
# print("mean squared error: {}".format(rms_error))
#
# log_error = metrics.mean_squared_log_error(stintY, pred)
# print("log squared error: {}".format(log_error))
#
# r2 = metrics.r2_score(stintY, pred)
# print("r2: {}".format(r2))

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

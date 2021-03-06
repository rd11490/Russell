import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"
shotCutOff = 250

shot_frequency = "shotFrequency"
attempts = "attempts"
true_attempts = "true_attempts"


shotsSeenQuery = "SELECT playerId, shots FROM nba.shots_seen where season = '{}';".format(season)
stintsQuery = "SELECT * FROM nba.shot_stint_data where season = '{}' and bin != 'Total';".format(season)
playerNamesQuery = "select playerId, playerName, teamId from nba.roster_player where season = '{}';".format(season)

o_query = "SELECT * FROM  nba.offense_expected_points_by_player_on_off_zoned " \
          "WHERE season = '{0}' and bin != 'Total'".format(season)

d_query = "SELECT * FROM  nba.defense_expected_points_by_player_on_off_zoned " \
          "WHERE season = '{0}' and bin != 'Total'".format(season)

stints = sql.runQuery(stintsQuery)
playerNames = sql.runQuery(playerNamesQuery).drop_duplicates()
shotsSeen = sql.runQuery(shotsSeenQuery)
shotSeenMap = shotsSeen.set_index("playerId").to_dict()["shots"]

o_shots = sql.runQuery(o_query)
o_shots = o_shots[o_shots["onOff"] == "On"]

d_shots = sql.runQuery(d_query)
d_shots = d_shots[d_shots["onOff"] == "On"]

players = list(
    set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
        list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
        list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
        list(stints["defensePlayer5Id"])))
players.sort()

filteredPlayers = [p for p in players if shotSeenMap[p] > shotCutOff]


def calculate_shot_frequency(df):
    df[shot_frequency] = 100 * df[attempts] / df[attempts].sum()
    df[true_attempts] = df[attempts].sum()
    return df


def check_player_qalifies(p):
    return shotSeenMap[p] > shotCutOff


def map_players(rowIn):
    p1 = rowIn[0]
    p2 = rowIn[1]
    p3 = rowIn[2]
    p4 = rowIn[3]
    p5 = rowIn[4]
    p6 = rowIn[5]
    p7 = rowIn[6]
    p8 = rowIn[7]
    p9 = rowIn[8]
    p10 = rowIn[9]

    rowOut = np.zeros([len(filteredPlayers) * 2])

    if check_player_qalifies(p1):
        rowOut[filteredPlayers.index(p1)] = 1
    if check_player_qalifies(p2):
        rowOut[filteredPlayers.index(p2)] = 1
    if check_player_qalifies(p3):
        rowOut[filteredPlayers.index(p3)] = 1
    if check_player_qalifies(p4):
        rowOut[filteredPlayers.index(p4)] = 1
    if check_player_qalifies(p5):
        rowOut[filteredPlayers.index(p5)] = 1

    if check_player_qalifies(p6):
        rowOut[filteredPlayers.index(p6) + len(filteredPlayers)] = -1
    if check_player_qalifies(p7):
        rowOut[filteredPlayers.index(p7) + len(filteredPlayers)] = -1
    if check_player_qalifies(p8):
        rowOut[filteredPlayers.index(p8) + len(filteredPlayers)] = -1
    if check_player_qalifies(p9):
        rowOut[filteredPlayers.index(p9) + len(filteredPlayers)] = -1
    if check_player_qalifies(p10):
        rowOut[filteredPlayers.index(p10) + len(filteredPlayers)] = -1

    return rowOut


def filter_row(row):
    # Filter stints that have players who have seen less than n shots
    return check_player_qalifies(row["offensePlayer1Id"]) and check_player_qalifies(
        row["offensePlayer2Id"]) and check_player_qalifies(row["offensePlayer3Id"]) and check_player_qalifies(
        row["offensePlayer4Id"]) and check_player_qalifies(row["offensePlayer5Id"]) and check_player_qalifies(
        row["defensePlayer1Id"]) and check_player_qalifies(row["defensePlayer2Id"]) and check_player_qalifies(
        row["defensePlayer3Id"]) and check_player_qalifies(row["defensePlayer4Id"]) and check_player_qalifies(
        row["defensePlayer5Id"])


bin_totals = stints["attempts"].groupby(stints["bin"]).sum().reset_index()
bin_totals.columns = ["bin", attempts]
print(bin_totals)
league_freq = calculate_shot_frequency(bin_totals)
print(league_freq)
league_avg = league_freq[league_freq['bin'] == "RestrictedArea"][shot_frequency].values[0]
print(league_avg)

stints = stints.groupby(
    by=["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
        "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id", "defensePlayer5Id"]).apply(
    calculate_shot_frequency)

o_shots = o_shots.groupby(by=["id"]).apply(calculate_shot_frequency)
o_shots = o_shots[o_shots['bin'] == 'RestrictedArea']
o_shots = o_shots[["id", shot_frequency]]
o_shots = o_shots.set_index("id").to_dict()[shot_frequency]

d_shots = d_shots.groupby(by=["id"]).apply(calculate_shot_frequency)
d_shots = d_shots[d_shots['bin'] == 'RestrictedArea']
d_shots = d_shots[["id", shot_frequency]]
d_shots = d_shots.set_index("id").to_dict()[shot_frequency]

stints = stints[stints['bin'] == "RestrictedArea"]

stints = stints[stints.apply(filter_row, axis=1)]
stints = stints.drop_duplicates()

stintsForReg = stints[['offensePlayer1Id', 'offensePlayer2Id',
                       'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                       'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                       'defensePlayer4Id', 'defensePlayer5Id', shot_frequency]]

stintX = stintsForReg.as_matrix(columns=['offensePlayer1Id', 'offensePlayer2Id',
                                         'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                                         'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                                         'defensePlayer4Id', 'defensePlayer5Id'])

stintX = np.apply_along_axis(map_players, 1, stintX)

stintY = stintsForReg.as_matrix([shot_frequency])

weights = np.zeros((len(filteredPlayers) * 2) + 1)
for i, f in enumerate(filteredPlayers):
    weights[i] = o_shots[f]
    weights[i + len(filteredPlayers)] = d_shots[f]
weights = np.transpose(weights)

print(len(filteredPlayers))
print(weights.shape)
print(len(weights))
print(stintX.shape)
print(stintY.shape)

alphas = np.array([0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0])
clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True)
# clf.coef_ = weights
model = clf.fit(stintX, stintY)

playerArr = np.transpose(np.array(filteredPlayers).reshape(1, len(filteredPlayers)))
coefOArr = np.transpose(model.coef_[:, 0:len(filteredPlayers)])
coefDArr = np.transpose(model.coef_[:, len(filteredPlayers):])
playerIdWithCoef = np.concatenate([playerArr, coefOArr, coefDArr], axis=1)

playersCoef = pd.DataFrame(playerIdWithCoef)
playersCoef.columns = ["playerId", "RimAttempt O", "RimAttempt D"]

merged = playersCoef.merge(playerNames, how='inner', on="playerId")[
    ["playerId", "playerName", "RimAttempt O", "RimAttempt D"]]

merged.to_csv("results/RimAttemptImpact{}.csv".format(season))

mergedO = merged.sort_values(by="RimAttempt O", ascending=False)

print("Top 20 Offensive Rim Attempt Impact")
print(mergedO.head(20))

print("Bottom 20 Offensive Rim Attempt Impact")
print(mergedO.tail(20))

mergedD = merged.sort_values(by="RimAttempt D", ascending=False)

print("Top 20 Defensive Rim Attempt Impact")
print(mergedD.head(20))

print("Bottom 20 Defensive Rim Attempt Impact")
print(mergedD.tail(20))

print("r^2 value: {}".format(model.score(stintX, stintY)))
print(model.alpha_)
print(model.intercept_)

pred = model.predict(stintX)

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

stints["Prediction"] = pred

stints["Error"] = stints["Prediction"] - stints[shot_frequency]


def calculate_rmse(group):
    group["RMSE"] = ((group["Error"]) ** 2).mean() ** .5
    group["AvgError"] = abs(group["Error"]).mean()
    group["Count"] = len(group)
    return group


rmse = stints.groupby(by=true_attempts).apply(calculate_rmse)

rmse_for_plot = rmse[[true_attempts, "Count", "RMSE", "AvgError"]].drop_duplicates().sort_values(by=true_attempts)

print(rmse_for_plot)

fig, ax = plt.subplots()
rmse_for_plot.plot.scatter(ax=ax, x=true_attempts, y="RMSE", color="Red", )
rmse_for_plot.plot.scatter(ax=ax, x=true_attempts, y="AvgError", color="Blue")
plt.legend(["RMSE", "AvgError"])
plt.xlabel("Attempts")
plt.ylabel("Error")
plt.title("In-Sample Error for Regularized Rim Attempt Frequency")
plt.ylim([0, 50])
plt.show()

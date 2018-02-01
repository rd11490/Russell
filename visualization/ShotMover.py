from sklearn.linear_model import RidgeCV
import pandas as pd
import numpy as np

import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"
shotCutOff = 250

shotsSeenQuery = "SELECT playerId, shots FROM nba.shots_seen where season = '{}';".format(season)
stintsQuery = "SELECT * FROM nba.shot_stint_data where season = '{}' and bin = 'Total';".format(season)
playerNamesQuery = "select playerId, playerName from nba.roster_player where season = '{}';".format(season)

stints = sql.runQuery(stintsQuery)
playerNames = sql.runQuery(playerNamesQuery)
shotsSeen = sql.runQuery(shotsSeenQuery)
shotSeenMap = shotsSeen.set_index("playerId").to_dict()["shots"]


stints["shotExpectedPointsPer100"] = stints["shotExpectedPoints"] * 100
stints["shotExpectedPointsPer100"] = stints["shotExpectedPoints"] * 100

stints["playerExpectedPointsPer100"] = stints["playerExpectedPoints"] * 100
stints["difference100"] = stints["shotExpectedPointsPer100"] - stints["playerExpectedPointsPer100"]

players = list(
    set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
        list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
        list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
        list(stints["defensePlayer5Id"])))
players.sort()

filteredPlayers = [p for p in players if shotSeenMap[p] > shotCutOff]

def check_player_qalifies(p):
    return shotSeenMap[p] > shotCutOff

def filter_row(row):
    # Filter stints that have players who have seen less than n shots
    return check_player_qalifies(row["offensePlayer1Id"]) and check_player_qalifies(
        row["offensePlayer2Id"]) and check_player_qalifies(row["offensePlayer3Id"]) and check_player_qalifies(
        row["offensePlayer4Id"]) and check_player_qalifies(row["offensePlayer5Id"]) and check_player_qalifies(
        row["defensePlayer1Id"]) and check_player_qalifies(row["defensePlayer2Id"]) and check_player_qalifies(
        row["defensePlayer3Id"]) and check_player_qalifies(row["defensePlayer4Id"]) and check_player_qalifies(
        row["defensePlayer5Id"])

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


stintsForReg = stints[['offensePlayer1Id', 'offensePlayer2Id',
                       'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                       'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                       'defensePlayer4Id', 'defensePlayer5Id', 'difference100']]


stintsForReg = stintsForReg.drop_duplicates()
stintsForReg = stintsForReg[stintsForReg.apply(filter_row, axis=1)]

stintX = stintsForReg.as_matrix(columns=['offensePlayer1Id', 'offensePlayer2Id',
                                         'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                                         'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                                         'defensePlayer4Id', 'defensePlayer5Id'])

stintX = np.apply_along_axis(map_players, 1, stintX)

stintY = stintsForReg.as_matrix(["difference100"])

alphas = np.array([0.01, 0.05, 0.1, 0.5, 1.0, 5, 10, 50, 100, 500, 1000, 2000, 5000])
clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=False)
model = clf.fit(stintX, stintY)

playerArr = np.transpose(np.array(filteredPlayers).reshape(1,len(filteredPlayers)))
coefOArr = np.transpose(model.coef_[:, 0:len(filteredPlayers)])
coefDArr = np.transpose(model.coef_[:, len(filteredPlayers):])
playerIdWithCoef = np.concatenate([playerArr, coefOArr, coefDArr], axis=1)

playersCoef = pd.DataFrame(playerIdWithCoef)
playersCoef.columns = ["playerId", "ShotQualityImpactO", "ShotQualityImpactD"]

merged = playersCoef.merge(playerNames, how='inner', on="playerId")[["playerName", "ShotQualityImpactO", "ShotQualityImpactD"]]

merged.to_csv("results/shotQualityImpact{}.csv".format(season))

mergedO = merged.sort_values(by="ShotQualityImpactO", ascending=False)

print("Top 20 Offensive Shot Quality Impact")
print(mergedO.head(20))

print("Bottom 20 Offensive Shot Quality Impact")
print(mergedO.tail(20))

mergedD = merged.sort_values(by="ShotQualityImpactD", ascending=False)

print("Top 20 Defensive Shot Quality Impact")
print(mergedD.head(20))

print("Bottom 20 Defensive Shot Quality Impact")
print(mergedD.tail(20))


print("r^2 value: {}".format(model.score(stintX, stintY)))
print(model.alpha_)
print(model.intercept_)


from sklearn.linear_model import Ridge
import pandas as pd
import numpy as np

stints = pd.read_csv("data/ShotMoverStints/201617Stints.csv")
playerNames = pd.read_csv("data/ShotMoverStints/playerNames.csv")
shotsSeen = pd.read_csv("data/ShotMoverStints/201617ShotsSeen.csv")
shotSeenMap = shotsSeen.set_index("playerId").to_dict()["shots"]

shotCutOff = 1000


stints["shotExpectedPointsPer100"] = stints["shotExpectedPoints"] * 100
stints["playerExpectedPointsPer100"] = stints["playerExpectedPoints"] * 100
stints["difference100"] = stints["shotExpectedPointsPer100"] - stints["playerExpectedPointsPer100"]

players = list(
    set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
        list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
        list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
        list(stints["defensePlayer5Id"])))
players.sort()

def mapPlayers(rowIn):
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

    rowOut = np.zeros([len(players)])
    if (shotSeenMap[p1]>shotCutOff):
        rowOut[players.index(p1)] = 1
    if (shotSeenMap[p2]>shotCutOff):
        rowOut[players.index(p2)] = 1
    if (shotSeenMap[p3]>shotCutOff):
        rowOut[players.index(p3)] = 1
    if (shotSeenMap[p4]>shotCutOff):
        rowOut[players.index(p4)] = 1
    if (shotSeenMap[p5]>shotCutOff):
        rowOut[players.index(p5)] = 1
    if (shotSeenMap[p6]>shotCutOff):
        rowOut[players.index(p6)] = -1
    if (shotSeenMap[p7]>shotCutOff):
        rowOut[players.index(p7)] = -1
    if (shotSeenMap[p8]>shotCutOff):
        rowOut[players.index(p8)] = -1
    if (shotSeenMap[p9]>shotCutOff):
        rowOut[players.index(p9)] = -1
    if (shotSeenMap[p10]>shotCutOff):
        rowOut[players.index(p10)] = -1
    return rowOut


stintsForReg = stints[['offensePlayer1Id', 'offensePlayer2Id',
                       'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                       'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                       'defensePlayer4Id', 'defensePlayer5Id', 'difference100']]


print(stintsForReg.columns)

stintX = stintsForReg.as_matrix(columns=['offensePlayer1Id', 'offensePlayer2Id',
                       'offensePlayer3Id', 'offensePlayer4Id', 'offensePlayer5Id',
                       'defensePlayer1Id', 'defensePlayer2Id', 'defensePlayer3Id',
                       'defensePlayer4Id', 'defensePlayer5Id'])


stintX = np.apply_along_axis(mapPlayers, 1, stintX)

stintY = stintsForReg.as_matrix(["difference100"])

clf = Ridge(alpha=.1)
clf.fit(stintX, stintY)

playerArr = np.transpose(np.array(players).reshape(1,len(players)))
coefArr = np.transpose(clf.coef_)
playerIdWithCoef = np.concatenate([playerArr, coefArr], axis=1)

playersCoef = pd.DataFrame(playerIdWithCoef)
playersCoef.columns = ["playerId", "ShotMoverScore"]

merged = playersCoef.merge(playerNames, how='inner', on="playerId")[["playerName", "ShotMoverScore"]]

merged = merged.sort_values(by='ShotMoverScore', ascending=False)

print(merged)

print("Top 20 Shot Quality")
print(merged.head(20))

print("Bottom 20 Shot Quality")
print(merged.tail(20))

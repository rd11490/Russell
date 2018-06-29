import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV
from sklearn.linear_model import BayesianRidge

import matplotlib.pyplot as plt
from sklearn.preprocessing import normalize
from sklearn import preprocessing

import MySqlDatabases.NBADatabase


import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2017-18"

seconds_query = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}';".format(season)
stints_query = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{}';".format(season)
player_names_query = "select playerId, playerName from nba.roster_player where season = '{}';".format(season)

stints = sql.runQuery(stints_query)
player_names = sql.runQuery(player_names_query).drop_duplicates()
secondsPlayed = sql.runQuery(seconds_query)
secondsPlayedMap = secondsPlayed.set_index("playerId").to_dict()["secondsPlayed"]

players = list(
    set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
        list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
        list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
        list(stints["defensePlayer5Id"])))
players.sort()

filtered_players = players  # [p for p in players if secondsPlayedMap[p] > shotCutOff]


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

    rowOut = np.zeros([len(filtered_players) * 2])

    rowOut[filtered_players.index(p1)] = 1
    rowOut[filtered_players.index(p2)] = 1
    rowOut[filtered_players.index(p3)] = 1
    rowOut[filtered_players.index(p4)] = 1
    rowOut[filtered_players.index(p5)] = 1

    rowOut[filtered_players.index(p6) + len(filtered_players)] = -1
    rowOut[filtered_players.index(p7) + len(filtered_players)] = -1
    rowOut[filtered_players.index(p8) + len(filtered_players)] = -1
    rowOut[filtered_players.index(p9) + len(filtered_players)] = -1
    rowOut[filtered_players.index(p10) + len(filtered_players)] = -1

    return rowOut


stints = stints[stints["possessions"] > 0]

stints["PointsPerPossession"] = 100 * stints["points"] / stints["possessions"]

stints["ExpectedPointsPerPossession"] = 100 * stints["expectedPoints"] / stints["possessions"]

stints["TurnoverPercent"] = 100 * stints["turnovers"] / stints["possessions"]

stints["FreeThrowAttemptRate"] = (100 * stints["freeThrowAttempts"] / stints["possessions"])

stints = stints.drop_duplicates()

stints_ORB = stints.copy(deep = True)
stints_EFG = stints.copy(deep = True)

stints_ORB["OffensiveReboundPercent"] = 100 * stints_ORB["offensiveRebounds"] / (stints_ORB["offensiveRebounds"] + stints_ORB["opponentDefensiveRebounds"])

stints_EFG["EffectiveFieldGoalPercent"] = 100 * (stints_EFG["fieldGoals"] + 0.5 * stints_EFG["threePtMade"]) / stints_EFG["fieldGoalAttempts"]

stints_ORB = stints_ORB.dropna()
stints_EFG = stints_EFG.dropna()

def extract_stints(stints, name):
    stints_for_reg = stints[["offensePlayer1Id", "offensePlayer2Id",
            "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
            "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
            "defensePlayer4Id", "defensePlayer5Id", name]]
    stints_x_base = stints_for_reg.as_matrix(columns=["offensePlayer1Id", "offensePlayer2Id",
                                                 "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
                                                 "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
                                                 "defensePlayer4Id", "defensePlayer5Id"])
    stint_X_rows = np.apply_along_axis(map_players, 1, stints_x_base)
    stint_Y_rows = stints_for_reg.as_matrix([name])
    return stint_X_rows, stint_Y_rows


stintX_adjusted, stintY_adjusted = extract_stints(stints, "ExpectedPointsPerPossession")
stintX_raw, stintY_raw = extract_stints(stints, "PointsPerPossession")

stintX_turnover, stintY_turnover = extract_stints(stints, "TurnoverPercent")
stintX_ftr, stintY_ftr = extract_stints(stints, "FreeThrowAttemptRate")
stintX_orbd, stintY_orbd = extract_stints(stints_ORB, "OffensiveReboundPercent")
stintX_efg, stintY_efg = extract_stints(stints_EFG, "EffectiveFieldGoalPercent")


def calculate_rapm(stintY, stintX, name):
    lambdas = [.01, .05, .1, .25, .5, .75]
    samples = stintX.shape[0]
    alphas = [l * samples / 2 for l in lambdas]
    
    clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True, normalize=False)
    weights = [secondsPlayedMap[p] / 60 for p in filtered_players]
    weights = np.array(weights)
    weights = np.concatenate((weights, weights))
    weights = weights - weights.mean()
    
    clf.coef_ = weights

    model = clf.fit(stintX, stintY)
    
    player_arr = np.transpose(np.array(filtered_players).reshape(1, len(filtered_players)))
    coef_offensive_array = np.transpose(model.coef_[:, 0:len(filtered_players)])
    coef_defensive_array = np.transpose(model.coef_[:, len(filtered_players):])

    player_id_with_coef = np.concatenate([player_arr, coef_offensive_array, coef_defensive_array], axis=1)
    players_coef = pd.DataFrame(player_id_with_coef)

    players_coef.columns = ["playerId", "{0} ORAPM".format(name), "{0} DRAPM".format(name)]

    print("\n\n")
    print(name)
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

    log_error = metrics.mean_squared_error(stintY, pred)
    print("log squared error: {}".format(log_error))

    print(players_coef.head(10))

    return players_coef


results_adjusted = calculate_rapm(stintY_adjusted, stintX_adjusted, "Luck Adjusted")
results_turnover = calculate_rapm(stintY_turnover, stintX_turnover, "Turnover Rate")
results_efg = calculate_rapm(stintY_efg, stintX_efg, "EFG")
results_orbd = calculate_rapm(stintY_orbd, stintX_orbd, "Offensive Rebound Rate")
results_ftr = calculate_rapm(stintY_ftr, stintX_ftr, "Free Throw Rate")

merged = results_adjusted\
    .merge(results_turnover, how="inner", on="playerId")\
    .merge(results_efg, how="inner", on="playerId")\
    .merge(results_orbd, how="inner", on="playerId")\
    .merge(results_ftr, how="inner", on="playerId")\
    .merge(player_names, how="inner", on="playerId")

print(merged.head(20))


merged.to_csv("results/Real Adjusted Four Factors {}.csv".format(season))





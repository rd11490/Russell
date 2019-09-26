import numpy as np
import pandas as pd

from cred import MySQLConnector
from ridge.RidgeWithPrior import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
seasons = ["2018-19"]
seasonType = "Regular Season"


def extract_stints(stints_for_extraction, name):
    stints_for_reg = stints_for_extraction[["offensePlayer1Id", "offensePlayer2Id",
                                            "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
                                            "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
                                            "defensePlayer4Id", "defensePlayer5Id", name]]
    stints_x_base = stints_for_reg.as_matrix(columns=["offensePlayer1Id", "offensePlayer2Id",
                                                      "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
                                                      "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
                                                      "defensePlayer4Id", "defensePlayer5Id"])
    stint_X_rows = np.apply_along_axis(map_players, 1, stints_x_base)
    stint_Y_rows = stints_for_reg.as_matrix([name])
    possessions = stints_for_extraction["possessions"].values
    return stint_X_rows, stint_Y_rows, possessions


def lambda_to_alpha(lambda_value, samples):
    return (lambda_value * samples) / 2.0


def alpha_to_lambda(alpha_value, samples):
    return (alpha_value * 2.0) / samples


def calculate_rmse(group):
    group["RMSE"] = ((group["Error"]) ** 2).mean() ** .5
    group["AvgError"] = abs(group["Error"]).mean()
    group["Count"] = len(group)
    return group


def calculate_rapm(x, y, prior, possessions, lambdas, name):
    model = ridge_with_prior(X=x, Y=y, prior=prior, weights=possessions, lmbdas=lambdas)

    player_arr = np.transpose(np.array(filtered_players).reshape(1, len(filtered_players)))
    coef = model.coef_ + prior
    coef_offensive_array = np.transpose(coef[:, 0:len(filtered_players)])
    coef_defensive_array = np.transpose(coef[:, len(filtered_players):])

    player_id_with_coef = np.concatenate([player_arr, coef_offensive_array, coef_defensive_array], axis=1)
    players_coef = pd.DataFrame(player_id_with_coef)
    intercept = model.intercept_

    players_coef.columns = ["playerId", "{0}__Off".format(name), "{0}__Def".format(name)]

    players_coef[name] = players_coef["{0}__Off".format(name)] + players_coef["{0}__Def".format(name)]

    players_coef["{0}_Rank".format(name)] = players_coef[name].rank(ascending=False)

    players_coef["{0}__Off_Rank".format(name)] = players_coef["{0}__Off".format(name)].rank(ascending=False)

    players_coef["{0}__Def_Rank".format(name)] = players_coef["{0}__Def".format(name)].rank(ascending=False)

    players_coef["{0}__intercept".format(name)] = intercept[0]

    print("\n\n")
    print(name)

    return players_coef, intercept


for season in seasons:

    [start, end] = season.split('-')
    prev_season = "{0}-{1}".format(int(start) - 1, int(end) - 1)

    # seconds_query = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}' and seasonType ='{}';".format(season, seasonType)
    stints_query = "SELECT * FROM nba.luck_adjusted_one_way_possessions where season = '{0}' and seasonType ='{1}' and possessions > 0;".format(
        season, seasonType)
    previous_season_ff = "SELECT * FROM nba.real_adjusted_four_factors where season = '{0}';".format(prev_season)
    player_names_query = "select playerId, playerName from nba.player_info;".format(season)

    stints = sql.runQuery(stints_query)
    previous_season = sql.runQuery(previous_season_ff)
    player_names = sql.runQuery(player_names_query).drop_duplicates()
    # secondsPlayed = sql.runQuery(seconds_query)
    # secondsPlayedMap = secondsPlayed.set_index("playerId").to_dict()["secondsPlayed"]

    players = list(
        set(list(stints["offensePlayer1Id"]) + list(stints["offensePlayer2Id"]) + list(stints["offensePlayer3Id"]) + \
            list(stints["offensePlayer4Id"]) + list(stints["offensePlayer5Id"]) + list(stints["defensePlayer1Id"]) + \
            list(stints["defensePlayer2Id"]) + list(stints["defensePlayer3Id"]) + list(stints["defensePlayer4Id"]) + \
            list(stints["defensePlayer5Id"])))
    players.sort()

    filtered_players = players  # [p for p in players if secondsPlayedMap[p] > shotCutOff]''


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


    def map_prior(prior_df):
        rowOut = np.zeros([len(filtered_players) * 2]) + -2
        for r in prior_df.iterrows():
            if r[1]['playerId'] in filtered_players:
                rowOut[filtered_players.index(r[1]['playerId'])] = r[1]['RAPM__Off']
                rowOut[filtered_players.index(r[1]['playerId']) + len(filtered_players)] = r[1]['RAPM__Def']
        return rowOut


    stints = stints[stints["possessions"] > 0]

    stints["PointsPerPossession"] = 100 * stints["points"] / stints["possessions"]

    stints = stints.drop_duplicates()

    prior = np.transpose(np.asmatrix(map_prior(previous_season)))

    stintX_raw, stintY_raw, possessions_raw = extract_stints(stints.copy(True), "PointsPerPossession")

    lambdas_rapm = [.01, .05, .1, .25, .5, .75]

    results_raw, raw_intercept = calculate_rapm(stintX_raw, stintY_raw, prior, possessions_raw, lambdas_rapm, "RAPM")

    merged = results_raw

    merged = np.round(merged, decimals=2)

    merged = merged.reindex_axis(sorted(merged.columns), axis=1)

    merged = player_names.merge(merged, how="inner", on="playerId")

    merged["season"] = season

    merged["primaryKey"] = merged["playerId"].astype(str) + "_" + merged["season"]

    print(merged)

    merged.to_csv("results/Real Adjusted Four Factors V4 {} .csv".format(season))

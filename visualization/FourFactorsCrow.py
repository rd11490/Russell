import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import RidgeCV

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
seasons = [["2016-17",  "2017-18", "2018-19"]]

teams_lst = ['1610612761', '1610612755', '1610612749', '1610612738', '1610612754', '1610612744', '1610612757', '1610612745', '1610612762', '1610612743']

for season in seasons:

    season_str = "('" + "', '".join(season) + "')"
    teams_str = "('" + "', '".join(teams_lst) + "')"
    print(season_str)
    # seconds_query = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}' and seasonType ='{}';".format(season, seasonType)
    stints_query = "SELECT * FROM nba.luck_adjusted_one_way_possessions where season in {0} and possessions > 0 and offenseTeamId1 in {1} and defenseTeamId2 in {1};".format(season_str, teams_str)
    player_names_query = "select playerId, playerName from nba.player_info;".format(season)

    print(stints_query)

    stints = sql.runQuery(stints_query)
    player_names = sql.runQuery(player_names_query).drop_duplicates()

    print(stints.shape)
    stints = stints[(stints['offenseTeamId1'].isin(teams_lst)) & (stints['defenseTeamId2'].isin(teams_lst)) & (stints['defenseTeamId2'] != stints['offenseTeamId1'])]
    print(stints.shape)

    # secondsPlayed = sql.runQuery(seconds_query)
    # secondsPlayedMap = secondsPlayed.set_index("playerId").to_dict()["secondsPlayed"]



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


    def calc_ftr(row):
        fga = row["fieldGoalAttempts"]
        fta = row["freeThrowAttempts"]

        if fta == 0.0:
            row["FreeThrowAttemptRate"] = 0.0
        elif fga == 0.0:
            row["FreeThrowAttemptRate"] = 100.0
        else:
            row["FreeThrowAttemptRate"] = 100.0 * fta / fga
        return row


    stints = stints[stints["possessions"] > 0]

    stints["PointsPerPossession"] = 100 * stints["points"].values / stints["possessions"].values

    stints["ExpectedPointsPerPossession"] = 100 * stints["expectedPoints"].values / stints["possessions"].values

    stints["TurnoverPercent"] = -100 * stints["turnovers"].values / stints["possessions"].values

    stints = stints.apply(calc_ftr, axis=1)

    stints = stints.drop_duplicates()

    stints_ORB = stints.copy(deep=True)
    stints_EFG = stints.copy(deep=True)

    stints_ORB["OffensiveReboundPercent"] = 100 * stints_ORB["offensiveRebounds"].values / (
        stints_ORB["offensiveRebounds"].values + stints_ORB["opponentDefensiveRebounds"].values)

    stints_EFG["EffectiveFieldGoalPercent"] = 100 * (stints_EFG["fieldGoals"].values + 0.5 * stints_EFG["threePtMade"].values) / \
                                              stints_EFG["fieldGoalAttempts"].values

    stints_ORB = stints_ORB.dropna()
    stints_EFG = stints_EFG.dropna()


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


    stintX_adjusted, stintY_adjusted, possessions_adjusted = extract_stints(stints.copy(True),
                                                                            "ExpectedPointsPerPossession")
    stintX_raw, stintY_raw, possessions_raw = extract_stints(stints.copy(True), "PointsPerPossession")

    stintX_turnover, stintY_turnover, possessions_turnover = extract_stints(stints.copy(True), "TurnoverPercent")
    stintX_ftr, stintY_ftr, possessions_ftr = extract_stints(stints.copy(True), "FreeThrowAttemptRate")
    stintX_orbd, stintY_orbd, possessions_orbd = extract_stints(stints_ORB.copy(True), "OffensiveReboundPercent")
    stintX_efg, stintY_efg, possessions_efg = extract_stints(stints_EFG.copy(True), "EffectiveFieldGoalPercent")


    def lambda_to_alpha(lambda_value, samples):
        return (lambda_value * samples) / 2.0


    def alpha_to_lambda(alpha_value, samples):
        return (alpha_value * 2.0) / samples


    def calculate_rmse(group):
        group["RMSE"] = ((group["Error"]) ** 2).mean() ** .5
        group["AvgError"] = abs(group["Error"]).mean()
        group["Count"] = len(group)
        return group


    def calculate_rapm(stintY, stintX, possessions, lambdas, name, raw_name, full_stint):
        alphas = [lambda_to_alpha(l, stintX.shape[0]) for l in lambdas]

        clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True, normalize=False)
        # weights = [secondsPlayedMap[p] / 60 for p in filtered_players]
        # weights = np.array(weights)
        # weights = np.concatenate((weights, weights))
        # weights = weights - weights.mean()
        #
        # clf.coef_ = weights

        model = clf.fit(stintX, stintY, sample_weight=possessions)

        player_arr = np.transpose(np.array(filtered_players).reshape(1, len(filtered_players)))
        coef_offensive_array = np.transpose(model.coef_[:, 0:len(filtered_players)])
        coef_defensive_array = np.transpose(model.coef_[:, len(filtered_players):])

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


    lambdas = [.01, .025, .05, .075, .1]
    lambdas_turnover = [.01, .02, .03, .04, .05]
    lambdas_efg = [.01, .02, .03, .04, .05]
    lambdas_ftr = lambdas * 2
    lambdas_rapm = [.01, .05, .1, .25, .5, .75]

    results_adjusted, adjusted_intercept = calculate_rapm(stintY_adjusted, stintX_adjusted, possessions_adjusted, lambdas_rapm,
                                      "LA_RAPM", "ExpectedPointsPerPossession", stints.copy(True))

    results_raw, raw_intercept = calculate_rapm(stintY_raw, stintX_raw, possessions_raw, lambdas_rapm, "RAPM",
                                 "PointsPerPossession",
                                 stints.copy(True))
    results_turnover, tov_intercept = calculate_rapm(stintY_turnover, stintX_turnover, possessions_turnover, lambdas_turnover,
                                      "RA_TOV", "TurnoverPercent", stints.copy(True))
    results_efg, efg_intercept = calculate_rapm(stintY_efg, stintX_efg, possessions_efg, lambdas_efg,
                                 "RA_EFG", "EffectiveFieldGoalPercent",
                                 stints_EFG.copy(True))
    results_orbd, orbd_intercept = calculate_rapm(stintY_orbd, stintX_orbd, possessions_orbd, lambdas,
                                  "RA_ORBD", "OffensiveReboundPercent", stints_ORB.copy(True))
    results_ftr, ftr_intercept = calculate_rapm(stintY_ftr, stintX_ftr, possessions_ftr, lambdas_ftr, "RA_FTR",
                                 "FreeThrowAttemptRate", stints.copy(True))

    merged = results_adjusted \
        .merge(results_raw, how="inner", on="playerId") \
        .merge(results_turnover, how="inner", on="playerId") \
        .merge(results_efg, how="inner", on="playerId") \
        .merge(results_orbd, how="inner", on="playerId") \
        .merge(results_ftr, how="inner", on="playerId")

    merged = np.round(merged, decimals=2)

    merged = merged.reindex_axis(sorted(merged.columns), axis=1)

    merged = player_names.merge(merged, how="inner", on="playerId")

    # merged["season"] = season
    #
    # merged["primaryKey"] = merged["playerId"].astype(str) + "_" + merged["season"]

    print(merged)

    # sql.truncate_table(MySqlDatabases.NBADatabase.real_adjusted_four_factors, MySqlDatabases.NBADatabase.NAME, "season = '{0}'".format(season))
    # sql.write(merged, MySqlDatabases.NBADatabase.real_adjusted_four_factors_v2, MySqlDatabases.NBADatabase.NAME)

    merged.to_csv("results/Real Adjusted Four Factors Crow.csv".format(season))

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
seasons = ["2018-19"]#["2009-10", "2010-11", "2011-12", "2012-13", "2013-14", "2014-15", "2015-16", "2016-17",  "2017-18", "2018-19"]
seasonType = "Regular Season"

player_names_query = "select playerId, playerName from nba.player_info;"
player_names = sql.runQuery(player_names_query).drop_duplicates()
player_map = player_names.set_index('playerId').to_dict()
print(player_map)

for season in seasons:

    stints_query = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{0}' and seasonType ='{1}' and possessions > 0;".format(season, seasonType)
    stints = sql.runQuery(stints_query)


    stints['o_lineup'] = stints["offensePlayer1Id"].astype(str) + '-' + stints["offensePlayer2Id"].astype(str) + '-'+ stints["offensePlayer3Id"].astype(str) + '-' + stints["offensePlayer4Id"].astype(str) + '-' +stints["offensePlayer5Id"].astype(str)
    stints['d_lineup'] = stints["defensePlayer1Id"].astype(str) + '-' + stints["defensePlayer2Id"].astype(str) + '-'+ stints["defensePlayer3Id"].astype(str) + '-' + stints["defensePlayer4Id"].astype(str) + '-' +stints["defensePlayer5Id"].astype(str)

    stints_poss_limit = stints[stints['possessions'] > 15]


    lineups = list(
        set(list(stints_poss_limit["o_lineup"]) + list(stints_poss_limit["d_lineup"]) + ['default']))
    lineups.sort()

    print(len(lineups))


    def map_players(row_in):
        l1 = row_in[0]
        l2 = row_in[1]

        rowOut = np.zeros([len(lineups) * 2])

        if l1 not in lineups:
            rowOut[lineups.index('default')] = 1
        else:
            rowOut[lineups.index(l1)] = 1

        if l2 not in lineups:
            rowOut[lineups.index('default') + len(lineups)] = -1
        else:
            rowOut[lineups.index(l2) + len(lineups)] = -1

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
        stints_for_reg = stints_for_extraction[["o_lineup", "d_lineup", name]]
        stints_x_base = stints_for_reg.as_matrix(columns=["o_lineup", "d_lineup"])
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

        player_arr = np.transpose(np.array(lineups).reshape(1, len(lineups)))
        coef_offensive_array = np.transpose(model.coef_[:, 0:len(lineups)])
        coef_defensive_array = np.transpose(model.coef_[:, len(lineups):])

        player_id_with_coef = np.concatenate([player_arr, coef_offensive_array, coef_defensive_array], axis=1)
        players_coef = pd.DataFrame(player_id_with_coef)
        intercept = model.intercept_

        players_coef.columns = ["lineupId", "{0}__Off".format(name), "{0}__Def".format(name)]

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

    merged = results_adjusted\
        .merge(results_raw, how="inner", on="lineupId") \
        .merge(results_turnover, how="inner", on="lineupId") \
        .merge(results_efg, how="inner", on="lineupId") \
        .merge(results_orbd, how="inner", on="lineupId") \
        .merge(results_ftr, how="inner", on="lineupId")

    merged = np.round(merged, decimals=2)

    merged = merged.reindex_axis(sorted(merged.columns), axis=1)

    # merged = player_names.merge(merged, how="inner", on="playerId")

    merged["season"] = season

    merged["primaryKey"] = merged["lineupId"].astype(str) + "_" + merged["season"]

    print(merged)

    # sql.truncate_table(MySqlDatabases.NBADatabase.real_adjusted_four_factors, MySqlDatabases.NBADatabase.NAME, "season = '{0}'".format(season))
    # sql.write(merged, MySqlDatabases.NBADatabase.real_adjusted_four_factors_v2, MySqlDatabases.NBADatabase.NAME)

    merged.to_csv("results/Real Adjusted Four Factors lineup {} .csv".format(season))

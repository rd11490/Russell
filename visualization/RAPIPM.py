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
seasons = ["2018-19"]
seasonType = "Regular Season"

for season in seasons:
    # seconds_query = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}' and seasonType ='{}';".format(season, seasonType)
    stints_query = "SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{0}' and seasonType ='{1}';".format(
        season, seasonType)
    player_names_query = "select playerId, playerName from nba.player_info;".format(season)

    stints = sql.runQuery(stints_query)
    player_names = sql.runQuery(player_names_query).drop_duplicates()
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


    stints = stints[stints["possessions"] > 0]

    stints["TSA"] = stints["fieldGoalAttempts"] + 0.44 * stints["freeThrowAttempts"]
    stints["intercept"] = 30.657
    stints["3PtFactor"] = 212.73 * (stints["threePtMade"] / stints["threePtAttempts"]) * (stints["threePtAttempts"] / stints["TSA"])
    stints["2pt%"] = 106 * (stints["fieldGoals"] - stints["threePtMade"]) / (stints["fieldGoalAttempts"] - stints["threePtAttempts"])
    stints["2PtRate"] = 64.3975 * (stints["fieldGoalAttempts"] - stints["threePtAttempts"]) / stints["TSA"]
    stints["FTFactor"] = 151.856 * (stints["freeThrowsMade"]/stints["freeThrowAttempts"]) * (stints["freeThrowAttempts"] / stints["TSA"])
    stints["ORBDFactor"] = 0.815 * (1 - (stints["fieldGoals"]/stints["fieldGoalAttempts"])) * (stints["offensiveRebounds"]/(stints["offensiveRebounds"]+stints["opponentDefensiveRebounds"]))
    stints["TO%"] = -1.177 * stints["turnovers"]/stints["possessions"]


    stints["L_ORTG"] =  stints["intercept"] + stints["3PtFactor"] + stints["2pt%"]  + stints["2PtRate"] + stints["FTFactor"] +  stints["ORBDFactor"] + stints["TO%"]

    stints = stints.dropna()


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


    stintX_lehigh, stintY_leigh, possessions_raw = extract_stints(stints.copy(True), "L_ORTG")


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
        print("r^2 value: {}".format(model.score(stintX, stintY)))
        print("Model Lambda: {0} -> {1}".format(model.alpha_, alpha_to_lambda(model.alpha_, stintX.shape[0])))
        print("Model Intercept: {0}".format(model.intercept_))

        pred = model.predict(stintX)
        err = stintY - pred
        print("METRICS:")

        print("max: {}".format(max(err)))
        print("min: {}".format(min(err)))

        abs_error = metrics.mean_absolute_error(stintY, pred)
        print("mean absolute error: {}".format(abs_error))

        rms_error = metrics.mean_squared_error(stintY, pred)
        print("mean squared error: {}".format(rms_error))

        log_error = metrics.mean_squared_error(stintY, pred)
        print("log squared error: {}".format(log_error))

        full_stint["Prediction"] = pred

        full_stint["Error"] = full_stint["Prediction"] - full_stint[raw_name]

        # print(players_coef.head(10))

        rmse = full_stint.groupby(by="possessions").apply(calculate_rmse)

        rmse_for_plot = rmse[["possessions", "Count", "RMSE", "AvgError"]].drop_duplicates().sort_values(
            by="possessions")

        # print(rmse_for_plot)

        fig, ax = plt.subplots()
        rmse_for_plot.plot.scatter(ax=ax, x="possessions", y="RMSE", color="Red", )
        rmse_for_plot.plot.scatter(ax=ax, x="possessions", y="AvgError", color="Blue")
        plt.legend(["RMSE", "AvgError"])
        plt.xlabel("Possessions")
        plt.ylabel("Error")
        plt.title("In-Sample Error for Regularized {0}".format(name))
        plt.ylim([0, 50])
        plt.savefig("results/Real Adjusted Four Factors - {0} Error - {1}.png".format(name, season))
        plt.close()

        return players_coef, intercept


    lambdas = [.01, .025, .05, .075, .1]
    lambdas_turnover = [.01, .02, .03, .04, .05]
    lambdas_efg = [.01, .02, .03, .04, .05]
    lambdas_ftr = lambdas * 2
    lambdas_rapm = [.01, .05, .1, .25, .5, .75]

    results_adjusted, adjusted_intercept = calculate_rapm(stintX_lehigh, stintY_leigh, possessions_raw,
                                                          lambdas_rapm,
                                                          "Lehigh_RAPM", "L_ORTG", stints.copy(True))

    merged = results_adjusted

    merged = np.round(merged, decimals=2)

    merged = merged.reindex_axis(sorted(merged.columns), axis=1)

    merged = player_names.merge(merged, how="inner", on="playerId")

    merged["season"] = season

    merged["primaryKey"] = merged["playerId"].astype(str) + "_" + merged["season"]

    print(merged.head(20))

    # sql.truncate_table(MySqlDatabases.NBADatabase.real_adjusted_four_factors, MySqlDatabases.NBADatabase.NAME,
    #                    "season={0}".format(season))
    # sql.write(merged, MySqlDatabases.NBADatabase.real_adjusted_four_factors, MySqlDatabases.NBADatabase.NAME)

    merged.to_csv("results/Lehigh RAPM {}.csv".format(season))

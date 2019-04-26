import numpy as np
import pandas as pd

import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#
#
# regress combo of 4F + RAPM + LA_RAPM to which team wins
# run MCMC
#


rookie_stats = {
    "LA_RAPM": -0.575833,
    "LA_RAPM__Def": -0.249615,
    "LA_RAPM__Off": -0.326538,
    "RAPM": -0.360577,
    "RAPM__Def": -0.198462,
    "RAPM__Off": -0.162436,
    "RA_EFG": -0.199936,
    "RA_EFG__Def": -0.119231,
    "RA_EFG__Off": -0.080192,
    "RA_FTR": -0.095256,
    "RA_FTR__Def": -0.135385,
    "RA_FTR__Off": 0.039808,
    "RA_ORBD": 0.090513,
    "RA_ORBD__Def": -0.042756,
    "RA_ORBD__Off": 0.133333,
    "RA_TOV": -0.109423,
    "RA_TOV__Def": -0.001859,
    "RA_TOV__Off": -0.107436
}

sql = MySQLConnector.MySQLConnector()


# seasons = [("2012-15", "2015-16")]


seasons = [("2012-15", "2015-16"), ("2013-16", "2016-17"), ("2014-17", "2017-18"), ("2015-18", "2018-19")]


def percentage_of_minutes(group):
    group["share"] = group["minutes"] / group["minutes"].sum()
    return group

def calculate_nets(group, base, offset):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = (group["{}__Off".format(base)] * group["share"]).sum()*5 + intercept - offset
    d_value = offset + intercept - (group["{}__Def".format(base)] * group["share"]).sum()*5
    full_value = (group[base] * group["share"]).sum()*5 - 2*offset
    return o_value, d_value, full_value

def calculate_nets_RBD(group, base):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = (group["{}__Off".format(base)] * group["share"]).sum()*5 + intercept
    d_value = (group["{}__Def".format(base)] * group["share"]).sum()*5 + (100-intercept)
    return o_value, d_value

def calculate_nets_TOV(group, base):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = -intercept - (group["{}__Off".format(base)] * group["share"]).sum()*5
    d_value = -intercept + (group["{}__Def".format(base)] * group["share"]).sum()*5
    return o_value, d_value

def calculate_net_rating(group):
    LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET = calculate_nets(group,"LA_RAPM", 2.43)
    RAPM_ORTG, RAPM_DRTG, RAPM_NET = calculate_nets(group,"RAPM", 1.75)

    RA_EFG_ORTG, RA_EFG_DRTG, NOT_USED = calculate_nets(group,"RA_EFG", 0)

    RA_TOV_ORTG, RA_TOV_DRTG = calculate_nets_TOV(group,"RA_TOV")

    RA_RBD_ORTG, RA_RBD_DRTG = calculate_nets_RBD(group,"RA_ORBD")

    RA_FTR_ORTG, RA_FTR_DRTG, NOT_USED = calculate_nets(group,"RA_FTR", 0)

    return pd.Series([LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET, RAPM_ORTG, RAPM_DRTG, RAPM_NET, RA_EFG_ORTG, RA_EFG_DRTG, RA_TOV_ORTG, RA_TOV_DRTG, RA_RBD_ORTG, RA_RBD_DRTG, RA_FTR_ORTG, RA_FTR_DRTG])

for (season_multi, season) in seasons:
    rapm = sql.runQuery("SELECT * FROM nba.real_adjusted_four_factors_multi where season = '{}'".format(season_multi)).drop(["playerName"], axis=1)

    rank_columns = [c for c in rapm.columns if "Rank" in c]
    rapm = rapm.drop(rank_columns, axis=1)
    boxscores = sql.runQuery("SELECT * FROM nba.raw_player_box_score_advanced where season = '{}'".format(season))

    minutes = boxscores[["teamId", "playerId", "playerName", "minutes"]].groupby(by=["teamId", "playerId", "playerName"],
                                                                   as_index=False).sum().groupby(by="teamId").apply(
        percentage_of_minutes).reset_index()

    merged = minutes.merge(rapm, on="playerId", how="left")

    rookies = merged[merged.isnull().any(axis=1)]

    for k in rookie_stats:
        rookies[k] = rookie_stats[k]

    merged = merged.dropna()
    merged = pd.concat([merged, rookies])

    merged["LA_RAPM__intercept"] = merged["LA_RAPM__intercept"][0]
    merged["RAPM__intercept"] = merged["LA_RAPM__intercept"][0]
    merged["RA_EFG__intercept"] = merged["RA_EFG__intercept"][0]
    merged["RA_FTR__intercept"] = merged["RA_FTR__intercept"][0]
    merged["RA_ORBD__intercept"] = merged["RA_ORBD__intercept"][0]
    merged["RA_TOV__intercept"] = merged["RA_TOV__intercept"][0]

    teams = merged.groupby(by="teamId").apply(calculate_net_rating)
    teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG", "RA_EFG_O", "RA_EFG_D",
                     "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

    teams = teams.reset_index()

    print(teams)
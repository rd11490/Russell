import numpy as np
import pandas as pd

import MySqlDatabases.NBADatabase
from cred import MySQLConnector
import pickle

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingClassifier

from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import normalize
from scipy import stats

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
np.set_printoptions(threshold=1000)

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


def calculate_nets(group, base, offset_o=0.0, offset_d=0.0):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = (group["{}__Off".format(base)] * group["share"]).sum() * 5 + intercept + offset_o
    d_value = offset_d + intercept - (group["{}__Def".format(base)] * group["share"]).sum() * 5
    full_value = ((group[base] * group["share"]).sum() * 5) + offset_o - offset_d
    return o_value, d_value, full_value


def calculate_nets_RBD(group, base):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = (group["{}__Off".format(base)] * group["share"]).sum() * 5 + intercept
    d_value = (group["{}__Def".format(base)] * group["share"]).sum() * 5 + (100 - intercept)
    return o_value, d_value


def calculate_nets_TOV(group, base):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = -intercept - (group["{}__Off".format(base)] * group["share"]).sum() * 5
    d_value = -intercept + (group["{}__Def".format(base)] * group["share"]).sum() * 5
    return o_value, d_value


def calculate_net_rating(group):
    LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET = calculate_nets(group, "LA_RAPM")
    RAPM_ORTG, RAPM_DRTG, RAPM_NET = calculate_nets(group, "RAPM")

    RA_EFG_ORTG, RA_EFG_DRTG, NOT_USED = calculate_nets(group, "RA_EFG")

    RA_TOV_ORTG, RA_TOV_DRTG = calculate_nets_TOV(group, "RA_TOV")

    RA_RBD_ORTG, RA_RBD_DRTG = calculate_nets_RBD(group, "RA_ORBD")

    RA_FTR_ORTG, RA_FTR_DRTG, NOT_USED = calculate_nets(group, "RA_FTR")

    return pd.Series(
        [LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET, RAPM_ORTG, RAPM_DRTG, RAPM_NET, RA_EFG_ORTG, RA_EFG_DRTG, RA_TOV_ORTG,
         RA_TOV_DRTG, RA_RBD_ORTG, RA_RBD_DRTG, RA_FTR_ORTG, RA_FTR_DRTG])


inputs = []
player_seasons = []
for (season_multi, season) in seasons:
    rapm = sql.runQuery(
        "SELECT * FROM nba.real_adjusted_four_factors_multi where season = '{}'".format(season_multi)).drop(
        ["playerName"], axis=1)

    rank_columns = [c for c in rapm.columns if "Rank" in c]
    rapm = rapm.drop(rank_columns, axis=1)
    boxscores = sql.runQuery("SELECT * FROM nba.raw_player_box_score_advanced where season = '{}'".format(season))

    minutes = boxscores[["teamId", "playerId", "playerName", "minutes"]].groupby(
        by=["teamId", "playerId", "playerName"],
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


    player_seasons.append(merged)

    teams = merged.groupby(by="teamId").apply(calculate_net_rating)
    teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG",
                     "RA_EFG_O", "RA_EFG_D",
                     "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

    teams = teams.reset_index()

    LA_AVG = teams['LA_RAPM_O_RTG'].mean() - teams['LA_RAPM_D_RTG'].mean()
    RA_AVG = teams['RAPM_O_RTG'].mean() - teams['RAPM_D_RTG'].mean()

    if LA_AVG > 0.0:
        teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] - (LA_AVG / 2)
        teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] + (LA_AVG / 2)
    else:
        teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] + (LA_AVG / 2)
        teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] - (LA_AVG / 2)

    if RA_AVG > 0.0:
        teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] - (RA_AVG / 2)
        teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] + (RA_AVG / 2)

    else:
        teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] + (RA_AVG / 2)
        teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] - (RA_AVG / 2)

    teams['LA_RAPM_NET_RTG'] = teams['LA_RAPM_O_RTG'] - teams['LA_RAPM_D_RTG']
    teams['RAPM_NET_RTG'] = teams['RAPM_O_RTG'] - teams['RAPM_D_RTG']

    results = sql.runQuery("SELECT * FROM nba.league_results where season = '{}'".format(season))

    joined = results.merge(teams, left_on=['homeTeam'], right_on=['teamId'], suffixes=('', '_home')).merge(teams,
                                                                                                           left_on=[
                                                                                                               'awayTeam'],
                                                                                                           right_on=[
                                                                                                               'teamId'],
                                                                                                           suffixes=(
                                                                                                               '_home',
                                                                                                               '_away'))

    inputs.append(joined)


pd.concat(player_seasons).to_csv('player_season.csv', index=False)
training_data = pd.concat(inputs)

training_data['LA_RAPM_O_RTG_diff'] = training_data['LA_RAPM_O_RTG_home'] - training_data['LA_RAPM_D_RTG_away']
training_data['LA_RAPM_D_RTG_diff'] = training_data['LA_RAPM_D_RTG_home'] - training_data['LA_RAPM_O_RTG_away']

training_data['RAPM_O_RTG_diff'] = training_data['RAPM_O_RTG_home'] - training_data['RAPM_D_RTG_away']
training_data['RAPM_D_RTG_diff'] = training_data['RAPM_D_RTG_home'] - training_data['RAPM_O_RTG_away']

training_data['RA_EFG_O_diff'] = training_data['RA_EFG_O_home'] - training_data['RA_EFG_D_away']
training_data['RA_EFG_D_diff'] = training_data['RA_EFG_D_home'] - training_data['RA_EFG_O_away']

training_data['RA_TOV_O_diff'] = training_data['RA_TOV_O_home'] - training_data['RA_TOV_D_away']
training_data['RA_TOV_D_diff'] = training_data['RA_TOV_D_home'] - training_data['RA_TOV_O_away']

training_data['RA_ORBD_O_diff'] = training_data['RA_ORBD_O_home'] - training_data['RA_ORBD_D_away']
training_data['RA_ORBD_D_diff'] = training_data['RA_ORBD_D_home'] - training_data['RA_ORBD_O_away']

training_data['RA_FTR_O_diff'] = training_data['RA_FTR_O_home'] - training_data['RA_FTR_D_away']
training_data['RA_FTR_D_diff'] = training_data['RA_FTR_D_home'] - training_data['RA_FTR_O_away']

train = training_data[training_data['season'].isin(['2015-16', '2016-17', '2017-18'])]

"""
train_cols = ['LA_RAPM_O_RTG_home', 'LA_RAPM_D_RTG_home', 'RAPM_O_RTG_home', 'RAPM_D_RTG_home', 'RA_EFG_O_home', 'RA_EFG_D_home', 'RA_TOV_O_home', 'RA_TOV_D_home', 'RA_ORBD_O_home',
           'RA_ORBD_D_home', 'RA_FTR_O_home', 'RA_FTR_D_home','LA_RAPM_O_RTG_away', 'LA_RAPM_D_RTG_away', 'RAPM_O_RTG_away', 'RAPM_D_RTG_away',  'RA_EFG_O_away', 'RA_EFG_D_away',
           'RA_TOV_O_away', 'RA_TOV_D_away', 'RA_ORBD_O_away', 'RA_ORBD_D_away', 'RA_FTR_O_away',
           'RA_FTR_D_away']

           ,'RA_EFG_O_home', 'RA_EFG_D_home', 'RA_TOV_O_home', 'RA_TOV_D_home','RA_ORBD_O_home','RA_ORBD_D_home', 'RA_FTR_O_home', 'RA_FTR_D_home','RA_EFG_O_away', 'RA_EFG_D_away', 'RA_TOV_O_away', 'RA_TOV_D_away','RA_ORBD_O_away', 'RA_ORBD_D_away', 'RA_FTR_O_away','RA_FTR_D_away'
"""

train_cols = [
    'LA_RAPM_O_RTG_home', 'LA_RAPM_D_RTG_home',
    # 'RAPM_O_RTG_home', 'RAPM_D_RTG_home',
    'RA_EFG_O_home', 'RA_EFG_D_home',
    'RA_TOV_O_home', 'RA_TOV_D_home',
    'RA_ORBD_O_home', 'RA_ORBD_D_home',
    'RA_FTR_O_home', 'RA_FTR_D_home',
    'LA_RAPM_O_RTG_away', 'LA_RAPM_D_RTG_away',
    # 'RAPM_O_RTG_away', 'RAPM_D_RTG_away',
    'RA_EFG_O_away', 'RA_EFG_D_away',
    'RA_TOV_O_away', 'RA_TOV_D_away',
    'RA_ORBD_O_away', 'RA_ORBD_D_away',
    'RA_FTR_O_away', 'RA_FTR_D_away'
]

Y = train['homeWin'].values
X = train[train_cols].values
# X = stats.zscore(X, axis=0)

# clf = RandomForestClassifier(n_estimators=50, min_samples_split=7, max_depth=18)
clf = KNeighborsClassifier(n_neighbors=20)
# clf = GradientBoostingClassifier()

clf.fit(X, Y)

test = training_data[training_data['season'].isin(['2018-19'])]

Y_test = test['homeWin']
X_test = test[train_cols]

pred = clf.score(X_test, Y_test)
print(pred)
probs = clf.predict_proba(X_test)

print(probs)
test['predict'] = probs[:, 0]

test.to_csv('predicted_test.csv', index=False)

season_multi = '2016-19'
rapm = sql.runQuery(
    "SELECT * FROM nba.real_adjusted_four_factors_multi where season = '{}'".format(season_multi)).drop(
    ["playerName"], axis=1)

rank_columns = [c for c in rapm.columns if "Rank" in c]
rapm = rapm.drop(rank_columns, axis=1)

minutes = pd.read_csv('clean_minutes.csv', index_col='index')

minutes = minutes[["teamId", "Player", 'playerId', "minutes"]].fillna(-1).groupby(
    by=["teamId", "Player", 'playerId'],
    as_index=False).sum().groupby(by="teamId").apply(
    percentage_of_minutes).reset_index()


merged = minutes.merge(rapm, on="playerId", how="left")

print(merged[merged['teamId'] == 1610612738])
print(merged[merged.isnull().any(axis=1)].head(30))
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

merged.to_csv('player_season_16-19.csv')

teams = merged.groupby(by="teamId").apply(calculate_net_rating)
teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG",
                 "RA_EFG_O", "RA_EFG_D",
                 "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

teams = teams.reset_index()

LA_AVG = teams['LA_RAPM_O_RTG'].mean() - teams['LA_RAPM_D_RTG'].mean()
RA_AVG = teams['RAPM_O_RTG'].mean() - teams['RAPM_D_RTG'].mean()

if LA_AVG > 0.0:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] - (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] + (LA_AVG / 2)
else:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] + (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] - (LA_AVG / 2)

if RA_AVG > 0.0:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] - (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] + (RA_AVG / 2)

else:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] + (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] - (RA_AVG / 2)

teams['LA_RAPM_NET_RTG'] = teams['LA_RAPM_O_RTG'] - teams['LA_RAPM_D_RTG']
teams['RAPM_NET_RTG'] = teams['RAPM_O_RTG'] - teams['RAPM_D_RTG']

teams.to_csv('team_projections_19_20.csv', index=False)

schedule = pd.read_csv('schedule.csv')

schedule_input = schedule.merge(teams, left_on=['homeTeam'], right_on=['teamId'], suffixes=('', '_home')).merge(teams,
                                                                                                                left_on=[
                                                                                                                    'awayTeam'],
                                                                                                                right_on=[
                                                                                                                    'teamId'],
                                                                                                                suffixes=(
                                                                                                                    '_home',
                                                                                                                    '_away'))

schedule_input['LA_RAPM_O_RTG_diff'] = schedule_input['LA_RAPM_O_RTG_home'] - schedule_input['LA_RAPM_D_RTG_away']
schedule_input['LA_RAPM_D_RTG_diff'] = schedule_input['LA_RAPM_D_RTG_home'] - schedule_input['LA_RAPM_O_RTG_away']

schedule_input['RAPM_O_RTG_diff'] = schedule_input['RAPM_O_RTG_home'] - schedule_input['RAPM_D_RTG_away']
schedule_input['RAPM_D_RTG_diff'] = schedule_input['RAPM_D_RTG_home'] - schedule_input['RAPM_O_RTG_away']

schedule_input['RA_EFG_O_diff'] = schedule_input['RA_EFG_O_home'] - schedule_input['RA_EFG_D_away']
schedule_input['RA_EFG_D_diff'] = schedule_input['RA_EFG_D_home'] - schedule_input['RA_EFG_O_away']

schedule_input['RA_TOV_O_diff'] = schedule_input['RA_TOV_O_home'] - schedule_input['RA_TOV_D_away']
schedule_input['RA_TOV_D_diff'] = schedule_input['RA_TOV_D_home'] - schedule_input['RA_TOV_O_away']

schedule_input['RA_ORBD_O_diff'] = schedule_input['RA_ORBD_O_home'] - schedule_input['RA_ORBD_D_away']
schedule_input['RA_ORBD_D_diff'] = schedule_input['RA_ORBD_D_home'] - schedule_input['RA_ORBD_O_away']

schedule_input['RA_FTR_O_diff'] = schedule_input['RA_FTR_O_home'] - schedule_input['RA_FTR_D_away']
schedule_input['RA_FTR_D_diff'] = schedule_input['RA_FTR_D_home'] - schedule_input['RA_FTR_O_away']

X_pred = schedule_input[train_cols]

predicted = clf.predict_proba(X_pred)
schedule_input['predict'] = predicted[:, 0]

schedule_input.to_csv('predicted.csv', index=False)

#####
##
####

season_multi = '2018-19'
rapm = sql.runQuery(
    "SELECT * FROM nba.real_adjusted_four_factors where season = '{}'".format(season_multi)).drop(
    ["playerName"], axis=1)

rank_columns = [c for c in rapm.columns if "Rank" in c]
rapm = rapm.drop(rank_columns, axis=1)

minutes = pd.read_csv('clean_minutes.csv', index_col='index')

minutes = minutes[["teamId", "Player", 'playerId', "minutes"]].fillna(-1).groupby(
    by=["teamId", "Player", 'playerId'],
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
teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG",
                 "RA_EFG_O", "RA_EFG_D",
                 "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

teams = teams.reset_index()

LA_AVG = teams['LA_RAPM_O_RTG'].mean() - teams['LA_RAPM_D_RTG'].mean()
RA_AVG = teams['RAPM_O_RTG'].mean() - teams['RAPM_D_RTG'].mean()

if LA_AVG > 0.0:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] - (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] + (LA_AVG / 2)
else:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] + (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] - (LA_AVG / 2)

if RA_AVG > 0.0:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] - (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] + (RA_AVG / 2)

else:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] + (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] - (RA_AVG / 2)

teams['LA_RAPM_NET_RTG'] = teams['LA_RAPM_O_RTG'] - teams['LA_RAPM_D_RTG']
teams['RAPM_NET_RTG'] = teams['RAPM_O_RTG'] - teams['RAPM_D_RTG']

teams.to_csv('team_projections_single_yr_19_20.csv', index=False)

schedule = pd.read_csv('schedule.csv')

schedule_input = schedule.merge(teams, left_on=['homeTeam'], right_on=['teamId'], suffixes=('', '_home')).merge(teams,
                                                                                                                left_on=[
                                                                                                                    'awayTeam'],
                                                                                                                right_on=[
                                                                                                                    'teamId'],
                                                                                                                suffixes=(
                                                                                                                    '_home',
                                                                                                                    '_away'))

schedule_input['LA_RAPM_O_RTG_diff'] = schedule_input['LA_RAPM_O_RTG_home'] - schedule_input['LA_RAPM_D_RTG_away']
schedule_input['LA_RAPM_D_RTG_diff'] = schedule_input['LA_RAPM_D_RTG_home'] - schedule_input['LA_RAPM_O_RTG_away']

schedule_input['RAPM_O_RTG_diff'] = schedule_input['RAPM_O_RTG_home'] - schedule_input['RAPM_D_RTG_away']
schedule_input['RAPM_D_RTG_diff'] = schedule_input['RAPM_D_RTG_home'] - schedule_input['RAPM_O_RTG_away']

schedule_input['RA_EFG_O_diff'] = schedule_input['RA_EFG_O_home'] - schedule_input['RA_EFG_D_away']
schedule_input['RA_EFG_D_diff'] = schedule_input['RA_EFG_D_home'] - schedule_input['RA_EFG_O_away']

schedule_input['RA_TOV_O_diff'] = schedule_input['RA_TOV_O_home'] - schedule_input['RA_TOV_D_away']
schedule_input['RA_TOV_D_diff'] = schedule_input['RA_TOV_D_home'] - schedule_input['RA_TOV_O_away']

schedule_input['RA_ORBD_O_diff'] = schedule_input['RA_ORBD_O_home'] - schedule_input['RA_ORBD_D_away']
schedule_input['RA_ORBD_D_diff'] = schedule_input['RA_ORBD_D_home'] - schedule_input['RA_ORBD_O_away']

schedule_input['RA_FTR_O_diff'] = schedule_input['RA_FTR_O_home'] - schedule_input['RA_FTR_D_away']
schedule_input['RA_FTR_D_diff'] = schedule_input['RA_FTR_D_home'] - schedule_input['RA_FTR_O_away']

X_pred = schedule_input[train_cols]

predicted = clf.predict_proba(X_pred)
schedule_input['predict'] = predicted[:, 0]

schedule_input.to_csv('predicted_single_yr.csv', index=False)


#####
##
####

season_multi = '2017-18'
rapm = sql.runQuery(
    "SELECT * FROM nba.real_adjusted_four_factors where season = '{}'".format(season_multi)).drop(
    ["playerName"], axis=1)

rank_columns = [c for c in rapm.columns if "Rank" in c]
rapm = rapm.drop(rank_columns, axis=1)

boxscores = sql.runQuery("SELECT * FROM nba.raw_player_box_score_advanced where season = '{}'".format(season))

minutes = boxscores[["teamId", "playerId", "playerName", "minutes"]].groupby(
    by=["teamId", "playerId", "playerName"],
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
teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG",
                 "RA_EFG_O", "RA_EFG_D",
                 "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

teams = teams.reset_index()

LA_AVG = teams['LA_RAPM_O_RTG'].mean() - teams['LA_RAPM_D_RTG'].mean()
RA_AVG = teams['RAPM_O_RTG'].mean() - teams['RAPM_D_RTG'].mean()

if LA_AVG > 0.0:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] - (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] + (LA_AVG / 2)
else:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] + (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] - (LA_AVG / 2)

if RA_AVG > 0.0:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] - (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] + (RA_AVG / 2)

else:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] + (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] - (RA_AVG / 2)

teams['LA_RAPM_NET_RTG'] = teams['LA_RAPM_O_RTG'] - teams['LA_RAPM_D_RTG']
teams['RAPM_NET_RTG'] = teams['RAPM_O_RTG'] - teams['RAPM_D_RTG']

teams.to_csv('team_projections_single_yr.csv', index=False)

schedule = pd.read_csv('schedule_18.csv')

schedule_input = schedule.merge(teams, left_on=['homeTeam'], right_on=['teamId'], suffixes=('', '_home')).merge(teams,
                                                                                                                left_on=[
                                                                                                                    'awayTeam'],
                                                                                                                right_on=[
                                                                                                                    'teamId'],
                                                                                                                suffixes=(
                                                                                                                    '_home',
                                                                                                                    '_away'))

schedule_input['LA_RAPM_O_RTG_diff'] = schedule_input['LA_RAPM_O_RTG_home'] - schedule_input['LA_RAPM_D_RTG_away']
schedule_input['LA_RAPM_D_RTG_diff'] = schedule_input['LA_RAPM_D_RTG_home'] - schedule_input['LA_RAPM_O_RTG_away']

schedule_input['RAPM_O_RTG_diff'] = schedule_input['RAPM_O_RTG_home'] - schedule_input['RAPM_D_RTG_away']
schedule_input['RAPM_D_RTG_diff'] = schedule_input['RAPM_D_RTG_home'] - schedule_input['RAPM_O_RTG_away']

schedule_input['RA_EFG_O_diff'] = schedule_input['RA_EFG_O_home'] - schedule_input['RA_EFG_D_away']
schedule_input['RA_EFG_D_diff'] = schedule_input['RA_EFG_D_home'] - schedule_input['RA_EFG_O_away']

schedule_input['RA_TOV_O_diff'] = schedule_input['RA_TOV_O_home'] - schedule_input['RA_TOV_D_away']
schedule_input['RA_TOV_D_diff'] = schedule_input['RA_TOV_D_home'] - schedule_input['RA_TOV_O_away']

schedule_input['RA_ORBD_O_diff'] = schedule_input['RA_ORBD_O_home'] - schedule_input['RA_ORBD_D_away']
schedule_input['RA_ORBD_D_diff'] = schedule_input['RA_ORBD_D_home'] - schedule_input['RA_ORBD_O_away']

schedule_input['RA_FTR_O_diff'] = schedule_input['RA_FTR_O_home'] - schedule_input['RA_FTR_D_away']
schedule_input['RA_FTR_D_diff'] = schedule_input['RA_FTR_D_home'] - schedule_input['RA_FTR_O_away']

X_pred = schedule_input[train_cols]

predicted = clf.predict_proba(X_pred)
schedule_input['predict'] = predicted[:, 0]

schedule_input.to_csv('predicted_test_single_yr.csv', index=False)


####
###
####


season_multi = '2015-18'
rapm = sql.runQuery(
    "SELECT * FROM nba.real_adjusted_four_factors_multi where season = '{}'".format(season_multi)).drop(
    ["playerName"], axis=1)

rank_columns = [c for c in rapm.columns if "Rank" in c]
rapm = rapm.drop(rank_columns, axis=1)

minutes = pd.read_csv('clean_minutes_18-19.csv')

minutes = minutes[["teamId", "playerId", "playerName", "minutes"]].groupby(
    by=["teamId", "playerId", "playerName"],
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
teams.columns = ["LA_RAPM_O_RTG", "LA_RAPM_D_RTG", "LA_RAPM_NET_RTG", "RAPM_O_RTG", "RAPM_D_RTG", "RAPM_NET_RTG",
                 "RA_EFG_O", "RA_EFG_D",
                 "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]

teams = teams.reset_index()

LA_AVG = teams['LA_RAPM_O_RTG'].mean() - teams['LA_RAPM_D_RTG'].mean()
RA_AVG = teams['RAPM_O_RTG'].mean() - teams['RAPM_D_RTG'].mean()

if LA_AVG > 0.0:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] - (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] + (LA_AVG / 2)
else:
    teams['LA_RAPM_O_RTG'] = teams['LA_RAPM_O_RTG'] + (LA_AVG / 2)
    teams['LA_RAPM_D_RTG'] = teams['LA_RAPM_D_RTG'] - (LA_AVG / 2)

if RA_AVG > 0.0:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] - (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] + (RA_AVG / 2)

else:
    teams['RAPM_O_RTG'] = teams['RAPM_O_RTG'] + (RA_AVG / 2)
    teams['RAPM_D_RTG'] = teams['RAPM_D_RTG'] - (RA_AVG / 2)

teams['LA_RAPM_NET_RTG'] = teams['LA_RAPM_O_RTG'] - teams['LA_RAPM_D_RTG']
teams['RAPM_NET_RTG'] = teams['RAPM_O_RTG'] - teams['RAPM_D_RTG']

teams.to_csv('team_projections_18_19.csv', index=False)

schedule = pd.read_csv('schedule_18.csv')

schedule_input = schedule.merge(teams, left_on=['homeTeam'], right_on=['teamId'], suffixes=('', '_home')).merge(teams,
                                                                                                                left_on=[
                                                                                                                    'awayTeam'],
                                                                                                                right_on=[
                                                                                                                    'teamId'],
                                                                                                                suffixes=(
                                                                                                                    '_home',
                                                                                                                    '_away'))

schedule_input['LA_RAPM_O_RTG_diff'] = schedule_input['LA_RAPM_O_RTG_home'] - schedule_input['LA_RAPM_D_RTG_away']
schedule_input['LA_RAPM_D_RTG_diff'] = schedule_input['LA_RAPM_D_RTG_home'] - schedule_input['LA_RAPM_O_RTG_away']

schedule_input['RAPM_O_RTG_diff'] = schedule_input['RAPM_O_RTG_home'] - schedule_input['RAPM_D_RTG_away']
schedule_input['RAPM_D_RTG_diff'] = schedule_input['RAPM_D_RTG_home'] - schedule_input['RAPM_O_RTG_away']

schedule_input['RA_EFG_O_diff'] = schedule_input['RA_EFG_O_home'] - schedule_input['RA_EFG_D_away']
schedule_input['RA_EFG_D_diff'] = schedule_input['RA_EFG_D_home'] - schedule_input['RA_EFG_O_away']

schedule_input['RA_TOV_O_diff'] = schedule_input['RA_TOV_O_home'] - schedule_input['RA_TOV_D_away']
schedule_input['RA_TOV_D_diff'] = schedule_input['RA_TOV_D_home'] - schedule_input['RA_TOV_O_away']

schedule_input['RA_ORBD_O_diff'] = schedule_input['RA_ORBD_O_home'] - schedule_input['RA_ORBD_D_away']
schedule_input['RA_ORBD_D_diff'] = schedule_input['RA_ORBD_D_home'] - schedule_input['RA_ORBD_O_away']

schedule_input['RA_FTR_O_diff'] = schedule_input['RA_FTR_O_home'] - schedule_input['RA_FTR_D_away']
schedule_input['RA_FTR_D_diff'] = schedule_input['RA_FTR_D_home'] - schedule_input['RA_FTR_O_away']

X_pred = schedule_input[train_cols]

predicted = clf.predict_proba(X_pred)
schedule_input['predict'] = predicted[:, 0]

schedule_input.to_csv('predicted_18-19.csv', index=False)

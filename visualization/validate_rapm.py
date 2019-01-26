import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

season = "2018-19"
seasonType = "Regular Season"

##
# Select RAPM
##
rapm = sql.runQuery("SELECT * FROM nba.real_adjusted_four_factors where season = '{}'".format(season))

##
# Calculate Minutes per player per team
##
def percentage_of_minutes(group):
    group["share"] = group["minutes"]/group["minutes"].sum()
    return group

def calculate_nets(group, base):
    intercept = group.iloc[0]["{}__intercept".format(base)]
    o_value = (group["{}__Off".format(base)] * group["share"]).sum()*5 + intercept
    d_value = (group["{}__Def".format(base)] * group["share"]).sum()*5 + intercept
    full_value = (group[base] * group["share"]).sum()*5
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
    team_id = group.iloc[0]["teamId"]
    LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET = calculate_nets(group,"LA_RAPM")
    RAPM_ORTG, RAPM_DRTG, RAPM_NET = calculate_nets(group,"RAPM")

    RA_EFG_ORTG, RA_EFG_DRTG, NOT_USED = calculate_nets(group,"RA_EFG")

    RA_TOV_ORTG, RA_TOV_DRTG = calculate_nets_TOV(group,"RA_TOV")

    RA_RBD_ORTG, RA_RBD_DRTG = calculate_nets_RBD(group,"RA_ORBD")

    RA_FTR_ORTG, RA_FTR_DRTG, NOT_USED = calculate_nets(group,"RA_FTR")

    return pd.Series([LA_RAPM_ORTG, LA_RAPM_DRTG, LA_RAPM_NET, RAPM_ORTG, RAPM_DRTG, RAPM_NET, RA_EFG_ORTG, RA_EFG_DRTG, RA_TOV_ORTG, RA_TOV_DRTG, RA_RBD_ORTG, RA_RBD_DRTG, RA_FTR_ORTG, RA_FTR_DRTG])


boxscores = sql.runQuery("SELECT * FROM nba.raw_player_box_score_advanced where season = '{}'".format(season))

minutes = boxscores[["teamId", "playerId", "minutes"]].groupby(by=["teamId", "playerId"], as_index=False).sum().groupby(by="teamId").apply(percentage_of_minutes).reset_index()

merged = minutes.merge(rapm, on="playerId", how="left").dropna()
teams = merged.groupby(by="teamId").apply(calculate_net_rating)
teams.columns = ["LA_RAPM_O", "LA_RAPM_D", "LA_RAPM", "RAPM_O", "RAPM_D", "RAPM", "RA_EFG_O", "RA_EFG_D", "RA_TOV_O", "RA_TOV_D", "RA_ORBD_O", "RA_ORBD_D", "RA_FTR_O", "RA_FTR_D"]


##
# Select Team Stats
##

def calculate_offensive_team_stats(group):
    points = group["points"].sum()
    possessions = group["possessions"].sum()
    time = group["seconds"].sum()
    o_rtg = round(points/possessions,3)*100

    return pd.Series([points, possessions, time, o_rtg])

def calculate_defensive_team_stats(group):
    points = group["points"].sum()
    possessions = group["possessions"].sum()
    time = group["seconds"].sum()
    o_rtg = round(points/possessions,3)*100


    return pd.Series([points, possessions, time, o_rtg])



stats = sql.runQuery("SELECT * FROM nba.league_net_rtg where season = '{}'".format(season))

stints = sql.runQuery("SELECT * FROM nba.luck_adjusted_one_way_stints where season = '{}' and seasonType ='{}';".format(season, seasonType))
# teams = sql.runQuery("select * from nba.team_info where season = '{0}'".format(season))


offensive_stats = stints.groupby(by="offenseTeamId1").apply(calculate_offensive_team_stats).reset_index()
offensive_stats.columns = ["teamId", "points", "possessions", "seconds", "ORTG"]

print(offensive_stats.sort_values(by="ORTG").head(30))


defensive_stats = stints.groupby(by="defenseTeamId2").apply(calculate_defensive_team_stats).reset_index()
defensive_stats.columns = ["teamId", "points", "possessions", "seconds", "DRTG"]

print(defensive_stats.sort_values(by="DRTG").head(30))
joined = teams.merge(stats, left_on="teamId", right_on="TEAM_ID")

joined["LARAPM_ERROR"] = abs(joined["LA_RAPM"] - joined["NET_RATING"])
joined["LRAPM_O_ERROR"] = abs(joined["LA_RAPM_O"] - joined["OFF_RATING"])
joined["LRAPM_D_ERROR"] = abs(joined["LA_RAPM_D"] - joined["DEF_RATING"])

joined["RAPM_ERROR"] = abs(joined["RAPM"] - joined["NET_RATING"])
joined["RAPM_O_ERROR"] = abs(joined["RAPM_O"] - joined["OFF_RATING"])
joined["RAPM_D_ERROR"] = abs(joined["RAPM_D"] - joined["DEF_RATING"])

joined["EFG_O_ERROR"] = abs(joined["RA_EFG_O"] - joined["EFG_PCT"]*100.0)

joined["RA_TOV_O_ERROR"] = abs(joined["RA_TOV_O"] - joined["TM_TOV_PCT"]*100.0)

joined["OREB_PCT_ERROR"] = abs(joined["RA_ORBD_O"] - joined["OREB_PCT"]*100.0)
joined["DREB_PCT_ERROR"] = abs(joined["RA_ORBD_D"] - joined["DREB_PCT"]*100.0)

errors = joined[["TEAM_NAME", "LARAPM_ERROR", "LRAPM_O_ERROR", "LRAPM_D_ERROR", "RAPM_ERROR", "RAPM_O_ERROR", "RAPM_D_ERROR", "EFG_O_ERROR", "RA_TOV_O_ERROR", "OREB_PCT_ERROR", "DREB_PCT_ERROR"]]
print(joined)

print(errors.describe())

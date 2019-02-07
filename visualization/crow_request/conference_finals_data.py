import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

stats_columns = ["points", "defensiveRebounds", "offensiveRebounds", "opponentDefensiveRebounds",
                 "opponentOffensiveRebounds", "turnovers", "fieldGoalAttempts", "fieldGoals", "threePtAttempts",
                 "threePtMade", "freeThrowAttempts", "freeThrowsMade", "possessions", "seconds"]

stat_columns_with_o = ["{0}_offense".format(c) for c in stats_columns]
stat_columns_with_d = ["{0}_defense".format(c) for c in stats_columns]

neutral_columns = ["player1Id", "player2Id", "player3Id", "player4Id", "player5Id", "opponentStarters"]

player_names = pd.read_csv("player_names.csv")[["playerId", "playerName"]]

#######
# Get Time Played in entire playoffs
#######
full_playoffs_data = pd.read_csv("possessions_with_num_starters.csv")
full_playoffs_data = full_playoffs_data[
    (full_playoffs_data["gameId"] == 41700303) & (
        (full_playoffs_data["offenseTeamId"] == 1610612744) | (full_playoffs_data["offenseTeamId"] == 1610612739) |
        (full_playoffs_data["offenseTeamId"] == 1610612738) | (full_playoffs_data["offenseTeamId"] == 1610612745) |
        (full_playoffs_data["defenseTeamId"] == 1610612744) | (full_playoffs_data["defenseTeamId"] == 1610612739) |
        (full_playoffs_data["defenseTeamId"] == 1610612738) | (full_playoffs_data["defenseTeamId"] == 1610612745))]


playoffs_O = full_playoffs_data[["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id",
                                 "offensePlayer5Id"] + stats_columns].groupby(
    by=["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id",
        "offensePlayer5Id"]).sum().reset_index()
playoffs_O.columns = ["player1Id", "player2Id", "player3Id", "player4Id", "player5Id"] + stats_columns
playoffs_D = full_playoffs_data[["defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id",
                                 "defensePlayer5Id"] + stats_columns].groupby(
    by=["defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id",
        "defensePlayer5Id"]).sum().reset_index()
playoffs_D.columns = ["player1Id", "player2Id", "player3Id", "player4Id", "player5Id"] + stats_columns

joined_playoffs = playoffs_O.merge(playoffs_D, on=["player1Id", "player2Id", "player3Id", "player4Id", "player5Id"],
                                   suffixes=('_offense', '_defense'))

joined_playoffs_with_name = joined_playoffs \
    .merge(player_names, left_on="player1Id", right_on="playerId", suffixes=("", "1")) \
    .merge(player_names, left_on="player2Id", right_on="playerId", suffixes=("", "2")) \
    .merge(player_names, left_on="player3Id", right_on="playerId", suffixes=("", "3")) \
    .merge(player_names, left_on="player4Id", right_on="playerId", suffixes=("", "4")) \
    .merge(player_names, left_on="player5Id", right_on="playerId", suffixes=("", "5"))

joined_playoffs_with_name = joined_playoffs_with_name[
    ["playerName", "playerName2", "playerName3", "playerName4",
     "playerName5"] + stat_columns_with_o + stat_columns_with_d]
joined_playoffs_with_name["total_seconds"] = joined_playoffs_with_name["seconds_offense"] + joined_playoffs_with_name[
    "seconds_defense"]
joined_playoffs_with_name["net_points"] = joined_playoffs_with_name["points_offense"] - joined_playoffs_with_name[
    "points_defense"]
joined_playoffs_with_name = joined_playoffs_with_name.sort_values(by="total_seconds", ascending=False)
total_lineup_time = joined_playoffs_with_name[
    ["playerName", "playerName2", "playerName3", "playerName4", "playerName5", "total_seconds"]].groupby(
    by=["playerName", "playerName2", "playerName3", "playerName4", "playerName5"]).sum().reset_index().sort_values(
    by="total_seconds", ascending=False)

filtered_lineups = total_lineup_time[total_lineup_time["total_seconds"] > 0]
filtered_lineups["keep"] = True
filtered_lineups = filtered_lineups[["playerName", "playerName2", "playerName3", "playerName4", "playerName5", "keep"]]


#######
# Calculate Conference Finals Stats
#######

def group_Dstarters(row):
    if row["defenseTeamStarters"] > 3:
        row["defenseTeamStarters"] = "4 to 5"
    elif row["defenseTeamStarters"] == 3:
        row["defenseTeamStarters"] = "3 starters"
    else:
        row["defenseTeamStarters"] = "0 to 2"
    return row


def group_Ostarters(row):
    if row["offenseTeamStarters"] > 3:
        row["offenseTeamStarters"] = "4 to 5"
    elif row["offenseTeamStarters"] == 3:
        row["offenseTeamStarters"] = "3 starters"
    else:
        row["offenseTeamStarters"] = "0 to 2"
    return row


o_columns = ["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
             "defenseTeamStarters"]
d_columns = ["defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id", "defensePlayer5Id",
             "offenseTeamStarters"]

data = pd.read_csv("possessions_with_num_starters.csv")
data = data[
    (data["gameId"] == 41700303) & (
    ((data["offenseTeamId"] == 1610612744) & (data["defenseTeamId"] == 1610612745)) |
    ((data["offenseTeamId"] == 1610612745) & (data["defenseTeamId"] == 1610612744)) |
    ((data["offenseTeamId"] == 1610612738) & (data["defenseTeamId"] == 1610612739)) |
    ((data["offenseTeamId"] == 1610612739) & (data["defenseTeamId"] == 1610612738)))]
data = data.apply(group_Dstarters, axis=1).apply(group_Ostarters, axis=1)

grouped_O = data[o_columns + stats_columns].groupby(by=o_columns).sum().reset_index()
grouped_O.columns = neutral_columns + stats_columns
grouped_D = data[d_columns + stats_columns].groupby(by=d_columns).sum().reset_index()
grouped_D.columns = neutral_columns + stats_columns

joined = grouped_O.merge(grouped_D, on=neutral_columns, suffixes=('_offense', '_defense'))

joined_with_name = joined \
    .merge(player_names, left_on="player1Id", right_on="playerId", suffixes=("", "1")) \
    .merge(player_names, left_on="player2Id", right_on="playerId", suffixes=("", "2")) \
    .merge(player_names, left_on="player3Id", right_on="playerId", suffixes=("", "3")) \
    .merge(player_names, left_on="player4Id", right_on="playerId", suffixes=("", "4")) \
    .merge(player_names, left_on="player5Id", right_on="playerId", suffixes=("", "5"))
joined_with_name = joined_with_name[
    ["playerName", "playerName2", "playerName3", "playerName4", "playerName5",
     "opponentStarters"] + stat_columns_with_o + stat_columns_with_d]
joined_with_name["total_seconds"] = joined_with_name["seconds_offense"] + joined_with_name["seconds_defense"]
joined_with_name["net_points"] = joined_with_name["points_offense"] - joined_with_name["points_defense"]

joined_with_name = joined_with_name.sort_values(by="total_seconds", ascending=False)
joined_with_name = joined_with_name.merge(filtered_lineups,
                                          on=["playerName", "playerName2", "playerName3", "playerName4", "playerName5"])
joined_with_name = joined_with_name[joined_with_name["keep"]].sort_values(by="net_points", ascending=False)
print(joined_with_name)
# joined_with_name.to_csv("conference_finals_linups.csv", index=False)

import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

stats_columns = ["defensiveRebounds", "offensiveRebounds", "opponentDefensiveRebounds", "opponentOffensiveRebounds", "turnovers", "fieldGoalAttempts", "fieldGoals", "threePtAttempts", "threePtMade", "freeThrowAttempts", "freeThrowsMade", "possessions", "seconds"]

o_columns = ["offensePlayer1Id","offensePlayer2Id","offensePlayer3Id","offensePlayer4Id","offensePlayer5Id","offenseTeamStarters","defenseTeamStarters"]
d_columns = ["defensePlayer1Id","defensePlayer2Id","defensePlayer3Id","defensePlayer4Id","defensePlayer5Id","offenseTeamStarters", "defenseTeamStarters"]

stat_columns_with_o = ["{0}_offense".format(c) for c in stats_columns]
stat_columns_with_d = ["{0}_defense".format(c) for c in stats_columns]


neutral_columns = ["player1Id","player2Id","player3Id","player4Id","player5Id","starters","opponentStarters"]

data = pd.read_csv("possessions_with_num_starters_conf_finals.csv")

grouped_O = data[o_columns + stats_columns].groupby(by=o_columns).sum().reset_index()
grouped_O.columns = neutral_columns + stats_columns
grouped_D = data[d_columns + stats_columns].groupby(by=d_columns).sum().reset_index()
grouped_D.columns = neutral_columns + stats_columns

player_names = pd.read_csv("player_names.csv")[["playerId", "playerName"]]

joined = grouped_O.merge(grouped_D, on=neutral_columns, suffixes=('_offense', '_defense'))

joined_with_name = joined\
    .merge(player_names, left_on="player1Id", right_on="playerId", suffixes=("", "1"))\
    .merge(player_names, left_on="player2Id", right_on="playerId", suffixes=("", "2"))\
    .merge(player_names, left_on="player3Id", right_on="playerId", suffixes=("", "3"))\
    .merge(player_names, left_on="player4Id", right_on="playerId", suffixes=("", "4"))\
    .merge(player_names, left_on="player5Id", right_on="playerId", suffixes=("", "5"))

joined_with_name = joined_with_name[["playerName", "playerName2", "playerName3", "playerName4", "playerName5","starters", "opponentStarters"] + stat_columns_with_o + stat_columns_with_d]
joined_with_name["total_seconds"] = joined_with_name["seconds_offense"] + joined_with_name["seconds_defense"]
joined_with_name=joined_with_name.sort_values(by="total_seconds", ascending=False)

joined_with_name.to_csv("conference_finals_linups.csv")
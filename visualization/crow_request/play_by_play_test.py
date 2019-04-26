import pandas as pd
import datetime

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

stats_columns = ["points", "defensiveRebounds", "offensiveRebounds", "opponentDefensiveRebounds",
                 "opponentOffensiveRebounds", "turnovers", "fieldGoalAttempts", "fieldGoals", "threePtAttempts",
                 "threePtMade", "freeThrowAttempts", "freeThrowsMade", "possessions", "seconds"]

stat_columns_with_o = ["{0}_offense".format(c) for c in stats_columns]
stat_columns_with_d = ["{0}_defense".format(c) for c in stats_columns]

# neutral_columns = ["player1Id", "player2Id", "player3Id", "player4Id", "player5Id"]
# neutral_columns = ["teamId", "period"]

#######
# Calculate Conference Finals Stats
#######

# o_columns = ["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id"]
# d_columns = ["defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id", "defensePlayer5Id"]

o_columns = ["offenseTeamId", "possessions"]
d_columns = ["defenseTeamId", "possessions"]
player_names = pd.read_csv("player_names.csv")[["playerId", "playerName"]]

data = pd.read_csv("possessions_with_num_starters.csv")
data = data[data["gameId"] == 41700305]


def split_df(df, col, prefix):
    out = df[[col, "seconds", "period", "possessions"]]
    out.columns = ["playerId", "{}_seconds".format(prefix), "period", "{}_possessions".format(prefix)]
    return out


def to_time(row):
    row["seconds"] = row["o_seconds"]
    row["time"] = str(datetime.timedelta(seconds=row["seconds"]))
    return row


O1 = split_df(data, "offensePlayer1Id", "o")
O2 = split_df(data, "offensePlayer2Id", "o")
O3 = split_df(data, "offensePlayer3Id", "o")
O4 = split_df(data, "offensePlayer4Id", "o")
O5 = split_df(data, "offensePlayer5Id", "o")

D1 = split_df(data, "defensePlayer1Id", "d")
D2 = split_df(data, "defensePlayer2Id", "d")
D3 = split_df(data, "defensePlayer3Id", "d")
D4 = split_df(data, "defensePlayer4Id", "d")
D5 = split_df(data, "defensePlayer5Id", "d")

players = pd.concat([O1, O2, O3, O4, O5, D1, D2, D3, D4, D5], axis=0).groupby(by=["playerId", "period"]).sum().reset_index()

named = players.merge(player_names, on="playerId").sort_values(by=["period", "playerId"], ascending=True)
named = named.apply(to_time, axis=1)

print(named)
print("\n\n")


o_Data = data[o_columns]
d_Data = data[d_columns]

o_Data.columns = ["teamId", "O_possessions"]
d_Data.columns = ["teamId", "D_possessions"]

o_Data = o_Data.groupby(by="teamId").sum().reset_index()
d_Data = d_Data.groupby(by="teamId").sum().reset_index()



summed = o_Data.merge(d_Data, on="teamId")

print(summed)




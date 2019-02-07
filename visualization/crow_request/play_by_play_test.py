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

# o_columns = ["offenseTeamId", "period"]
# d_columns = ["defenseTeamId", "period"]
player_names = pd.read_csv("player_names.csv")[["playerId", "playerName"]]

data = pd.read_csv("possessions_with_num_starters.csv")
data = data[data["gameId"] == 41700303]


def split_df(df, col):
    out = df[[col, "seconds"]]
    out.columns = ["playerId", "seconds"]
    return out


def to_time(row):
    row["time"] = str(datetime.timedelta(seconds=row["seconds"]))
    return row


O1 = split_df(data, "offensePlayer1Id")
O2 = split_df(data, "offensePlayer2Id")
O3 = split_df(data, "offensePlayer3Id")
O4 = split_df(data, "offensePlayer4Id")
O5 = split_df(data, "offensePlayer5Id")

D1 = split_df(data, "defensePlayer1Id")
D2 = split_df(data, "defensePlayer2Id")
D3 = split_df(data, "defensePlayer3Id")
D4 = split_df(data, "defensePlayer4Id")
D5 = split_df(data, "defensePlayer5Id")

players = pd.concat([O1, O2, O3, O4, O5, D1, D2, D3, D4, D5], axis=0).groupby(by="playerId").sum().reset_index()
players["seconds"] = players["seconds"] / 2.0

named = players.merge(player_names, on="playerId").sort_values(by="seconds", ascending=False)
named = named.apply(to_time, axis=1)

total_seconds = named["seconds"].sum()
print(named)
print(total_seconds)

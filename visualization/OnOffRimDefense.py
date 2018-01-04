import MySQLConnector

season = "2017-18"

sql = MySQLConnector.MySQLConnector()

shot_frequency = "shotFrequency"
attempts = "attempts"


def split_on_off(df):
    player_on_df = df[df["onOff"] == "On"]
    player_off_df = df[df["onOff"] == "Off"]
    return player_on_df, player_off_df


def calculate_shot_frequency(df):
    df = df.groupby(by="playerName").apply(calc_frequency)
    return df


def calc_frequency(df):
    df[shot_frequency] = 100 * df[attempts] / df[attempts].sum()
    return df


def combine_on_off(on, off):
    comb = on[["playerName", "bin", "attempts", shot_frequency]].merge(off[["playerName", "bin", "attempts", shot_frequency]],
                                                           on=["playerName", "bin"], suffixes=["_on", "_off"],
                                                           how='outer')
    return comb

d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_player_on_off_zoned " \
          "WHERE season = '{0}') a " \
          "left join  (SELECT playerId, playerName FROM nba.roster_player " \
          "WHERE season = '{0}') b " \
          "on (a.id = b.playerId)".format(season)

shot_zones_D = sql.runQuery(d_query)

shot_zones_d_on, shot_zones_d_off = split_on_off(shot_zones_D)

shot_zones_d_on = calculate_shot_frequency(shot_zones_d_on)
shot_zones_d_off = calculate_shot_frequency(shot_zones_d_off)

shot_zones_d_comb = combine_on_off(shot_zones_d_on, shot_zones_d_off)

rim_d = shot_zones_d_comb[shot_zones_d_comb["bin"] == "RestrictedArea"]
rim_d = rim_d[rim_d["attempts_on"] > 250]

rim_d["Deterrence"] = rim_d["{}_on".format(shot_frequency)] - rim_d["{}_off".format(shot_frequency)]
rim_d = rim_d.sort_values(by="Deterrence")
rim_d = rim_d[["playerName", "shotFrequency_on", "shotFrequency_off", "Deterrence"]]

print("Top 20 Players by Rim Attempt Deterrence")
print(rim_d.head(20))
print()
print("Bottom 20 Players by Rim Attempt Deterrence")
print(rim_d.tail(20))

rim_d = rim_d.sort_values(by="shotFrequency_on", ascending=True)
print("Top 20 Players by Rim Attempt %")
print(rim_d.head(20))
print()
print("Bottom 20 Players by Rim Attempt %")
print(rim_d.tail(20))
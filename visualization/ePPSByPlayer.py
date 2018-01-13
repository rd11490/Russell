import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"
shotCutoff = 250

o_query = "SELECT * FROM (select * from nba.offense_expected_points_by_player " \
          "WHERE season = '{0}' and attempts > {1} and bin = 'Total') a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season, shotCutoff)

d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_player " \
          "WHERE season = '{0}' and attempts > {1} and bin = 'Total') a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player " \
          "WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season, shotCutoff)

o = sql.runQuery(o_query)
d = sql.runQuery(d_query)


def diffAndSort(df, ascending=True):
    df["ePPS-PPS"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["playerName", "ePPS-PPS", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by='ePPS-PPS', ascending=ascending)
    df.columns = ["playerName", "ePPS-PPS", "PPS", "expectedPPS"]
    return df


d = diffAndSort(d)
d.to_csv("results/DPPSByPlayer201718Filtered.csv")
print("Defense: Points Per Shot Allowed While on Court")
print(d.head(15))
print()
print("Defense: Points Per Shot Allowed While on Court")
print(d.tail(15))
print()
print()
print()

o = diffAndSort(o, False)
o.to_csv("results/OPPSByPlayer201718Filtered.csv")
print("Offense: Points Per Shot Scored While on Court")
print(o.head(15))
print()
print("Offense: Points Per Shot Scored While on Court")
print(o.tail(15))

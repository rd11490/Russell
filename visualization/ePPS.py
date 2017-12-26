import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"

o_query = "SELECT * FROM (select * from nba.offense_expected_points_total where season = '{}' ) a " \
          "left join  (select * from nba.team_info) b " \
          "on (a.teamId = b.teamId)".format(season)
d_query = "SELECT * FROM (select * from nba.defense_expected_points_total where season = '{}' ) a " \
          "left join  (select * from nba.team_info) b " \
          "on (a.teamId = b.teamId)".format(season)

o = sql.runQuery(o_query)
d = sql.runQuery(d_query)


def diffAndSort(df, ascending=True):
    df["ePPS-PPS"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "ePPS-PPS", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by='ePPS-PPS', ascending=ascending)
    df.columns = ["teamName", "ePPS-PPS", "PPS", "expectedPPS"]
    print(df)


print("Defense: Points Per Shot Allowed")
diffAndSort(d)

print()
print()
print()

print("Offense: Points Per Shot Scored")
diffAndSort(o, False)

import numpy as np

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2018-19"
season_type = "Playoffs"


o_query = "SELECT * FROM (select * from nba.offense_expected_points where season = '{0}' and bin = 'Total' and seasontype = '{1}' ) a " \
          "left join  (select * from nba.team_info where season = '{0}') b " \
          "on (a.teamId = b.teamId)".format(season, season_type)
d_query = "SELECT * FROM (select * from nba.defense_expected_points where season = '{0}' and bin = 'Total'  and seasontype = '{1}' ) a " \
          "left join  (select * from nba.team_info where season = '{0}') b " \
          "on (a.teamId = b.teamId)".format(season, season_type)

o = sql.runQuery(o_query)
d = sql.runQuery(d_query)

def diffAndSort(df, field='ePPS-PPS', ascending=True):
    df["ePPS-PPS"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "ePPS-PPS", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by=field, ascending=ascending)
    df.columns = ["teamName", "ePPS-PPS", "PPS", "expectedPPS"]
    print(df)

d = np.round(d, decimals=3)
o = np.round(o, decimals=3)




print("Defense: Points Per Shot Allowed")
diffAndSort(d)
print()
print("\n\n")
print("Defense: Points Per Shot Allowed")
print("Expected Points")
diffAndSort(d, 'expectedPointsAvg')
print("\n\n")
print("Defense: Points Per Shot Allowed")
print("Points")
diffAndSort(d, 'pointsAvg')



print()
print()
print()

print("Offense: Points Per Shot Scored")
diffAndSort(o, ascending=False)
print()
print("\n\n")
print("Offense: Points Per Shot Scored")
print("Expected Points")
diffAndSort(o, 'expectedPointsAvg', ascending=False)
print("\n\n")
print("Offense: Points Per Shot Scored")
print("Points")
diffAndSort(o, 'pointsAvg', ascending=False)





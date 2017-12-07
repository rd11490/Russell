import pandas as pd


season = "2016-17"
seasonFname = season[2:4]+season[5:7]
d = pd.read_csv("data/ePPS/D{}.csv".format(seasonFname))
o = pd.read_csv("data/ePPS/O{}.csv".format(seasonFname))


def diffAndSort(df, ascending = True):
    df["diff"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "diff", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by='diff', ascending=ascending)
    df.columns = ["teamName", "diff", "PPS", "expectedPPS"]
    print(df)


print("Defense: Points Per Shot Allowed")
print(season)
diffAndSort(d)

print()
print()
print()

print("Offense: Points Per Shot Scored")
print(season)
diffAndSort(o, False)
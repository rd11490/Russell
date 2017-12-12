import pandas as pd


d = pd.read_csv("data/ePPS/D1718ByPlayer.csv")
o = pd.read_csv("data/ePPS/O1718ByPlayer.csv")


def diffAndSort(df, ascending = True):
    df["diff"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["playerName", "diff", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by='diff', ascending=ascending)
    df.columns = ["playerName", "diff", "PPS", "expectedPPS"]
    return df


d = diffAndSort(d)
d.to_csv("data/DPPSByPlayer201718")
print("Defense: Points Per Shot Allowed While on Court")
print(d.head(15))
print()
print("Defense: Points Per Shot Allowed While on Court")
print(d.tail(15))
print()
print()
print()

o = diffAndSort(o, False)
o.to_csv("data/OPPSByPlayer201718")
print("Offense: Points Per Shot Scored While on Court")
print(o.head(15))
print()
print("Offense: Points Per Shot Scored While on Court")
print(o.tail(15))


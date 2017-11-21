import pandas as pd


d = pd.read_csv("data/ePointsPerShotDefense.csv")
o = pd.read_csv("data/ePointsPerShotOffense.csv")


def diffAndSort(df, ascending = True):
    df["diff"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "diff", "pointsAvg", "expectedPointsAvg"]]
    df = df.sort_values(by='diff', ascending=ascending)
    df.columns = ["teamName", "diff", "PPS", "expectedPPS"]
    print(df)


d = pd.read_csv("data/ePointsPerShotDefense.csv")
o = pd.read_csv("data/ePointsPerShotOffense.csv")


print("Defense: Points Per Shot Allowed")
diffAndSort(d)

print()
print()
print()

print("Offense: Points Per Shot Scored")
diffAndSort(o, False)
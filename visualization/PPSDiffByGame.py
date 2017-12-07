import pandas as pd
import matplotlib.pyplot as plt


d = pd.read_csv("data/ePPS/D1718ByGame.csv")
o = pd.read_csv("data/ePPS/O1718ByGame.csv")


def diff(df):
    df["expectedPointsAvg"] *= 100
    df["pointsAvg"] *= 100
    df["diff"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "games", "diff", "pointsAvg", "expectedPointsAvg"]]
    df.columns = ["teamName", "games", "ePPS100-PPS100", "PPS100", "ePPS100"]
    return df

def filterAndSort(df, teamName):
    dfnew = df[df["teamName"] == teamName]
    dfnew = dfnew.sort_values(by='games')
    return dfnew

def plot(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)
    print(dTeam)
    print()
    print(oTeam)

    plt.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="r", label="Defense ePPS100 - PPS100")
    plt.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="g", label="Offense ePPS100 - PPS100")

def plotBG(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)
    plt.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)

dnew = diff(d)
onew = diff(o)

teams = list(set(dnew["teamName"]))


for t in teams:
    for t2 in teams:
        plotBG(dnew, onew, t2)
    plot(dnew, onew, t)
    plt.ylim(-30, 30)
    plt.xlabel("Games Played")
    plt.ylabel("ePPS100 - PPS100")
    plt.legend()
    plt.title("Difference in ePPS100 and PPS100:\n {}".format(t))
    plt.savefig("plots/PPS100Diff/{}".format(t))
    plt.close()
    #plt.show()



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


def plotePPS(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["ePPS100"], c="r", label="Defense")
    plt.plot(oTeam["games"], oTeam["ePPS100"], c="g", label="Offense")


def plotBGePPS(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["ePPS100"], c="gray", label='_nolegend_', alpha=0.25)
    plt.plot(oTeam["games"], oTeam["ePPS100"], c="gray", label='_nolegend_', alpha=0.25)


def plotPPS(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["PPS100"], c="r", label="Defense")
    plt.plot(oTeam["games"], oTeam["PPS100"], c="g", label="Offense")


def plotBGPPS(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["PPS100"], c="gray", label='_nolegend_', alpha=0.25)
    plt.plot(oTeam["games"], oTeam["PPS100"], c="gray", label='_nolegend_', alpha=0.25)

def plotDiff(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="r", label="Defense")
    plt.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="g", label="Offense")

def plotBGDiff(d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    plt.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)
    plt.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)

dnew = diff(d)
onew = diff(o)

teams = list(set(dnew["teamName"]))


for t in teams:
    print(t)
    for t2 in teams:
        plt.subplot(3, 1, 1)
        plotBGPPS(dnew, onew, t2)
        plt.subplot(3, 1, 2)
        plotBGePPS(dnew, onew, t2)
        plt.subplot(3, 1, 3)
        plotBGDiff(dnew, onew, t2)
    plt.subplot(3, 1, 1)
    plotPPS(dnew, onew, t)
    plt.ylim(80, 125)
    plt.xlabel("Games Played")
    plt.ylabel("PPS100")
    plt.legend()
    plt.title("{}".format(t))

    plt.subplot(3, 1, 2)
    plotePPS(dnew, onew, t)
    plt.ylim(80, 125)
    plt.xlabel("Games Played")
    plt.ylabel("ePPS100")
    plt.legend()

    plt.subplot(3, 1, 3)
    plotDiff(dnew, onew, t)
    plt.ylim(-25, 25)
    plt.xlabel("Games Played")
    plt.ylabel("ePPS100 - PPS100")
    plt.legend()

    plt.savefig("plots/PPS100Comb/{}".format(t), dpi=900, figsize=(14, 6))
    plt.close()
    #plt.show()



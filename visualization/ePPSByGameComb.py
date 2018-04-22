import matplotlib.pyplot as plt
import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season = "2017-18"

o_query = "SELECT * FROM (select * from nba.offense_expected_points_by_game where season = '{}' and bin = 'Total' ) a " \
          "left join  (select * from nba.team_info) b " \
          "on (a.teamId = b.teamId)".format(season)
d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_game where season = '{}' and bin = 'Total' ) a " \
          "left join  (select * from nba.team_info) b " \
          "on (a.teamId = b.teamId)".format(season)

o = sql.runQuery(o_query)
d = sql.runQuery(d_query)


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


def plotePPS(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ax.plot(dTeam["games"], dTeam["ePPS100"], c="r", label="Defense")
    ax.plot(oTeam["games"], oTeam["ePPS100"], c="g", label="Offense")


def plotBGePPS(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ax.plot(dTeam["games"], dTeam["ePPS100"], c="gray", label='_nolegend_', alpha=0.25)
    ax.plot(oTeam["games"], oTeam["ePPS100"], c="gray", label='_nolegend_', alpha=0.25)


def plotPPS(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ax.plot(dTeam["games"], dTeam["PPS100"], c="r", label="Defense")
    ax.plot(oTeam["games"], oTeam["PPS100"], c="g", label="Offense")


def plotBGPPS(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ax.plot(dTeam["games"], dTeam["PPS100"], c="gray", label='_nolegend_', alpha=0.25)
    ax.plot(oTeam["games"], oTeam["PPS100"], c="gray", label='_nolegend_', alpha=0.25)

def plotDiff(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ldinner = ax.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="r", label="Defense")
    loinner = ax.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="g", label="Offense")
    return ldinner, loinner

def plotBGDiff(ax, d, o, teamName):
    dTeam = filterAndSort(d, teamName)
    oTeam = filterAndSort(o, teamName)

    ax.plot(dTeam["games"], dTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)
    ax.plot(oTeam["games"], oTeam["ePPS100-PPS100"], c="gray", label='_nolegend_', alpha=0.25)

dnew = diff(d)
onew = diff(o)

teams = list(set(dnew["teamName"]))


for t in teams:
    print(t)
    fig = plt.figure(figsize=(10, 8))
    ax1 = fig.add_subplot(3, 1, 1)
    ax2 = fig.add_subplot(3, 1, 2)
    ax3 = fig.add_subplot(3, 1, 3)

    for t2 in teams:
        plotBGPPS(ax1, dnew, onew, t2)
        plotBGePPS(ax2, dnew, onew, t2)
        plotBGDiff(ax3, dnew, onew, t2)
    plotPPS(ax1, dnew, onew, t)
    ax1.set_ylim(80, 125)
    ax1.set_ylabel("PPS100")
    ax1.set_title("{}".format(t))

    plotePPS(ax2, dnew, onew, t)
    ax2.set_ylim(80, 125)
    ax2.set_ylabel("ePPS100")

    ld, lo = plotDiff(ax3, dnew, onew, t)
    ax3.set_ylim (-25, 25)
    ax3.set_xlabel("Games Played")
    ax3.set_ylabel("ePPS100 - PPS100")
    fig.legend([ld[0], lo[0]], ["Defense", "Offense"], loc="upper right", ncol=2)
    fig.tight_layout()
    plt.savefig("plots/PPSComb/{0}/{1}".format(season,t))
    plt.close()
    #plt.show()



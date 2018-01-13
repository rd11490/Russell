import matplotlib.colors as colors

import MySQLConnector
import drawCourt
import matplotlib.pyplot as plt
import ShotZones

shotZones = ShotZones.buildShotZones()
valueForPlotting = "Diff"
player = "Shabazz Muhammad"
season = "2017-18"

o_query = "SELECT * FROM (select * from nba.offense_expected_points_by_player " \
          "WHERE season = '{0}' and bin != 'Total' ) a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season)

d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_player " \
          "WHERE season = '{0}' and bin != 'Total') a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player " \
          "WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season)

sql = MySQLConnector.MySQLConnector()

shotZonesO = sql.runQuery(o_query)
shotZonesD = sql.runQuery(d_query)

shotZonesD[valueForPlotting] = shotZonesD["expectedPointsAvg"] - shotZonesD["pointsAvg"]
shotZonesO[valueForPlotting] = shotZonesO["pointsAvg"] - shotZonesO["expectedPointsAvg"]

maxVal = 1.5  # max(max(shotZonesO[valueForPlotting]), max(shotZonesD[valueForPlotting]))
minVal = -1.5  # min(min(shotZonesO[valueForPlotting]), min(shotZonesD[valueForPlotting]))

norm = colors.Normalize(vmin=minVal, vmax=maxVal)

cls = plt.cm.get_cmap('RdYlGn')

sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
sm._A = []
teamNames = [player] #set(shotZonesD["playerName"])
for name in teamNames:
    print(name)

    font = {'family': 'serif',
            'color': 'black',
            'weight': 'bold',
            'size': 6,
            'ha': 'center',
            'va': 'center'}

    # OFFENSE
    plt.rcParams["figure.figsize"] = [16, 6]
    # plt.colorbar( mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])

    plt.subplot(1, 2, 1)
    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)
    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')
    shotZonesDataTeamO = shotZonesO[shotZonesO["playerName"] == name]
    plt.title("Offensive Points Per Shot While On Court:\n {}".format(name))
    plt.tight_layout()

    for r in shotZonesDataTeamO.index:
        row = shotZonesDataTeamO.loc[r,]
        bin = row["bin"]
        plotVal = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])

        if (bin == "Right27FT" or bin == "Left27FT"):
            yAvg = yAvg + 20
        elif (bin == "Right23FT" or bin == "Left23FT"):
            yAvg = yAvg + 10
        elif (bin == "RightLong3" or bin == "LeftLong3"):
            yAvg = yAvg - 50

        if (bin == "RightCorner" or bin == "LeftCorner"):
            txt = "{0}/{1} \n {2:.2f} \n PPS \n {3:.2f} \n ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                  row["expectedPointsAvg"])
        else:
            txt = "{0}/{1} \n {2:.2f} PPS \n {3:.2f} ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                  row["expectedPointsAvg"])

        plt.text(xAvg, yAvg, txt, fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker=".")

    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("PPS - ePPS")

    # DEFENSE

    plt.subplot(1, 2, 2)
    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)
    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')
    plt.title("Defensive Points Per Shot While On Court:\n {}".format(name))

    shotZonesDataTeamD = shotZonesD[shotZonesD["playerName"] == name]
    for r in shotZonesDataTeamD.index:
        row = shotZonesDataTeamD.loc[r,]
        bin = row["bin"]
        plotVal = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])

        if (bin == "Right27FT" or bin == "Left27FT"):
            yAvg = yAvg + 20
        elif (bin == "Right23FT" or bin == "Left23FT"):
            yAvg = yAvg + 10
        elif (bin == "RightLong3" or bin == "LeftLong3"):
            yAvg = yAvg - 50

        if (bin == "RightCorner" or bin == "LeftCorner"):
            txt = "{0}/{1} \n {2:.2f} \n PPS \n {3:.2f} \n ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                  row["expectedPointsAvg"])
        else:
            txt = "{0}/{1} \n {2:.2f} PPS \n {3:.2f} ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                  row["expectedPointsAvg"])

        plt.text(xAvg, yAvg, txt, fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker=".")

    plt.subplots_adjust(right=1)
    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("ePPS - PPS")
    plt.tight_layout()

    plt.savefig("plots/PlayerShotChart/{}_{}".format(name, season), dpi=900, figsize=(14, 6))
    plt.close()

    #plt.show()

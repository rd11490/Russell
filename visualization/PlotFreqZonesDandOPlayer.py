import matplotlib.colors as colors
import matplotlib.pyplot as plt
import pandas as pd

import MySQLConnector
import ShotZones
import drawCourt

shotZones = ShotZones.buildShotZones()
valueToCreate = "Frequency"
valueToCreatePlayer = "Frequency_player"
valueToCreateLeague = "Frequency_league"
player = "Andrew Wiggins"
valueForPlotting = "Frequency Diff"

sql = MySQLConnector.MySQLConnector()
season = "2017-18"
shotCutoff = 250

o_query = "SELECT * FROM (select * from nba.offense_expected_points_by_player_zoned " \
          "WHERE season = '{0}' and attempts > {1} ) a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season, shotCutoff)

d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_player_zoned " \
          "WHERE season = '{0}' and attempts > {1} ) a " \
          "left join  (SELECT primaryKey, playerName FROM nba.roster_player " \
          "WHERE season = '{0}') b " \
          "on (a.id = b.primaryKey)".format(season, shotCutoff)

shotZonesO = sql.runQuery(o_query)
shotZonesD = sql.runQuery(d_query)

shotZonesD = shotZonesD[shotZonesD["playerName"] == player]
shotZonesO = shotZonesO[shotZonesO["playerName"] == player]

shotZonesDLeague = pd.read_csv("data/LeagueDShotChartAttempts.csv")
shotZonesOLeague = pd.read_csv("data/LeagueOShotChartAttempts.csv")

shotZonesD[valueToCreate] = 100 * shotZonesD["attempts"] / shotZonesD["attempts"].sum()
shotZonesO[valueToCreate] = 100 * shotZonesO["attempts"] / shotZonesO["attempts"].sum()

shotZonesDLeague[valueToCreate] = 100 * shotZonesDLeague["attempts"] / shotZonesDLeague["attempts"].sum()
shotZonesOLeague[valueToCreate] = 100 * shotZonesOLeague["attempts"] / shotZonesOLeague["attempts"].sum()

shotZonesDJoined = shotZonesD.merge(shotZonesDLeague[["bin", valueToCreate]], on="bin", suffixes=["_player", "_league"])
shotZonesDJoined[valueForPlotting] = shotZonesDJoined[valueToCreatePlayer] - shotZonesDJoined[valueToCreateLeague]

shotZonesOJoined = shotZonesO.merge(shotZonesOLeague[["bin", valueToCreate]], on="bin", suffixes=["_player", "_league"])
shotZonesOJoined[valueForPlotting] = shotZonesOJoined[valueToCreatePlayer] - shotZonesOJoined[valueToCreateLeague]

maxVal = 4  # max(max(shotZonesO[valueForPlotting]), max(shotZonesD[valueForPlotting]))
minVal = -4  # min(min(shotZonesO[valueForPlotting]), min(shotZonesD[valueForPlotting]))

norm = colors.Normalize(vmin=minVal, vmax=maxVal)

cls = plt.cm.get_cmap('RdYlGn')

sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
sm._A = []
teamNames = set(shotZonesDJoined["playerName"])
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

    plt.subplot(1, 2, 1)
    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)
    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')
    shotZonesDataTeamO = shotZonesOJoined[shotZonesOJoined["playerName"] == name]
    plt.title("Offensive Shot Frequency While On Court:\n {}".format(name))
    plt.tight_layout()

    for r in shotZonesDataTeamO.index:
        row = shotZonesDataTeamO.loc[r,]
        bin = row["bin"]
        plotVal = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])

        if bin == "Right27FT" or bin == "Left27FT":
            yAvg += 20
        elif bin == "Right23FT" or bin == "Left23FT":
            yAvg += 10
        elif bin == "RightLong3" or bin == "LeftLong3":
            yAvg -= 50

        if bin == "RightCorner" or bin == "LeftCorner" or "LeftBaseline23FT" or "RightBaseline23FT":
            txt = "{0}/{1} \n W/Player \n {2:.2f}%  \n League Avg \n {3:.2f}%".format(row["made"], row["attempts"],
                                                                                      row[valueToCreatePlayer],
                                                                                      row[valueToCreateLeague])
        else:
            txt = "{0}/{1} \n W/Player {2:.2f}% \n League Avg {3:.2f}%".format(row["made"], row["attempts"],
                                                                               row[valueToCreatePlayer],
                                                                               row[valueToCreateLeague])

        plt.text(xAvg, yAvg, txt, fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker=".")

    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("Freq - League Freq")

    # DEFENSE

    plt.subplot(1, 2, 2)
    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)
    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')
    plt.title("Defensive Shot Frequency While On Court:\n {}".format(name))

    shotZonesDataTeamD = shotZonesDJoined[shotZonesDJoined["playerName"] == name]
    for r in shotZonesDataTeamD.index:
        row = shotZonesDataTeamD.loc[r,]
        bin = row["bin"]
        plotVal = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])

        if bin == "Right27FT" or bin == "Left27FT":
            yAvg += 20
        elif bin == "Right23FT" or bin == "Left23FT":
            yAvg += 10
        elif bin == "RightLong3" or bin == "LeftLong3":
            yAvg -= 50

        if bin == "RightCorner" or bin == "LeftCorner" or "LeftBaseline23FT" or "RightBaseline23FT":
            txt = "{0}/{1} \n W/Player \n {2:.2f}%  \n League Avg \n {3:.2f}%".format(row["made"], row["attempts"],
                                                                                      row[valueToCreatePlayer],
                                                                                      row[valueToCreateLeague])
        else:
            txt = "{0}/{1} \n W/Player {2:.2f}% \n League Avg {3:.2f}%".format(row["made"], row["attempts"],
                                                                               row[valueToCreatePlayer],
                                                                               row[valueToCreateLeague])

        plt.text(xAvg, yAvg, txt, fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker=".")

    plt.subplots_adjust(right=1)
    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("Shot Freq - League Shot Freq")
    plt.tight_layout()

    plt.savefig("plots/PlayerShotFreqChart/{}".format(name), dpi=900, figsize=(14, 6))
    plt.close()

    # plt.show()

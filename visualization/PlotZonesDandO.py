import matplotlib.colors as colors
import pandas as pd
import drawCourt
import matplotlib.pyplot as plt
import ShotZones

shotZones = ShotZones.buildShotZones()
valueForPlotting = "Diff"

shotZonesD = pd.read_csv("data/ePPSZonedD.csv")
shotZonesO = pd.read_csv("data/ePPSZonedO.csv")

shotZonesD[valueForPlotting] = shotZonesD["expectedPointsAvg"] - shotZonesD["pointsAvg"]
shotZonesO[valueForPlotting] = shotZonesO["pointsAvg"] - shotZonesO["expectedPointsAvg"]

maxVal = max(max(shotZonesO[valueForPlotting]), max(shotZonesD[valueForPlotting]))
minVal = min(min(shotZonesO[valueForPlotting]), min(shotZonesD[valueForPlotting]))

norm = colors.Normalize(vmin=minVal, vmax=maxVal)

cls = plt.cm.get_cmap('RdYlGn')

sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
sm._A = []
teamNames = set(shotZonesD["teamName"])
for name in teamNames:
    print(name)

    font = {'family': 'serif',
            'color': 'black',
            'weight': 'bold',
            'size': 4,
            'ha': 'center',
            'va': 'center'}

    # OFFENSE
    plt.rcParams["figure.figsize"] = [16, 6]
    #plt.colorbar( mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])

    plt.subplot(1, 2, 1)
    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)
    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')
    shotZonesDataTeamO = shotZonesO[shotZonesO["teamName"] == name]
    plt.title("Offensive Points Per Shot:\n {}".format(name))
    plt.tight_layout()

    for r in shotZonesDataTeamO.index:
        row = shotZonesDataTeamO.loc[r,]
        bin = row["bin"]
        ePPS = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])
        plt.text(xAvg, yAvg,
                 "{0}/{1} \n {2:.2f} PPS \n {3:.2f} ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                 row["expectedPointsAvg"]),
                 fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(ePPS)), marker=".")

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
    plt.title("Defensive Points Per Shot:\n {}".format(name))

    shotZonesDataTeamD = shotZonesD[shotZonesD["teamName"] == name]
    for r in shotZonesDataTeamD.index:
        row = shotZonesDataTeamD.loc[r,]
        bin = row["bin"]
        ePPS = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])
        plt.text(xAvg, yAvg,
                 "{0}/{1} \n {2:.2f} PPS \n {3:.2f} ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                                 row["expectedPointsAvg"]),
                 fontdict=font)
        plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(ePPS)), marker=".")

    plt.subplots_adjust(right=1)
    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("ePPS - PPS")
    plt.tight_layout()
    plt.savefig("plots/ShotChart/{}".format(name), dpi=900, figsize=(14, 6))
    plt.close()

    #plt.show()

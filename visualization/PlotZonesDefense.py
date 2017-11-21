import matplotlib.colors as colors
import pandas as pd
import drawCourt
import matplotlib.pyplot as plt
import ShotZones

shotZones = ShotZones.buildShotZones()
valueForPlotting = "Diff"

shotZonesData = pd.read_csv("data/ePPSZonedD.csv")

shotZonesData[valueForPlotting] = shotZonesData["expectedPointsAvg"] - shotZonesData["pointsAvg"]

maxVal = max(shotZonesData[valueForPlotting])
minVal = min(shotZonesData[valueForPlotting])

norm = colors.Normalize(vmin=minVal, vmax=maxVal)

cls = plt.cm.get_cmap('RdYlGn')

sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
sm._A = []
teamNames = set(shotZonesData["teamName"])
for name in teamNames:
    print(name)
    shotZonesDataTeam = shotZonesData[shotZonesData["teamName"] == name]

    plt.xlim(-250, 250)
    plt.ylim(-47.5, 422.5)

    ax = drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
    ax.xaxis.label.set_visible(False)
    ax.yaxis.label.set_visible(False)
    plt.axis('off')

    font = {'family': 'serif',
            'color': 'black',
            'weight': 'bold',
            'size': 4,
            'ha': 'center',
            'va': 'center'}

    for r in shotZonesDataTeam.index:
        row = shotZonesDataTeam.loc[r,]
        name = row["teamName"]
        bin = row["bin"]
        ePPS = row[valueForPlotting]
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])
        plt.text(xAvg, yAvg,
                 "{0}/{1} \n {2:.2f} PPS \n {3:.2f} ePPS".format(row["made"], row["attempts"], row["pointsAvg"],
                                                       row["expectedPointsAvg"]),
                 fontdict=font)
        plot = plt.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(ePPS)), marker=".")

    plt.title("Defensive Points Per Shot for: {}".format(name))
    plt.colorbar(sm, ticks=[minVal, (maxVal+minVal)/2, maxVal], label='ePPS - PPS')
    plt.savefig("DShotCharts/{}_D".format(name), dpi=900)
    plt.close()

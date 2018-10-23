import matplotlib.colors as colors
import drawCourt
import matplotlib.pyplot as plt
import ShotZones


import MySQLConnector


params = {

}
print(params)

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
plt.title("Shot Frequency for:\n {}".format(params))
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

    if bin == "RightCorner" or bin == "LeftCorner":
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
plt.title("Defensive Points Per Shot:\n {}".format(name))

shotZonesDataTeamD = shotZonesD[shotZonesD["teamName"] == name]
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

    if bin == "RightCorner" or bin == "LeftCorner":
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

plt.savefig("plots/ShotChart/{}".format(name), dpi=900, figsize=(14, 6))
plt.close()

#plt.show()

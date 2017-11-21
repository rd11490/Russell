import pandas as pd
import drawCourt
import matplotlib.pyplot as plt


def buildShotPlot(shots, teamIds):
    for t in teamIds:
        print(t)
        teamShots = shots[shots["defenseTeamId"] == t]
        makes = teamShots[teamShots["shotMadeFlag"] == 1]
        misses = teamShots[teamShots["shotMadeFlag"] == 0]

        ax = makes.plot(x="xCoordinate", y="yCoordinate", kind="scatter", label="Make", marker="o", facecolors='none', edgecolors='green')
        ax = misses.plot(ax=ax, x="xCoordinate", y="yCoordinate", kind="scatter", c="Red", label="Miss", marker="x")

        plt.xlim(-250, 250)
        plt.ylim(-47.5, 422.5)
        drawCourt.draw_shot_chart_court(outer_lines=True)
        plt.title("Defensive Shot Chart for {}".format(teamName))
        ax.xaxis.label.set_visible(False)
        ax.yaxis.label.set_visible(False)
        plt.axis('off')
        plt.legend()
        plt.savefig("plots/{}_chart".format(teamName))
        plt.close()

colors = {0: "r",
          1: "g"}

markers = {
    0: "x",
    1: "o"}

label = {
    0: "miss",
    1: "make"}

dShots = pd.read_csv("data/ePPSZonedD.csv")
dShots["color"] = dShots['shotMadeFlag'].apply(lambda x: colors[x])
dShots["marker"] = dShots['shotMadeFlag'].apply(lambda x: markers[x])
dShots["label"] = dShots['shotMadeFlag'].apply(lambda x: label[x])


teamIds = list(teamInfo["teamId"])


buildShotPlot(shots, teamIds)


"""
shotLocs = shots[["xCoordinate", "yCoordinate", "shotMadeFlag"]].head(1000)

shotLocs = shotLocs[
    (shotLocs["yCoordinate"] < 400) & (shotLocs["yCoordinate"] > -50) & (shotLocs["xCoordinate"] < 250) & (
    shotLocs["xCoordinate"] > -250)]




shotLocs["color"] = shotLocs['shotMadeFlag'].apply(lambda x: colors[x])

shotLocs

plt.show()
"""
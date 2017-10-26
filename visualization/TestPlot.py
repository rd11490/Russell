import pandas as pd
import drawCourt
import matplotlib.pyplot as plt

data = pd.read_csv("data/event5.csv").sort_values(by="gameClock", ascending=False)

ballLoc = data[["ballxLoc", "ballyLoc", "gameClock"]].as_matrix()

colors = {1610612751: "k",
          1610612738: "g",
          -1: "r"}


def playerColumns(i):
    return ["player{}Id".format(i), "player{}TeamId".format(i), "player{}xLoc".format(i), "player{}yLoc".format(i)]





players = []
rows = int(data.size)
plt.ion()
for i in range(1, 11):
    players.append(data[playerColumns(i)].as_matrix())

for i in range(rows-1):
    plt.cla()
    drawCourt.draw_court()
    for p in players:
        playerId = p[i, 0]
        teamId = int(p[i, 1])
        xLoc = p[i, 2]
        yLoc = p[i, 3]

        plt.plot(xLoc, yLoc, label=playerId, color=colors[teamId], marker='o')
    plt.plot(ballLoc[i, 0], ballLoc[i, 1], label="ball", color=colors[-1], marker='o')
    plt.xlim((-5, 105))
    plt.ylim((-5, 55))
    plt.title("Time: {}".format(ballLoc[i, 2]))
    plt.pause(.1)

import pandas as pd
import drawCourt
import matplotlib.pyplot as plt

shots = pd.read_csv("data/rozierShots.csv")

shotLocs = shots[["xCoordinate", "yCoordinate", "eventType"]]

print(shotLocs)

shotLocs = shotLocs[(shotLocs["yCoordinate"]<400) & (shotLocs["yCoordinate"]>-50) & (shotLocs["xCoordinate"]<250) & (shotLocs["xCoordinate"]>-250)]

colors = {"Missed Shot": "r",
          "Made Shot": "g"}

shotLocs["color"] = shotLocs['eventType'].apply(lambda x: colors[x])

#shotLocs.plot(x="xCoordinate", y="yCoordinate", kind="scatter", c=shotLocs["color"])
plt.xlim(-250,250)
plt.ylim(422.5, -47.5)
drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
plt.show()
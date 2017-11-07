import pandas as pd
import drawCourt
import matplotlib.pyplot as plt

shots = pd.read_csv("data/allShots.csv")

shotLocs = shots[["loc", "xCoordinate", "yCoordinate"]].head(1000)

shotLocs = shotLocs[
    (shotLocs["yCoordinate"] < 400) & (shotLocs["yCoordinate"] > -50) & (shotLocs["xCoordinate"] < 250) & (
    shotLocs["xCoordinate"] > -250)]

colors = {"Missed Shot": "r",
          "Made Shot": "g"}

area_colors = {
    "Left Corner 3_Left Side(L)": "orange",
    "Above the Break 3_Left Side Center(LC)": "blue",
    "Above the Break 3_Center(C)": "orange",
    "Above the Break 3_Right Side Center(RC)": "blue",
    "Right Corner 3_Right Side(R)": "orange",

    "Mid-Range_Left Side(L)": "darkgreen",
    "Mid-Range_Left Side Center(LC)": "gold",
    "Mid-Range_Center(C)": "navy",
    "Mid-Range_Right Side Center(RC)": "gold",
    "Mid-Range_Right Side(R)": "darkgreen",

    "Restricted Area_Center(C)": "red",

    "In The Paint (Non-RA)_Left Side(L)": "darkcyan",
    "In The Paint (Non-RA)_Center(C)": "black",
    "In The Paint (Non-RA)_Right Side(R)": "darkcyan",

}

shotLocs["color"] = shotLocs['loc'].apply(lambda x: area_colors[x])

#shotLocs.plot(x="xCoordinate", y="yCoordinate", kind="scatter", c=shotLocs["color"])
plt.xlim(-250, 250)
plt.ylim(-47.5, 422.5)
drawCourt.draw_shot_chart_court_with_zones(outer_lines=True)
plt.show()
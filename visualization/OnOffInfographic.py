import matplotlib.colors as colors
import matplotlib.pyplot as plt
import math

import MySQLConnector
import ShotZones
import drawCourt
import matplotlib.gridspec as gridspec

shotZones = ShotZones.buildShotZones()
players = ["Al Horford", "Aron Baynes", "Daniel Theis", "Jaylen Brown", "Jayson Tatum", "Kyrie Irving", "Marcus Smart",
           "Terry Rozier", "Semi Ojeleye", "Marcus Morris", "Abdel Nader"]
season = "2017-18"

sql = MySQLConnector.MySQLConnector()

shot_frequency = "shotFrequency"
attempts = "attempts"


def extract_player_on_off(df, player_name):
    player_df = df[df["playerName"] == player_name]
    player_on_df = player_df[player_df["onOff"] == "On"]
    player_off_df = player_df[player_df["onOff"] == "Off"]
    return player_on_df, player_off_df


def calculate_shot_frequency(df):
    df[shot_frequency] = 100 * df[attempts] / df[attempts].sum()
    return df


def combine_on_off(on, off):
    comb = on[["bin", shot_frequency]].merge(off[["bin", shot_frequency]], on="bin", suffixes=["_on", "_off"],
                                             how='outer')
    return comb


def plot_frequency_shot_chart(ax, df):
    for r in df.index:
        row = df.loc[r,]
        bin = row["bin"]
        on = row["{}_on".format(shot_frequency)]
        if math.isnan(on):
            on = 0.0
        off = row["{}_off".format(shot_frequency)]
        if math.isnan(off):
            off = 0.0
        plotVal = on - off
        zoneLocs = shotZones[bin]
        xAvg = sum(zoneLocs["X"]) / len(zoneLocs["X"])
        yAvg = sum(zoneLocs["Y"]) / len(zoneLocs["Y"])

        if bin == "Right27FT" or bin == "Left27FT":
            yAvg += 20
        elif bin == "Right23FT" or bin == "Left23FT":
            yAvg += 10
        elif bin == "RightLong3" or bin == "LeftLong3":
            yAvg -= 50

        txt = "On: \n {0:.2f}% \n Off: \n {1:.2f}%".format(on, off)

        ax.text(xAvg, yAvg, txt, fontdict=font)
        ax.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker="h", s=3)
    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("On - Off")
    return ax


o_query = "SELECT * FROM (select * from nba.offense_expected_points_by_player_on_off_zoned " \
          "WHERE season = '{0}') a " \
          "left join  (SELECT playerId, playerName FROM nba.roster_player WHERE season = '{0}') b " \
          "on (a.id = b.playerId)".format(season)

d_query = "SELECT * FROM (select * from nba.defense_expected_points_by_player_on_off_zoned " \
          "WHERE season = '{0}') a " \
          "left join  (SELECT playerId, playerName FROM nba.roster_player " \
          "WHERE season = '{0}') b " \
          "on (a.id = b.playerId)".format(season)

shot_zones_O = sql.runQuery(o_query)
shot_zones_D = sql.runQuery(d_query)

maxVal = 4  # max(max(shotZonesO[valueForPlotting]), max(shotZonesD[valueForPlotting]))
minVal = -4  # min(min(shotZonesO[valueForPlotting]), min(shotZonesD[valueForPlotting]))

norm = colors.Normalize(vmin=minVal, vmax=maxVal)

cls = plt.cm.get_cmap('RdYlGn')

sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
sm._A = []

font = {'family': 'serif',
        'color': 'black',
        'weight': 'bold',
        'size': 6,
        'ha': 'center',
        'va': 'center'}

for player in players:
    shot_zones_d_on, shot_zones_d_off = extract_player_on_off(shot_zones_D, player)
    shot_zones_o_on, shot_zones_o_off = extract_player_on_off(shot_zones_O, player)

    shot_zones_d_on = calculate_shot_frequency(shot_zones_d_on)
    shot_zones_d_off = calculate_shot_frequency(shot_zones_d_off)
    shot_zones_o_on = calculate_shot_frequency(shot_zones_o_on)
    shot_zones_o_off = calculate_shot_frequency(shot_zones_o_off)

    shot_zones_d_comb = combine_on_off(shot_zones_d_on, shot_zones_d_off)
    shot_zones_o_comb = combine_on_off(shot_zones_o_on, shot_zones_o_off)

    fig = plt.figure(figsize=(16, 6))
    fig.suptitle("{} \n On-Off Shot Frequency Charts".format(player), fontsize=12)
    gs0 = gridspec.GridSpec(1, 2)

    ax1 = plt.Subplot(fig, gs0[0])
    fig.add_subplot(ax1)

    ax1.set_xlim(-250, 250)
    ax1.set_ylim(-47.5, 422.5)
    ax1 = drawCourt.draw_shot_chart_court_with_zones(ax=ax1, outer_lines=True)
    ax1.get_xaxis().set_visible(False)
    ax1.get_yaxis().set_visible(False)
    ax1.xaxis.label.set_visible(False)
    ax1.yaxis.label.set_visible(False)
    ax1.set_title("Offensive Shot Frequency", fontsize=12)

    ax1 = plot_frequency_shot_chart(ax1, shot_zones_o_comb)

    ax2 = plt.Subplot(fig, gs0[1])
    fig.add_subplot(ax2)

    ax2.set_xlim(-250, 250)
    ax2.set_ylim(-47.5, 422.5)
    ax2 = drawCourt.draw_shot_chart_court_with_zones(ax=ax2, outer_lines=True)
    ax2.get_xaxis().set_visible(False)
    ax2.get_yaxis().set_visible(False)
    ax2.xaxis.label.set_visible(False)
    ax2.yaxis.label.set_visible(False)
    ax2.set_title("Defensive Shot Frequency", fontsize=12)

    ax2 = plot_frequency_shot_chart(ax2, shot_zones_d_comb)
    fig.tight_layout(rect=[0, 0, 1, .925])
    plt.savefig("plots/PlayerShotFreqChart/Celtics/{}".format(player), figsize=(16, 6), dpi=900)
    plt.close()
    # plt.show()

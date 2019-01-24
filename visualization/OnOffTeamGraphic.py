import math
import os

import matplotlib.colors as colors
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt

import ShotZones
import drawCourt
from cred import MySQLConnector

shotZones = ShotZones.buildShotZones()
season = "2017-18"

sql = MySQLConnector.MySQLConnector()

shot_frequency = "shotFrequency"
attempts = "attempts"
pps = "pointsAvg"
epps = "expectedPointsAvg"


def extract_team_and_league(df, team_name):
    team_df = df[df["teamName"] == team_name]
    league_df = df[df["teamName"] == "League"]
    return team_df, league_df


def calculate_shot_frequency(df):
    df[shot_frequency] = 100 * df[attempts] / df[attempts].sum()
    return df


def combine_on_off(on, off):
    comb = on.merge(off, on="bin", suffixes=["_on", "_off"], how='outer')
    return comb


def plot_on_off_shot_chart(ax, df, col, minVal, maxVal, type):
    norm = colors.Normalize(vmin=minVal, vmax=maxVal)

    cls = plt.cm.get_cmap('RdYlGn')

    sm = plt.cm.ScalarMappable(cmap=cls, norm=norm)
    sm._A = []
    for r in df.index:
        row = df.loc[r,]
        bin = row["bin"]
        on = row["{}_on".format(col)]
        if math.isnan(on):
            on = 0.0
        off = row["{}_off".format(col)]
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

        if (bin == "RightCorner" or bin == "LeftCorner") and type != "%":
            txt = "Team: \n {0:.2f}\n{2} \n League: \n {1:.2f}\n{2}".format(on, off, type)
        else:
            txt = "Team: \n {0:.2f}{2} \n League: \n {1:.2f}{2}".format(on, off, type)

        ax.text(xAvg, yAvg, txt, fontdict=font)
        ax.scatter(x=zoneLocs["X"], y=zoneLocs["Y"], color=cls(norm(plotVal)), marker="h", s=3)
    clb = plt.colorbar(mappable=sm, ticks=[minVal, (maxVal + minVal) / 2, maxVal])
    clb.ax.set_title("Team - League")
    return ax


o_query = "SELECT * FROM (select * from nba.offense_expected_points " \
          "WHERE season = '{0}' and bin != 'Total') a " \
          "left join  (select * from nba.team_info where season = '{0}') b " \
          "on (a.teamId = b.teamId)".format(season)

d_query = "SELECT * FROM (select * from nba.defense_expected_points " \
          "WHERE season = '{0}' and bin != 'Total') a " \
          "left join  (select * from nba.team_info where season = '{0}') b " \
          "on (a.teamId = b.teamId)".format(season)

shot_zones_O = sql.runQuery(o_query)
shot_zones_D = sql.runQuery(d_query)

font = {'family': 'serif',
        'color': 'black',
        'weight': 'bold',
        'size': 6,
        'ha': 'center',
        'va': 'center'}

teams = set(shot_zones_O["teamName"])

for team in teams:
    print(team)
    shot_zones_d_team, shot_zones_d_league = extract_team_and_league(shot_zones_D, team)
    shot_zones_o_team, shot_zones_o_league = extract_team_and_league(shot_zones_O, team)

    shot_zones_d_on = calculate_shot_frequency(shot_zones_d_team)
    shot_zones_d_off = calculate_shot_frequency(shot_zones_d_league)
    shot_zones_o_on = calculate_shot_frequency(shot_zones_o_team)
    shot_zones_o_off = calculate_shot_frequency(shot_zones_o_league)

    shot_zones_d_comb = combine_on_off(shot_zones_d_on, shot_zones_d_off)
    shot_zones_o_comb = combine_on_off(shot_zones_o_on, shot_zones_o_off)

    fig = plt.figure(figsize=(16, 6))
    fig.suptitle("{} \n Shot Frequency Charts (vs League Avg)".format(team), fontsize=12)
    gs0 = gridspec.GridSpec(1, 2)

    ax1 = plt.Subplot(fig, gs0[0, 0])
    fig.add_subplot(ax1)

    ####
    #### Offensive Shot Profile
    ####
    ax1.set_xlim(-250, 250)
    ax1.set_ylim(-47.5, 422.5)
    ax1 = drawCourt.draw_shot_chart_court_with_zones(ax=ax1, outer_lines=True)
    ax1.get_xaxis().set_visible(False)
    ax1.get_yaxis().set_visible(False)
    ax1.xaxis.label.set_visible(False)
    ax1.yaxis.label.set_visible(False)
    ax1.set_title("Offensive Shot Frequency", fontsize=12)

    ax1 = plot_on_off_shot_chart(ax1, shot_zones_o_comb, shot_frequency, -4, 4, "%")

    ####
    #### Defensive Shot Profile
    ####
    ax2 = plt.Subplot(fig, gs0[0, 1])
    fig.add_subplot(ax2)

    ax2.set_xlim(-250, 250)
    ax2.set_ylim(-47.5, 422.5)
    ax2 = drawCourt.draw_shot_chart_court_with_zones(ax=ax2, outer_lines=True)
    ax2.get_xaxis().set_visible(False)
    ax2.get_yaxis().set_visible(False)
    ax2.xaxis.label.set_visible(False)
    ax2.yaxis.label.set_visible(False)
    ax2.set_title("Defensive Shot Frequency", fontsize=12)

    ax2 = plot_on_off_shot_chart(ax2, shot_zones_d_comb, shot_frequency, -4, 4, "%")

    results_dir = "plots/TeamShotFreqChart/{0}".format(season)
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

    fig.tight_layout(rect=[0, 0, 1, .925])
    plt.savefig("{0}/{1}.png".format(results_dir, team), figsize=(16, 6), dpi=900)
    plt.close()

    ####
    #### Offensive PPS
    ####

    fig = plt.figure(figsize=(16, 6))
    fig.suptitle("{} \n Points Per Shot Charts (vs League Avg)".format(team), fontsize=12)
    gs0 = gridspec.GridSpec(1, 2)

    ax1 = plt.Subplot(fig, gs0[0, 0])
    fig.add_subplot(ax1)

    ax1.set_xlim(-250, 250)
    ax1.set_ylim(-47.5, 422.5)
    ax1 = drawCourt.draw_shot_chart_court_with_zones(ax=ax1, outer_lines=True)
    ax1.get_xaxis().set_visible(False)
    ax1.get_yaxis().set_visible(False)
    ax1.xaxis.label.set_visible(False)
    ax1.yaxis.label.set_visible(False)
    ax1.set_title("Offensive Points Per Shot", fontsize=12)

    ax1 = plot_on_off_shot_chart(ax1, shot_zones_o_comb, pps, -1, 1, " PPS")

    ####
    #### Defensive PPS
    ####
    ax2 = plt.Subplot(fig, gs0[0, 1])
    fig.add_subplot(ax2)

    ax2.set_xlim(-250, 250)
    ax2.set_ylim(-47.5, 422.5)
    ax2 = drawCourt.draw_shot_chart_court_with_zones(ax=ax2, outer_lines=True)
    ax2.get_xaxis().set_visible(False)
    ax2.get_yaxis().set_visible(False)
    ax2.xaxis.label.set_visible(False)
    ax2.yaxis.label.set_visible(False)
    ax2.set_title("Defensive Points Per Shot", fontsize=12)

    ax2 = plot_on_off_shot_chart(ax2, shot_zones_d_comb, pps, -1, 1, " PPS")

    results_dir = "plots/TeamPPSChart/{0}".format(season)
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

    fig.tight_layout(rect=[0, 0, 1, .925])
    plt.savefig("{0}/{1}.png".format(results_dir, team), figsize=(16, 6), dpi=900)
    plt.close()

    ####
    #### Offensive ePPS
    ####

    fig = plt.figure(figsize=(16, 6))
    fig.suptitle("{} \n Expected Points Per Shot Charts (vs League Avg)".format(team), fontsize=12)
    gs0 = gridspec.GridSpec(1, 2)

    ax1 = plt.Subplot(fig, gs0[0, 0])
    fig.add_subplot(ax1)
    ax1.set_xlim(-250, 250)
    ax1.set_ylim(-47.5, 422.5)
    ax1 = drawCourt.draw_shot_chart_court_with_zones(ax=ax1, outer_lines=True)
    ax1.get_xaxis().set_visible(False)
    ax1.get_yaxis().set_visible(False)
    ax1.xaxis.label.set_visible(False)
    ax1.yaxis.label.set_visible(False)
    ax1.set_title("Offensive Expected Points Per Shot", fontsize=12)

    ax1 = plot_on_off_shot_chart(ax1, shot_zones_o_comb, epps, -1, 1, " ePPS")

    ####
    #### Defensive ePPS
    ####
    ax2 = plt.Subplot(fig, gs0[0, 1])
    fig.add_subplot(ax2)

    ax2.set_xlim(-250, 250)
    ax2.set_ylim(-47.5, 422.5)
    ax2 = drawCourt.draw_shot_chart_court_with_zones(ax=ax2, outer_lines=True)
    ax2.get_xaxis().set_visible(False)
    ax2.get_yaxis().set_visible(False)
    ax2.xaxis.label.set_visible(False)
    ax2.yaxis.label.set_visible(False)
    ax2.set_title("Defensive Expected Points Per Shot", fontsize=12)

    ax2 = plot_on_off_shot_chart(ax2, shot_zones_d_comb, epps, -1, 1, " ePPS")

    results_dir = "plots/TeamEPPSChart/{0}".format(season)
    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

    fig.tight_layout(rect=[0, 0, 1, .925])
    plt.savefig("{0}/{1}.png".format(results_dir, team), figsize=(16, 6), dpi=900)
    plt.close()
    # plt.show()

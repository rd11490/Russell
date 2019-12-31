import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("TkAgg")

import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

team_info = pd.read_csv('team_info.csv', dtype={'teamId': str})
team_info = team_info[team_info['teamId'] != '0']
team_info['name'] = team_info['teamCity'] + ' ' + team_info['teamName']
team_name_lookup = team_info[['teamId', 'name']].set_index('teamId').to_dict()['name']
east = ['Atlanta Hawks', 'Boston Celtics', 'Cleveland Cavaliers', 'Chicago Bulls', 'Miami Heat', 'Milwaukee Bucks', 'Brooklyn Nets', 'New York Knicks', 'Orlando Magic', 'Indiana Pacers', 'Philadelphia 76ers', 'Toronto Raptors', 'Washington Wizards', 'Detroit Pistons', 'Charlotte Hornets']
west = set(team_info['name']) - set(east)

seasons = pd.read_csv('season_sim_19_20.csv', index_col='index')

cols = [team_name_lookup[c] for c in seasons.columns]
seasons.columns = cols

east_season = seasons[east]
west_season = seasons[west]

ranked_east = east_season.rank(axis=1, ascending=False, method='min')
ranked_west = west_season.rank(axis=1, ascending=False, method='min')

rank_frame_east = pd.DataFrame()
rank_frame_east['rank'] = np.arange(0,15)

win_frame_east = pd.DataFrame()
win_frame_east['wins'] = np.arange(0,82)

# fig_wins, axes_wins = plt.subplots(nrows=15, ncols=1)
#
# fig_rank, axes_rank = plt.subplots(nrows=15, ncols=1)
# fig_rank.size = (10, 40)

for ind, team in enumerate(east):


    cnts = east_season[team].value_counts().sort_index()
    cnts = cnts/100
    win_frame_east[team] = cnts
    rnk_cnts = ranked_east[team].value_counts().sort_index()
    rnk_cnts /= 100
    rank_frame_east[team] = rnk_cnts

    fig, axes =  plt.subplots(nrows=2, ncols=1,figsize=(10,5))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle(team, fontsize=14)
    axes[0].set_xlim(15, 1)
    axes[0].set_xlabel('Rank')
    axes[0].set_ylim(0, 100)
    axes[0].set_ylabel('% of Sims')
    axes[0].set_xticks(np.arange(1,15))
    axes[0].set_title('Rank Probability')


    axes[0].plot(np.arange(0,15),rank_frame_east[team].values)

    axes[1].set_xlim(1, 82)
    axes[1].set_ylabel('Wins')
    axes[1].set_ylim(0, 20)
    axes[1].set_ylabel('% of Sims')
    axes[1].set_title('Wins Probaility')


    axes[1].plot(np.arange(0, 82), win_frame_east[team].values)

    plt.savefig('teams/{}'.format(team))




win_frame_east=win_frame_east.fillna(0.0).set_index('wins')
rank_frame_east=rank_frame_east.fillna(0.0).set_index('rank')


rank_frame_west = pd.DataFrame()
rank_frame_west['rank'] = np.arange(0,15)

win_frame_west = pd.DataFrame()
win_frame_west['wins'] = np.arange(0,82)

for team in west:
    cnts = west_season[team].value_counts().sort_index()
    cnts = cnts/100
    win_frame_west[team] = cnts
    rnk_cnts = ranked_west[team].value_counts().sort_index()
    rnk_cnts = rnk_cnts/100
    rank_frame_west[team] = rnk_cnts

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 5))
    fig.suptitle(team, fontsize=14)
    plt.subplots_adjust(hspace=0.5)

    axes[0].set_xlim(15, 1)
    axes[0].set_xlabel('Rank')
    axes[0].set_ylim(0, 100)
    axes[0].set_ylabel('% of Sims')
    axes[0].set_xticks(np.arange(1, 15))
    axes[0].set_title('Rank Probability')

    axes[0].plot(np.arange(0, 15), rank_frame_west[team].values)


    axes[1].plot(np.arange(0, 82), win_frame_west[team].values)

    axes[1].set_xlim(1, 82)
    axes[1].set_ylabel('Wins')
    axes[1].set_ylim(0, 20)
    axes[1].set_ylabel('% of Sims')
    axes[1].set_title('Wins Probaility')

    plt.savefig('teams/{}'.format(team))

win_frame_west=win_frame_west.fillna(0.0).set_index('wins')
rank_frame_west=rank_frame_west.fillna(0.0).set_index('rank')

print(win_frame_east)
print(win_frame_west)
print(rank_frame_east)
print(rank_frame_west)


win_frame_east.to_csv('teams/wins_east.csv')
win_frame_west.to_csv('teams/wins_west.csv')

rank_frame_east.to_csv('teams/rank_east.csv')
rank_frame_west.to_csv('teams/rank_west.csv')
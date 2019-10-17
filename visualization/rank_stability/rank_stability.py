import pandas as pd
import MySqlDatabases.NBADatabase
from cred import MySQLConnector
import numpy as np
import matplotlib
matplotlib.use("TKAgg")
import matplotlib.pyplot as plt



pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()


game_log = sql.runQuery("SELECT * FROM nba_data.game_log where SEASON_TYPE = 'Regular Season'")

game_log = game_log[['TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'WL', 'SEASON']]

seasons = game_log['SEASON'].unique()

teams = game_log['TEAM_NAME'].unique()

east = ['Atlanta Hawks', 'Boston Celtics', 'Cleveland Cavaliers', 'Chicago Bulls', 'Miami Heat', 'Milwaukee Bucks', 'Brooklyn Nets', 'New York Knicks', 'Orlando Magic', 'Indiana Pacers', 'Philadelphia 76ers', 'Toronto Raptors', 'Washington Wizards', 'Detroit Pistons', 'Charlotte Bobcats', 'Charlotte Hornets']

game_log['conference'] = game_log.apply(lambda x: 'E' if x['TEAM_NAME'] in east else 'W', axis=1)

def rank_diff(team_season):
    final_rank = team_season['rank'].values[-1]
    team_season['rank_diff'] = team_season['rank'] - final_rank
    team_season['in_playoffs'] = team_season['rank'] <= 8
    max_diff = 0
    diffs = []
    in_playoffs = final_rank <= 8
    playoffs = []
    for index, row in team_season[::-1].iterrows():
        if abs(row['rank_diff']) > max_diff:
            max_diff = abs(row['rank_diff'])
        if not (in_playoffs and row['in_playoffs']):
            in_playoffs = False
        playoffs.append(in_playoffs)
        diffs.append(max_diff)

    playoffs[-1] = False
    team_season['max_diff'] = diffs[::-1]
    team_season['in_playoffs_stable'] = playoffs[::-1]
    team_season['final_rank'] = final_rank
    return team_season

# TODO: Remove seasons slice
all_seasons = []
for season in seasons:
    records = []
    game_log_season = game_log[game_log['SEASON'] == season].copy(deep=True)
    game_log_season = game_log_season.sort_values(by='GAME_DATE')
    groups = game_log_season.groupby(by=['TEAM_NAME'])
    for team_name, group in groups:
        conf = group['conference'].values[0]
        record = {
            'game_day': 0,
            'team': team_name,
            'wins': 0,
            'losses': 0,
            'conference': conf
        }
        team_records = [record.copy()]
        for ind, game in group.iterrows():
            new_record = {
                'game_day': record['game_day'] + 1,
                'team': team_name,
                'wins': record['wins'],
                'losses': record['losses'],
                'conference': conf
            }
            if game['WL'] == 'L':
                new_record['losses'] += 1
            else:
                new_record['wins'] += 1
            team_records.append(new_record)
            record = new_record
        team_season = pd.DataFrame(team_records)
        records.append(team_season)

    season_records = pd.concat(records)
    season_records['win_pct'] = season_records['wins']/season_records['game_day']
    season_records = season_records.fillna(0.0)

    season_records['rank'] = season_records.groupby(by=['game_day', 'conference'])['win_pct'].rank(ascending=False)
    season_records = season_records.groupby(by='team').apply(rank_diff)
    groups = season_records.groupby(by='team')
    fig = plt.figure(figsize=(15, 10), facecolor='w')
    for name, group in groups:
        plt.plot(group['game_day'].values, group['rank_diff'].values, label=name, marker='o')
    plt.xlabel('games')
    plt.ylabel('Dist from final rank')
    plt.legend(loc='lower left', bbox_to_anchor= (0.0, 1.01), ncol=7, borderaxespad=0, frameon=False)

    plt.savefig('stability - {}.png'.format(season), dpi=300)
    plt.close()

    fig_stab = plt.figure(figsize=(15, 10), facecolor='w')
    for name, group in groups:
        plt.plot(group['game_day'].values, group['max_diff'].values, label=name, marker='o')
    plt.xlabel('games')
    plt.ylabel('Stable Dist from final rank)')
    plt.legend(loc='lower left', bbox_to_anchor=(0.0, 1.01), ncol=7, borderaxespad=0, frameon=False)

    plt.savefig('true stability - {}.png'.format(season), dpi=300)
    plt.close()
    all_seasons.append(season_records)

all_seasons = pd.concat(all_seasons)
groups = all_seasons.groupby(by='game_day')

in_stable_band_pct_by_day = []
for day, group in groups:
    group['abs_diff'] = abs(group['rank_diff'])
    in_stable_band_pct_05 = group[group['abs_diff']<=0.5].shape[0]/group.shape[0]
    in_stable_band_pct_10 = group[group['abs_diff']<=1.0].shape[0]/group.shape[0]
    in_stable_band_pct_15 = group[group['abs_diff']<=1.5].shape[0]/group.shape[0]
    in_stable_band_pct_20 = group[group['abs_diff']<=2.0].shape[0]/group.shape[0]
    in_stable_band_pct_25 = group[group['abs_diff']<=2.5].shape[0]/group.shape[0]

    in_stable_band_pct_by_day.append({
        'game_day': day,
        'in_stable_band_pct_05': in_stable_band_pct_05*100,
        'in_stable_band_pct_10': in_stable_band_pct_10*100,
        'in_stable_band_pct_15': in_stable_band_pct_15*100,
        'in_stable_band_pct_20': in_stable_band_pct_20*100,
        'in_stable_band_pct_25': in_stable_band_pct_25*100
    })
in_stable_band_pct_frame = pd.DataFrame(in_stable_band_pct_by_day)

in_stable_band_pct_frame.to_csv('in_stable_band_pct.csv', index=False)

fig2 = plt.figure(figsize=(15, 10), facecolor='w')
plt.plot(in_stable_band_pct_frame['game_day'].values, in_stable_band_pct_frame['in_stable_band_pct_05'].values, label='+.5 Ranks', marker='o')
plt.plot(in_stable_band_pct_frame['game_day'].values, in_stable_band_pct_frame['in_stable_band_pct_10'].values, label='+-1.0 Ranks', marker='o')
plt.plot(in_stable_band_pct_frame['game_day'].values, in_stable_band_pct_frame['in_stable_band_pct_15'].values, label='+-1.5 Ranks', marker='o')
plt.plot(in_stable_band_pct_frame['game_day'].values, in_stable_band_pct_frame['in_stable_band_pct_20'].values, label='+-2.0 Ranks', marker='o')
plt.plot(in_stable_band_pct_frame['game_day'].values, in_stable_band_pct_frame['in_stable_band_pct_25'].values, label='+-2.5 Ranks', marker='o')
plt.xlabel('games')
plt.ylabel('% of teams within bounds')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=7, borderaxespad=0, frameon=False)
plt.gca().yaxis.grid(True)
plt.xticks(np.arange(0, 82, step=5))
plt.yticks(np.arange(0, 101, step=5))
plt.gca().xaxis.grid(True)
plt.title('Movement in Conference Ranking by Game')
plt.savefig('total_stability.png', dpi=300)


stable_pct_by_day = []
for day, group in groups:
    stable_pct_05 = group[group['max_diff']<=0.5].shape[0]/group.shape[0]
    stable_pct_10 = group[group['max_diff']<=1.0].shape[0]/group.shape[0]
    stable_pct_15 = group[group['max_diff']<=1.5].shape[0]/group.shape[0]
    stable_pct_20 = group[group['max_diff']<=2.0].shape[0]/group.shape[0]
    stable_pct_25 = group[group['max_diff']<=2.5].shape[0]/group.shape[0]

    in_playoff_pct = group[group['in_playoffs'] == True].shape[0] / (group.shape[0] * (16.0/ 30.0))
    in_playoff_stable_pct = group[group['in_playoffs_stable'] == True].shape[0] / (group.shape[0] * (16.0/ 30.0))

    if in_playoff_pct >= 1.0:
        in_playoff_pct=1.0
    if in_playoff_stable_pct >= 1.0:
        in_playoff_stable_pct=1.0


    stable_pct_by_day.append({
        'game_day': day,
        'stable_pct_05': stable_pct_05*100,
        'stable_pct_10': stable_pct_10*100,
        'stable_pct_15': stable_pct_15*100,
        'stable_pct_20': stable_pct_20*100,
        'stable_pct_25': stable_pct_25*100,

        'in_playoffs': in_playoff_pct * 100,
        'in_playoff_stable_pct': in_playoff_stable_pct * 100
    })
stable_pct_frame = pd.DataFrame(stable_pct_by_day)

stable_pct_frame.to_csv('stable_pct.csv', index=False)

fig3 = plt.figure(figsize=(15, 10), facecolor='w')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['stable_pct_05'].values, label='+.5 Ranks', marker='o')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['stable_pct_10'].values, label='+-1.0 Ranks', marker='o')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['stable_pct_15'].values, label='+-1.5 Ranks', marker='o')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['stable_pct_20'].values, label='+-2.0 Ranks', marker='o')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['stable_pct_25'].values, label='+-2.5 Ranks', marker='o')
plt.xlabel('games')
plt.ylabel('% of teams stable')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=7, borderaxespad=0, frameon=False)
plt.gca().yaxis.grid(True)
plt.xticks(np.arange(0, 82, step=5))
plt.yticks(np.arange(0, 101, step=5))
plt.gca().xaxis.grid(True)
plt.title('Stability in Conference Ranking by Game')
plt.savefig('true_stability.png', dpi=300)

fig4 = plt.figure(figsize=(15, 10), facecolor='w')
plt.plot(stable_pct_frame['game_day'].values, stable_pct_frame['in_playoff_stable_pct'].values, label='In Playoffs (Stable)', marker='o')
plt.xlabel('games')
plt.ylabel('% of playoff teams set')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=7, borderaxespad=0, frameon=False)
plt.gca().yaxis.grid(True)
plt.xticks(np.arange(0, 82, step=5))
plt.yticks(np.arange(0, 101, step=5))
plt.gca().xaxis.grid(True)
plt.title('Stability Playoff Teams by Game')
plt.savefig('playoff_stability.png', dpi=300)


standing_position = []
for day, group in groups:
    standing_position.append({
        'game_day': day,
        '1': group[group['final_rank'] == 1]['rank'].mean(),
        '2': group[group['final_rank'] == 2]['rank'].mean(),
        '3': group[group['final_rank'] == 3]['rank'].mean(),
        '4': group[group['final_rank'] == 4]['rank'].mean(),
        '5': group[group['final_rank'] == 5]['rank'].mean(),
        '6': group[group['final_rank'] == 6]['rank'].mean(),
        '7': group[group['final_rank'] == 7]['rank'].mean(),
        '8': group[group['final_rank'] == 8]['rank'].mean(),
    })
    
rank_by_finish = pd.DataFrame(standing_position)


fig5 = plt.figure(figsize=(15, 10), facecolor='w')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['1'].values, label='1st in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['2'].values, label='2nd in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['3'].values, label='3rd in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['4'].values, label='4th in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['5'].values, label='5th in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['6'].values, label='6th in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['7'].values, label='7th in Standings', marker='o')
plt.plot(rank_by_finish['game_day'].values, rank_by_finish['8'].values, label='8th in Standings', marker='o')

plt.xlabel('Games')
plt.ylabel('Average Rank of Team')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=4, borderaxespad=0, frameon=False)
plt.gca().yaxis.grid(True)
plt.xticks(np.arange(0, 82, step=5))
plt.yticks(np.arange(12, 0, step=-1))
plt.gca().xaxis.grid(True)
plt.title('Average Position in Standings of Playoff Teams by Game')
plt.savefig('playoff_position.png', dpi=300)
    
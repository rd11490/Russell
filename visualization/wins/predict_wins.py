import pandas as pd
import random


import MySqlDatabases.NBADatabase
from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sched = pd.read_csv('predicted_test.csv', dtype={'homeTeam': str, 'awayTeam': str})
sched['homeTeam']=sched['homeTeam'].astype(float).astype(int).astype(str)
sched['awayTeam']=sched['awayTeam'].astype(float).astype(int).astype(str)


teams = sql.runQuery("select * from nba.team_info where season = '2018-19';")
teams['teamId']=teams['teamId'].astype(int).astype(str)
teams=teams[['teamId', 'teamName']]

sched = sched.merge(teams, left_on='homeTeam', right_on='teamId').merge(teams, left_on='awayTeam', right_on='teamId', suffixes=('_home', '_away'))
results = sql.runQuery("SELECT * FROM nba.league_results where season = '2018-19'")
results['homeTeam']=results['homeTeam'].astype(int).astype(str)
results['awayTeam']=results['awayTeam'].astype(int).astype(str)



sims = []
for i in range(0, 100):
    wins = {}

    sched=sched[['homeTeam', 'teamName_home', 'awayTeam', 'teamName_away', 'predict']]

    def add_to_wins(team_id):
        if team_id in wins.keys():
            wins[team_id] += 1
        else:
            wins[team_id] = 1

    for ind,g in sched.iterrows():
        rng = random.random()

        # if g['predict'] > 0.5:
        #     rng -= 0.05
        # else:
        #     rng += 0.1

        if rng < g['predict']:
            add_to_wins(g['awayTeam'])
        else:
            add_to_wins(g['homeTeam'])
    sims.append(wins)

all_sims = pd.DataFrame(sims)
frame = pd.DataFrame(all_sims.mean()).reset_index()
frame.columns = ['teamId', 'wins']

frame = frame.merge(teams, on='teamId')

print(frame)

frame.to_csv('wins_19_20.csv', index=False)

wins_res = {}


def add_to_wins_res(team_id):
    if team_id in wins_res.keys():
        wins_res[team_id] += 1
    else:
        wins_res[team_id] = 1

for ind,r in results.iterrows():
    if r['homeWin'] > 0.5:
        add_to_wins_res(r['homeTeam'])
    else:
        add_to_wins_res(r['awayTeam'])

def add_results(row):
    row['actual'] = wins_res[row['teamId']]
    return row

frame = frame.apply(add_results, axis=1)


frame['diff'] = abs(frame['actual'] - frame['wins'])

print(frame)
print(frame['diff'].mean())

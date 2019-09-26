import pandas as pd
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2018-19"
season_type = "Playoffs"


shots_query = "select * from nba.team_scored_shots where season = '{0}' and seasontype = '{1}' and gameId in ('0041800401','0041800402')".format(season, season_type)

teams_query = "select * from nba.team_info where season = '{0}'".format(season)

shots = sql.runQuery(shots_query)
teams = sql.runQuery(teams_query)
team_names = teams[['teamId', 'teamName']].set_index('teamId').to_dict()['teamName']

shots['points'] = shots['shotValue'] * shots['shotMade']

def calculate_epps(group):
    count = group.shape[0]
    return pd.Series({'epps': group['expectedPoints'].sum() / count, 'pps': group['points'].sum() / count})

def lookup_o_team_name(row):
    row['teamName'] = team_names[int(row['offenseTeamId'])]
    return row

def lookup_d_team_name(row):
    row['teamName'] = team_names[int(row['defenseTeamId'])]
    return row

offensive = shots.groupby(by=['offenseTeamId']).apply(calculate_epps).reset_index().apply(lookup_o_team_name, axis=1)[['teamName', 'pps', 'epps']]
offensive['diff'] = offensive['pps'] - offensive['epps']
offensive = offensive.sort_values(by='epps', ascending=True)

print('offense')
print(offensive)

defensive = shots.groupby(by=['defenseTeamId']).apply(calculate_epps).reset_index().apply(lookup_d_team_name, axis=1)[['teamName', 'pps', 'epps']]
defensive['diff'] = defensive['pps'] - defensive['epps']
defensive = defensive.sort_values(by='epps', ascending=True)
print('defense')
print(defensive)



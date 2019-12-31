import pandas as pd
import numpy as np

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()
season_type = "Regular Season"

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

all_season_o = []
all_season_d = []

for season in ['2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19','2019-20']:

    query = "SELECT * FROM nba.team_scored_shots where season = '{}' and seasonType = '{}';".format(season, season_type)
    teams_query = "select * from nba.team_info where season = '{0}'".format(season)

    shots = sql.runQuery(query)
    teams = sql.runQuery(teams_query)[['teamId', 'teamName']]

    shots_3 = shots[shots['shotValue'] == 3]

    shots_3['points'] = shots_3['shotValue'] * shots_3['shotMade']

    epps_d = shots_3.groupby(by='defenseTeamId')[['expectedPoints', 'points']].mean().reset_index()

    epps_d = epps_d.merge(teams, left_on='defenseTeamId', right_on='teamId')[['teamName', 'expectedPoints', 'points']]
    epps_d['diff'] = epps_d['points'] - epps_d['expectedPoints']

    epps_d['PointsPerShotRank'] = epps_d['points'].rank(ascending=False)
    epps_d['ExpectedPointsPerShotRank'] = epps_d['expectedPoints'].rank(ascending=False)
    epps_d['DifferenceRank'] = epps_d['diff'].rank(ascending=False)
    epps_d['season'] = season

    epps_o = shots_3.groupby(by='offenseTeamId')[['expectedPoints', 'points']].mean().reset_index()

    epps_o = epps_o.merge(teams, left_on='offenseTeamId', right_on='teamId')[['teamName', 'expectedPoints', 'points']]
    epps_o['diff'] = epps_o['points'] - epps_o['expectedPoints']

    epps_o['PointsPerShotRank'] = epps_o['points'].rank(ascending=False)
    epps_o['ExpectedPointsPerShotRank'] = epps_o['expectedPoints'].rank(ascending=False)
    epps_o['DifferenceRank'] = epps_o['diff'].rank(ascending=False)
    epps_o['season'] = season

    all_season_o.append(epps_o)
    all_season_d.append(epps_d)


def print_team(group):
    print('\n')
    print(group[['Team', 'Season', 'ExpectedPointsPerShot', 'ExpectedPointsPerShotRank', 'PointsPerShot','PointsPerShotRank', 'Difference', 'DifferenceRank']])
    print('\n')

print('OFFENSE')
frame_o = pd.concat(all_season_o)
frame_o.columns = ['Team', 'ExpectedPointsPerShot', 'PointsPerShot', 'Difference','PointsPerShotRank', 'ExpectedPointsPerShotRank', 'DifferenceRank', 'Season']
frame_o.groupby(by='Team').apply(print_team)
print('\n\n\n\n\n')
print('DEFENSE')
frame_d = pd.concat(all_season_d)
frame_d.columns = ['Team', 'ExpectedPointsPerShot', 'PointsPerShot', 'Difference','PointsPerShotRank', 'ExpectedPointsPerShotRank', 'DifferenceRank', 'Season']
frame_d.groupby(by='Team').apply(print_team)

import pandas as pd
import numpy as np

from cred import MySQLConnector

sql = MySQLConnector.MySQLConnector()
all_season_o = []
all_season_d = []

for season in ['2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19', '2019-20']:
    season_type = "Regular Season"

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    query = "SELECT * FROM nba.lineup_shots where season = '{}' and seasonType = '{}';".format(season, season_type)
    teams_query = "select * from nba.team_info where season = '{0}'".format(season)
    shots = sql.runQuery(query)
    teams = sql.runQuery(teams_query)[['teamId', 'teamName']]

    # shots_3 = shots[shots['shotValue'] == 3]

    def calculate_3_pt_breakdown(group):
        num_shots = group.shape[0]

        group_3s = group[group['shotValue'] == 3]
        num_3s =  group_3s.shape[0]

        bins = group_3s['shotZone'].value_counts().to_dict()


        percent_3s = num_3s / num_shots

        percent_left_corner = bins['LeftCorner'] / num_3s
        percent_right_corner = bins['RightCorner'] / num_3s

        percent_corner = percent_right_corner + percent_left_corner

        percent_above_break_left = bins['Left27FT'] / num_3s
        percent_above_break_right = bins['Right27FT'] / num_3s

        percent_above_break = percent_above_break_left + percent_above_break_right


        percent_long_left = bins['LeftLong3'] / num_3s
        percent_long_right = bins['RightLong3'] / num_3s

        percent_long = percent_long_left + percent_long_right

        data = {
            'PercentShots3': percent_3s,

            'Percent3sLeftCorner': percent_left_corner,
            'Percent3sRightCorner': percent_right_corner,
            'Percent3sCorner': percent_corner,

            'PercentAboveBreakLeft': percent_above_break_left,
            'PercentAboveBreakRight': percent_above_break_right,
            'PercentAboveBreak': percent_above_break,

            'Percent3sLong3Left': percent_long_left,
            'Percent3sLong3Right': percent_long_right,
            'Percent3sLong3': percent_long,

        }

        return pd.Series(data = data)



    results_o = shots.groupby(by='offenseTeamId').apply(calculate_3_pt_breakdown).reset_index()

    results_o = results_o.merge(teams, left_on='offenseTeamId', right_on='teamId')
    results_o = results_o.sort_values(by=['Percent3sCorner'])

    results_o['PercentShots3Rank'] = results_o['PercentShots3'].rank(ascending=False)
    results_o['Percent3sCornerRank'] = results_o['Percent3sCorner'].rank(ascending=False)
    results_o['PercentAboveBreakRank'] = results_o['PercentAboveBreak'].rank(ascending=False)
    results_o['Percent3sLong3Rank'] = results_o['Percent3sLong3'].rank(ascending=False)
    results_o['Season'] = season
    all_season_o.append(results_o)

    results_d = shots.groupby(by='defenseTeamId').apply(calculate_3_pt_breakdown).reset_index()

    results_d = results_d.merge(teams, left_on='defenseTeamId', right_on='teamId')
    results_d = results_d.sort_values(by=['Percent3sCorner'])

    results_d['PercentShots3Rank'] = results_d['PercentShots3'].rank(ascending=False)
    results_d['Percent3sCornerRank'] = results_d['Percent3sCorner'].rank(ascending=False)
    results_d['PercentAboveBreakRank'] = results_d['PercentAboveBreak'].rank(ascending=False)
    results_d['Percent3sLong3Rank'] = results_d['Percent3sLong3'].rank(ascending=False)
    results_d['Season'] = season
    all_season_d.append(results_d)


def print_team(group):
    print('\n')
    print(group[['teamName', 'Season', 'PercentShots3', 'PercentShots3Rank', 'Percent3sCorner',
                 'Percent3sCornerRank', 'PercentAboveBreak', 'PercentAboveBreakRank', 'Percent3sLong3',
                 'Percent3sLong3Rank']])
    print('\n')

print('OFFENSE')
frame_o = pd.concat(all_season_o)
frame_o.groupby(by='teamName').apply(print_team)
print('\n\n\n\n\n')
print('DEFENSE')
frame_d = pd.concat(all_season_d)
frame_d.groupby(by='teamName').apply(print_team)


import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

season = "2018-19"

events_query = "SELECT * FROM nba.play_by_play_with_lineup where season = '{0}';".format(season)

events = sql.runQuery(events_query)

def count_orbd_shot_groups(group):
    new_group = group.sort_values(by=['timeElapsed','eventNumber'])
    group_list = list(new_group.iterrows())
    count = 0
    curr_time = 0
    playerId = None
    i = 0
    while i < len(group_list):
        time = group_list[i][1]['timeElapsed']
        if group_list[i][1]['playType'] == 'Rebound':
            curr_time = time
            playerId = group_list[i][1]['player1Id']
        if group_list[i][1]['playType'] == 'Turnover' and (group_list[i][1]['eventActionType'] == 1 or group_list[i][1]['eventActionType'] == 2):
            curr_time = time
            playerId = group_list[i][1]['player2Id']
        if (group_list[i][1]['playType'] == 'Make' or group_list[i][1]['playType'] == 'Miss') and time - curr_time < 3 and (playerId == group_list[i][1]['player1Id'] or playerId == group_list[i][1]['player1TeamId']):
            count += 1
        i += 1
    return count


out = events.groupby(by=['gameId', 'period']).apply(count_orbd_shot_groups).reset_index()
out.columns = ['gameId', 'period', 'putbacks']
print(out.head(10))

print('total shots immediately after ORBDs {}'.format(out['putbacks'].sum()))


import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

player_names_query = "select playerId, playerName from nba.player_info;"

player_names = sql.runQuery(player_names_query).drop_duplicates()

lineup_rapm = pd.read_csv("lineup_rapm.csv", dtype={'playerId': str})

player_names['playerId'] = player_names['playerId'].astype(str)

player_map = player_names.set_index('playerId').to_dict()['playerName']


def calculate_names(lineup_id):
    if lineup_id == 'default':
        return 'default'
    players = lineup_id.split('-')
    names = []
    for p in players:
        names.append(player_map[p])
    name_str = ', '.join(names)
    return name_str


lineup_rapm['players'] = lineup_rapm['lineupId'].apply(calculate_names)

lineup_rapm.to_csv('lineup_rapm_names.csv', index=False)
import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()
season = "2018-19"
seasonType = "Regular Season"


# seconds_query = "SELECT playerId, secondsPlayed FROM nba.seconds_played where season = '{}' and seasonType ='{}';".format(season, seasonType)
pbp_query = "SELECT * FROM nba.play_by_play_with_lineup where season = '{0}' and seasonType = '{1}' and gameId = '0021800001';".format(season, seasonType)
player_names_query = "select playerId, playerName from nba.player_info;".format(season)


pbp = sql.runQuery(pbp_query)
player_names = sql.runQuery(player_names_query).drop_duplicates()

pbp = pbp[pbp['gameId'] == '0021800001']

ft_pct = {}

def calculate_ft_pct(group):
    group.sort_values(['timeElapsed', 'eventNumber'], ascending=True)

    fouler = None
    cnt = 0
    for r in group.iterrows():
        if r['playType'] == 'Foul':
            fouler = r['player1Id']
        elif r['FreeThrow']:
            if fouler in ft_pct.keys():
                ft_pct[fouler].append()




groups = pbp.groupby(by=['gameId', 'period'])

print(groups.size())

groups.apply(calculate_ft_pct)
import pandas as pd

from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()


def take_last_2(string):
    return string[2:]


def take_first_2(string):
    return string[:2]


def build_season(season):
    next_season = str(season + 1)
    season_str = str(season)
    return season_str + '-' + take_last_2(next_season)


def build_season_list(season):
    seasons = []
    [start, stop] = season.split('-')

    start_int = int(start)
    stop_int = int(take_first_2(start) + stop)

    while start_int < stop_int:
        print(start_int)
        seasons.append(build_season(start_int))
        start_int += 1
    return seasons


seasons = ["2010-11", "2011-12", "2012-13", "2013-14", "2014-15", "2015-16", "2016-17", "2017-18", "2018-19", "2014-19",
           "2013-18", "2012-17", "2011-16", "2010-15", "2016-19", "2015-18", "2014-17", "2013-16", "2012-15", "2011-14",
           "2010-13"]

for season in seasons:

    season_list = build_season_list(season)

    season_str = "('" + "', '".join(season_list) + "')"

    events_query = "SELECT * FROM nba.luck_adjusted_one_way_possessions where season in ${};".format(season_str)

    player_names_query = "select playerId, playerName from nba.player_info;".format(season)

    events = sql.runQuery(events_query)
    player_names = sql.runQuery(player_names_query).drop_duplicates()

    columns = ["offensePlayer1Id", "offensePlayer2Id",
               "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id",
               "defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id",
               "defensePlayer4Id", "defensePlayer5Id", "possessions", "points"]

    events = events[columns]

    o_players = ["offensePlayer1Id", "offensePlayer2Id", "offensePlayer3Id", "offensePlayer4Id", "offensePlayer5Id"]

    d_players = ["defensePlayer1Id", "defensePlayer2Id", "defensePlayer3Id", "defensePlayer4Id", "defensePlayer5Id"]

    o_posession_dfs = []
    d_posession_dfs = []

    for p in o_players:
        p_events = events[[p, "possessions", 'points']]
        p_events.columns = ['playerId', 'o_poss', 'o_points']
        o_posession_dfs.append(p_events)

    o_dfs = pd.concat(o_posession_dfs).groupby(by=['playerId']).sum().reset_index()

    for p in d_players:
        p_events = events[[p, "possessions", 'points']]
        p_events.columns = ['playerId', 'd_poss', 'd_points']
        d_posession_dfs.append(p_events)

    d_dfs = pd.concat(d_posession_dfs).groupby(by=['playerId']).sum().reset_index()

    joined = o_dfs.merge(d_dfs, on='playerId')

    merged = player_names.merge(joined, how="inner", on='playerId')
    print(merged)
    merged['total_poss'] = merged['o_poss'] + merged['d_poss']
    merged['ORTG'] = merged['o_points'] / merged['o_poss']
    merged['DRTG'] = merged['d_points'] / merged['d_poss']
    merged['season'] = 'season'
    merged['primaryKey'] = merged['playerId'] + '_' + season

    df = merged.sort_values(by='total_poss', ascending=False)

    # sql.truncate_table(MySqlDatabases.NBADatabase.real_adjusted_four_factors_multi, MySqlDatabases.NBADatabase.NAME, "season = '{0}'".format(season))
    sql.write(merged, MySqlDatabases.NBADatabase.real_adjusted_four_factors_multi_v2, MySqlDatabases.NBADatabase.NAME)

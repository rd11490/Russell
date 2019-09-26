import pandas as pd
from fuzzywuzzy import fuzz

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

nba_data = pd.read_csv('nba_data/stats_nba_player_data.csv')


bbm_data = pd.read_csv('bbm_data/bbm_stats.csv', index_col=0)



def convert_nba_season_to_bbm_season(season_str):
    first, second = season_str.split('-')
    return int(first[0:2] + second)

def replace_team_abbr(name):
    if name == 'NJN':
        return 'BKN'
    if name == 'NOH':
        return 'NOP'
    if name == 'NOK':
        return 'NOP'
    if name =='nan':
        return 'FA'
    return name

def replace_team_name(name):
    if name == 'PHO':
        return 'PHX'
    if name == 'NOR':
        return 'NOP'
    if name =='nan':
        return 'FA'
    return name




nba_data['SEASON'] = nba_data['SEASON'].apply(convert_nba_season_to_bbm_season)


nba_data_for_join = nba_data[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'FTA', 'FGA', 'SEASON']]
nba_data_for_join['TEAM_ABBREVIATION'] = nba_data_for_join['TEAM_ABBREVIATION'].apply(replace_team_abbr)
nba_data_for_join['TEAM_ABBREVIATION'] = nba_data_for_join['TEAM_ABBREVIATION'].fillna('FA')
bbm_data_for_join = bbm_data[['ID', 'Name', 'Team', 'g', 'fta', 'fga','season']]
bbm_data_for_join['Team'] = bbm_data_for_join['Team'].apply(replace_team_name)
bbm_data_for_join['Team'] = bbm_data_for_join['Team'].fillna('FA')


joined = nba_data_for_join.merge(bbm_data_for_join, how='outer', left_on=['TEAM_ABBREVIATION','SEASON'], right_on=['Team', 'season'])


def check_names_fuzzy_match(row):
    row["name_match"] = fuzz.partial_ratio(row["Name"], row["PLAYER_NAME"]) > 70
    return row


nans = joined[joined.isnull().any(axis=1)]
print(nans)


joined=joined.dropna()
print(joined)
joined['ft_match'] = joined['fta'] == joined['FTA']

joined['fg_match'] = joined['fga'] == joined['FGA']
joined = joined[(joined['fg_match'] == True) | (joined['ft_match'] == True)]
print(joined)
joined = joined.apply(check_names_fuzzy_match, axis=1)
joined=joined[joined['name_match'] == True]
joined = joined[['PLAYER_ID', 'PLAYER_NAME', 'ID', 'Name']]
joined = joined.drop_duplicates()
joined.columns = ['NBA_ID', 'NBA_NAME', 'BBM_ID', 'BBM_NAME']
print(joined)
joined.to_csv('player_id_match.csv', index=False)

import numpy as np
import pandas as pd

import MySqlDatabases.NBADatabase
from cred import MySQLConnector
from fuzzywuzzy import fuzz

minutes = pd.read_csv('minutes_19-20.csv')
# print(minutes)

sql = MySQLConnector.MySQLConnector()


players = sql.runQuery("select playerId, playerName from nba.player_info;")
teams = sql.runQuery("select * from nba.team_info where season = '2018-19';")

teams['teamAbbreviationLower'] = teams['teamAbbreviation'].str.lower()

# print(players)

joined = minutes.merge(players, left_on='Player', right_on='playerName', how='left')

nulls = joined[pd.isnull(joined).any(axis=1)]
filled = joined[~pd.isnull(joined).any(axis=1)]
cats = []
cat_names = []
for ind, r in nulls.iterrows():
    for ind2, p in players.iterrows():
        rat = fuzz.partial_ratio(r['Player'], p['playerName'])
        if rat > 90:
            print(r['Player'], p['playerName'], rat)
            merged = r.append(p)
            cats.append(merged.to_dict())
            cat_names.append(r['Player'])
            print(merged)
            continue

fuzzy_joined = pd.DataFrame(cats)
nulls = nulls[~nulls['Player'].isin(cat_names)]
clean_minutes = pd.concat([filled, fuzzy_joined, nulls])
clean_minutes = clean_minutes.merge(teams, left_on='Team', right_on='teamAbbreviationLower')
clean_minutes.to_csv('clean_minutes.csv')



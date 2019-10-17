import pandas as pd
from cred import MySQLConnector

import matplotlib
matplotlib.use("TKAgg")
import matplotlib.pyplot as plt
import numpy as np



pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

sql = MySQLConnector.MySQLConnector()

rapm = sql.runQuery('SELECT * FROM nba.real_adjusted_four_factors;')
clusters = pd.read_csv('player_clusters.csv')

clusters =clusters[['PLAYER_NAME', 'AGE', 'PLAYER_ID', 'SEASON', 'cluster']]

merged = clusters.merge(rapm, left_on=['PLAYER_ID', 'SEASON'], right_on=['playerId', 'season'])

cols = ['LA_RAPM', 'LA_RAPM__Def', 'LA_RAPM__Off', 'RAPM', 'RAPM__Def', 'RAPM__Off',
                 'RA_EFG', 'RA_EFG__Def', 'RA_EFG__Off', 'RA_FTR', 'RA_FTR__Def', 'RA_FTR__Off', 'RA_ORBD',
                 'RA_ORBD__Def', 'RA_ORBD__Off', 'RA_TOV', 'RA_TOV__Def', 'RA_TOV__Off']
merged = merged[['cluster', 'AGE'] + cols]

groups = merged.groupby(by='cluster')
#
# for cluster, group in groups:
#     for c in cols:
#         fig = plt.figure(figsize=(15, 10))
#         plt.title('Aging Curve for: {} in Group {}'.format(c, cluster))
#         plt.scatter(group['AGE'], group[c])
#         plt.xlabel('Age')
#         plt.ylabel(c)
#         plt.yticks(np.arange(-6, 6, step=1))
#         plt.savefig('Aging Curve for: {} in Group {}'.format(c, cluster))
#         plt.close()

means = merged.groupby('AGE').mean().reset_index()

for c in cols:
    fig = plt.figure(figsize=(15, 10))
    plt.title('Aging Curve for: {}'.format(c))
    plt.scatter(merged['AGE'], merged[c])
    plt.scatter(means['AGE'], means[c], label='Average')
    plt.xlabel('Age')
    plt.ylabel(c)
    plt.legend()
    plt.yticks(np.arange(-10, 10, step=1))
    plt.xticks(np.arange(15, 45, step=5))

    plt.savefig('Aging Curve for: {}'.format(c))
    plt.close()


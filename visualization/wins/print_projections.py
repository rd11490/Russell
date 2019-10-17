import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# wins = pd.read_csv('wins_19_20.csv', index_col='index').reset_index()
# wins=wins.sort_values(by='wins')
# wins = wins[['teamName', 'wins']]
# print(wins)

proj = pd.read_csv('team_projections_18_19.csv')
team_info = pd.read_csv('team_info.csv')

proj = proj.merge(team_info, on='teamId')
cols = ['LA_RAPM_O_RTG', 'LA_RAPM_D_RTG', 'LA_RAPM_NET_RTG', 'RAPM_O_RTG', 'RAPM_D_RTG', 'RAPM_NET_RTG', 'RA_EFG_O', 'RA_EFG_D', 'RA_TOV_O', 'RA_TOV_D', 'RA_ORBD_O', 'RA_ORBD_D', 'RA_FTR_O', 'RA_FTR_D']
proj = proj[['teamName'] + cols]

for col in cols:
    proj["{}_Rank".format(col)] = proj[col].rank(ascending='_D' in col)

print(proj)
proj.to_csv('team_projections_ranked_18_19.csv', index=False)
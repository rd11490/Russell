import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# wins = pd.read_csv('wins_19_20.csv', index_col='index').reset_index()
# wins=wins.sort_values(by='wins')
# wins = wins[['teamName', 'wins']]
# print(wins)

wins = pd.read_csv('wins_19_20.csv')[['teamName', 'wins']]
wins=wins.sort_values(by='wins', ascending=False)
wins['wins'] = wins['wins'].round(1)
print(wins)
# wins.to_csv('wins_19-20_formatted.csv')

print('\n\n')
wins2 = pd.read_csv('wins_19_20_single_yr.csv')[['teamName', 'wins']]
wins2.columns = ['teamName', 'wins2']
wins2=wins2.sort_values(by='wins2', ascending=False)
wins2['wins2'] = wins2['wins2'].round(1)
print(wins2)
# wins2.to_csv('wins_19-20_formatted_single_yr.csv')
print('\n\n')

total = wins.merge(wins2, on='teamName')

total['blend'] = (0.4 * total['wins'] + 0.6 * total['wins2'])
total=total.sort_values(by='blend', ascending=False)
print(total)
total.to_csv('wins_19-20_formatted.csv')
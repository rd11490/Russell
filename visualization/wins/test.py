import pandas as pd

wins = pd.read_csv('wins_19_20.csv', index_col='index').reset_index()
wins=wins.sort_values(by='wins')
wins = wins[['teamName', 'wins']]
print(wins)
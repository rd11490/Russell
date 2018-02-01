import bs4
import pandas as pd
import urllib3
from bs4 import Comment

seasons = range(2010, 2018)

empty_columns = [5, 7]
empty_columns_data = [4, 6]

def game_index_page(id, season):
    return "https://www.basketball-reference.com/players/i/{0}/gamelog/{1}".format(id, season)


def extract_column_names(table):
    columns = [col["aria-label"] for col in table.find_all("thead")[0].find_all("th")]
    return [col for i, col in enumerate(columns) if i not in empty_columns]


def extract_rows(table):
    rows = table.find_all("tbody")[0].find_all("tr")
    return [parse_row(r) for r in rows]


def parse_row(row):
    rank = row.find_all("th")[0].string
    other_data = row.find_all("td")
    row_data = [td.string for i, td in enumerate(other_data) if i not in empty_columns_data]
    row_data.insert(0, rank)
    return row_data


http = urllib3.PoolManager()

players = pd.read_csv("PlayerList.csv")
player_ids = players[["First year", "Last year", "id"]]

player_map = {}
for index, row in player_ids.iterrows():
    player_map[row["id"]] = (row["First year"], row["Last year"])

ids = list(player_ids["id"])
columns = []
rows = []

for season in seasons:
    for id in ids:
        print(season)
        print(id)
        years = player_map[id]
        if years[0] <= season <= years[1]:
            r = http.request('GET', game_index_page(id, season))
            soup = bs4.BeautifulSoup(r.data, 'html')
            f = soup.find_all("div", {"id": "all_pgl_basic_playoffs"})
            if len(f) > 0:
                div = f[0]
                table = div.find_all(string=lambda text: isinstance(text, Comment))[0]
                new_soup = bs4.BeautifulSoup(table, 'html')
                new_table = new_soup.find_all("table")[0]
                columns = extract_column_names(new_table)
                rows = rows + extract_rows(new_table)

frame = pd.DataFrame()

for r in rows:
    frame = frame.append(pd.Series(r), ignore_index=True)

frame.columns = columns
frame.to_csv("PlayoffData.csv")

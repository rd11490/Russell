import string

import bs4
import urllib3
import pandas as pd


def player_index_page(letter):
    return "https://www.basketball-reference.com/players/{}/".format(letter)


def extract_column_names(table):
    columns = [col["aria-label"] for col in table.find_all("thead")[0].find_all("th")]
    columns.append("id")
    return columns


def extract_rows(table):
    rows = table.find_all("tbody")[0].find_all("tr")
    return [parse_row(r) for r in rows]


def parse_row(row):
    name = row.find_all("th")[0]
    other_data = row.find_all("td")
    id = name["data-append-csv"]
    player_name = name.find_all("a")[0].string

    row_data = [td.string for td in other_data]
    row_data.insert(0, player_name)
    row_data.append(id)
    return row_data


http = urllib3.PoolManager()

columns = []
rows = []

for letter in list(string.ascii_lowercase):
    print(letter)
    r = http.request('GET', player_index_page(letter))
    soup = bs4.BeautifulSoup(r.data, 'html')
    f = soup.find_all("table")
    if len(f) > 0:
        columns = extract_column_names(f[0])
        rows = rows + extract_rows(f[0])

frame = pd.DataFrame()

for r in rows:
    frame = frame.append(pd.Series(r), ignore_index=True)

frame.columns = columns
frame.to_csv("PlayerList.csv")

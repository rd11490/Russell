import bs4
import pandas as pd
import urllib3
import sqlalchemy
from sqlalchemy.dialects.mysql import *

import MySQLConnector
import MySqlDatabases.ACBDatabase

link_string_lit = "partido.php?c="


def game_index_page(day):
    return "http://www.acb.com/jv/index.php?jornada={}".format(day)


def game_stat_link(game_id):
    return "http://www.fibalivestats.com/data/{}/data.json".format(game_id)


def extract_id(link):
    return link.rsplit("=", 1)[-1]


active = True
http = urllib3.PoolManager()
day = 1
sql = MySQLConnector.MySQLConnector()
sql.drop_table(MySqlDatabases.ACBDatabase.raw_game_json, MySqlDatabases.ACBDatabase.NAME)
while active:
    print(day)
    r = http.request('GET', game_index_page(day))
    soup = bs4.BeautifulSoup(r.data, 'html')
    f = soup.find_all("a")
    links = set([extract_id(l["href"]) for l in f if "partido.php?c=" in l["href"]])
    if len(links) < 1:
        active = False
    else:
        frame = pd.DataFrame()
        for l in links:
            print(l)
            r = http.request('GET', game_stat_link(l))
            json = r.data.decode()
            frame = frame.append(pd.Series([day, l, json]), ignore_index=True)
        frame.columns = ['day', 'id', 'json']
        types = {'day': INTEGER,
                 'id': sqlalchemy.types.NVARCHAR(length=255),
                 'json': LONGTEXT}
        sql.write(frame, MySqlDatabases.ACBDatabase.raw_game_json, MySqlDatabases.ACBDatabase.NAME, dtype=types)
        day += 1


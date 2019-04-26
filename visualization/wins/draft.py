import json

import pandas as pd
import urllib3
import MySqlDatabases.NBADatabase
from cred import MySQLConnector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

header_data = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Referer': 'stats.nba.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}

http = urllib3.PoolManager()
sql = MySQLConnector.MySQLConnector()


def build_url(season):
    return "https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season={}&TeamID=0&TopX=".format(season)

def extract_data(url):
    print(url)
    r = http.request('GET', url, headers=header_data)
    resp = json.loads(r.data)
    results = resp['resultSets'][0]
    headers = results['headers']
    rows = results['rowSet']
    frame = pd.DataFrame(rows)
    frame.columns = headers
    return frame

for season in range(2012, 2019):
    season_str = str(season) + "-" + str(season-2000+1)
    draft = extract_data(build_url(season))
    draft["primaryKey"] = draft["PERSON_ID"]
    draft["SEASON"] = season_str


    # sql.truncate_table(MySqlDatabases.NBADatabase.draft, MySqlDatabases.NBADatabase.NAME, "season = '{0}'".format(season_str))
    sql.write(draft, MySqlDatabases.NBADatabase.draft, MySqlDatabases.NBADatabase.NAME)


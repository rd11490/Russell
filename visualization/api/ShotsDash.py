import json

import pandas as pd
import urllib3

players = [1628369, 1627759,202954,202681,1626179,202694,202330,1628464,1627824,203935,1628400,201143,203382]

def build_ulr(player):
    return "https://stats.nba.com/stats/playerdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID={0}&Season=2018-19&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=".format(player)


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

def extract_data(url, player):
    print(url)
    r = http.request('GET', url, headers=header_data)
    resp = json.loads(r.data)
    results = resp['resultSets'][4]
    headers = results['headers']
    headers.append("player")
    rows = results['rowSet']
    frame = pd.DataFrame(rows)
    frame["player"] = player
    frame.columns = headers
    return frame

frames = []

for player in players:
    url = build_ulr(player)
    data = extract_data(url, player)
    frames.append(data)

out = pd.concat(frames)
out.to_csv("CsDefenderDist.csv")








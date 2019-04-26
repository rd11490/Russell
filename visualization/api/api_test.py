import pandas as pd
import requests


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

url = "https://stats.nba.com/stats/shotchartdetail/?leagueId=00&season=2018-19&seasonType=Regular+Season&teamId=1610612738&playerId=1628369&gameId=&outcome=&location=&month=0&seasonSegment=&dateFrom=&dateTo=&opponentTeamId=0&vsConference=&vsDivision=&position=&playerPosition=&rookieYear=&gameSegment=&period=0&lastNGames=0&clutchTime=&aheadBehind=&pointDiff=&rangeType=0&startPeriod=1&endPeriod=10&startRange=0&endRange=2147483647&contextFilter=&contextMeasure=PTS"

print(url)
r = requests.get(url, headers=header_data)
resp = r.json()
results = resp['resultSets'][0]
headers = results['headers']
rows = results['rowSet']
frame = pd.DataFrame(rows)
frame.columns = headers

frame.to_csv("test.csv")







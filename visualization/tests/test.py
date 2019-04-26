import requests
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

shot_chart_url = 'https://stats.nba.com/stats/shotchartdetail/?leagueId=00&season=2018-19&seasonType=Regular+Season&teamId=1610612738&playerId=1628369&gameId=&outcome=&location=&month=0&seasonSegment=&dateFrom=&dateTo=&opponentTeamId=0&vsConference=&vsDivision=&position=&playerPosition=&rookieYear=&gameSegment=&period=0&lastNGames=0&clutchTime=&aheadBehind=&pointDiff=&rangeType=0&startPeriod=1&endPeriod=10&startRange=0&endRange=2147483647&contextFilter=&contextMeasure=PTS'

print(shot_chart_url)

# Get the webpage containing the data
response = requests.get(shot_chart_url)
print(response)
# Grab the headers to be used as column headers for our DataFrame
headers = response.json()['resultSets'][0]['headers']
# Grab the shot chart data
shots = response.json()['resultSets'][0]['rowSet']

shot_df = pd.DataFrame(shots, columns=headers)


sns.set_style("white")
sns.set_color_codes()
plt.figure(figsize=(12,11))
plt.scatter(shot_df.LOC_X, shot_df.LOC_Y)
plt.show()

right = shot_df[shot_df.SHOT_ZONE_AREA == "Right Side(R)"]
plt.figure(figsize=(12,11))
plt.scatter(right.LOC_X, right.LOC_Y)
plt.xlim(-300,300)
plt.ylim(-100,500)
plt.show()

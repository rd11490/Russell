import requests
import pandas as pd
href = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2019/league/00_full_schedule_week.json'
resp = requests.get(href)
resp_json = resp.json()
schedule = []
for month in resp_json['lscd']:
    games = month['mscd']['g']
    for game in games:
        if game['gweek'] is not None:
            schedule.append({
                'gameId': game['gid'],
                'homeTeam': game['h']['tid'],
                'awayTeam': game['v']['tid']
            })


schedule = pd.DataFrame(schedule)
schedule.to_csv('schedule.csv', index=False)
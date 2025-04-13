
#%%

import requests
import pandas as pd

BASE_URL = "https://api.jolpi.ca/ergast/f1" #GitHub : https://github.com/jolpica/jolpica-f1/
START_YEAR = 2000
END_YEAR = 2024


def fetch(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()


def extract_race_results():
    all_results = []

    for year in range(START_YEAR, END_YEAR + 1):
        print(year)
        schedule_url = f"{BASE_URL}/{year}.json"
        schedule_data = fetch(schedule_url)
        if not schedule_data:
            continue

        races = schedule_data['MRData']['RaceTable']['Races']
        for race in races: #Extract basic race performance data
            round_no = race['round']
            results_url = f"{BASE_URL}/{year}/{round_no}/results.json"
            results_data = fetch(results_url)
            if not results_data:
                continue

            race_info = results_data['MRData']['RaceTable']['Races']
            if not race_info:
                continue

            for result in race_info[0]['Results']:
                driver = result['Driver']
                constructor = result['Constructor']
                all_results.append({
                    "season": year,
                    "round": round_no,
                    "raceName": race_info[0]['raceName'],
                    "circuitId": race_info[0]['Circuit']['circuitId'],
                    "date": race_info[0]['date'],
                    "driverId": driver['driverId'],
                    "driverName": f"{driver['givenName']} {driver['familyName']}",
                    "constructorId": constructor['constructorId'],
                    "constructorName": constructor['name'],
                    "grid": int(result.get('grid', 0)),
                    "position": int(result['position']),
                    "positionText": result['positionText'],
                    "status": result['status'],
                    "points": float(result['points']),
                    "laps": int(result.get('laps', 0)),
                    "time": result.get('Time', {}).get('time', None),
                    "rank": result.get('FastestLap', {}).get('rank', None),
                    "fastestLapTime": result.get('FastestLap', {}).get('Time', {}).get('time', None)
                })

    df_results = pd.DataFrame(all_results)
    df_results.to_csv("Data/race_results_2000_2024.csv", index=False)


if __name__ == "__main__":
    extract_race_results()

# %%

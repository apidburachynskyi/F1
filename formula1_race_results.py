import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib import cm
import fastf1 as ff1
from fastf1.core import Laps
from fastf1 import utils
from fastf1 import plotting
plotting.setup_mpl()
from timple.timedelta import strftimedelta
import requests
import timeit

start = timeit.default_timer()

pd.set_option('display.max_columns', None)

ff1.Cache.enable_cache('Cache')

session_list = ['Race']

year_list = []
for i in range(1950,2021+1):
    year_list.append(i)

practice_df = pd.DataFrame()

for year in year_list:

    season = ff1.get_event_schedule(year)

    eventname_list = season['EventName'].tolist()

    for eventname in eventname_list:
        for session in session_list:
            print(session)
            try:
                session_data = ff1.get_session(year,eventname,session)

            except:
                print(f"Session Data does not exist for {year}, {eventname}, {session}")
                continue
            try:
                session_data.load()
                session_results = session_data.results
                session_results["year"] = year
                session_results["session"] = session
                session_results["event"] = eventname

                session_event = session_data.event
                session_results["EventDate"] = session_event.EventDate

                print(pd.DataFrame(session_results).shape)

            except:
                print(f"Lap Data does not exist for {year}, {eventname}, {session}")
                continue

            if eventname == eventname_list[0] and session == session_list[0] and year == year_list[0]:
                race_results_df = pd.DataFrame(session_results)
            else:
                race_results_df = pd.concat([race_results_df,pd.DataFrame(session_results)])

            print(race_results_df.shape)

race_results_df.to_csv('csv/race_bronze_df.csv')

stop = timeit.default_timer()

print('Time: ', stop - start)

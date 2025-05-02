#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from tqdm import tqdm
import pathlib
from datetime import datetime

station = input("Station-stationtype: ")


#station = "LUBA-rr"


file_path = station + '.csv'
data = pd.read_csv("outputs/" + file_path)

frame = pd.DataFrame(data)
frame["date"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[0]
frame["time"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[1]
frame["timestamp"] = pd.to_datetime(frame["logtime"])
frame["timestamp"] = frame["timestamp"] - pd.to_timedelta(8, unit='h')

csv_file_set_utc = set()

# UTC
for row in tqdm(range(len(frame.recno))):
    csv_file = pathlib.Path(
        "outputs/" + "monthly-" + str(frame.loc[row]["timestamp"])[:7] + "-" + station + "-utc" + ".csv")
    csv_file_set_utc.add(csv_file)

    datarow = frame.loc[row].to_frame().T
    # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
    datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)

#print("DONE")

# UTC
for csv in tqdm(csv_file_set_utc):
    data_prev = pd.read_csv(csv)
    frame2 = pd.DataFrame(data_prev)

    frame2["timestamp"] = pd.to_datetime(frame2["timestamp"])
    frame2.set_index('timestamp', inplace=True)
    frame3 = frame2.copy()
    del frame2["recno"]
    del frame2["nodeid"]
    del frame2["logtime"]
    del frame2["logtype"]
    del frame2["unit"]
    del frame2["date"]
    del frame2["time"]

    if frame3['logtype'].iloc[0] == 'rr':
        del frame2["wl_data"]
        frame2 = frame2.resample('10min').sum()
    else:
        del frame2["value"]
        frame2 = frame2.resample('10min').mean()

    frame2['logtime'] = [item.strftime("%Y-%m-%d %H:%M:%S") for item in frame2.index]
    frame2["date"] = frame2["logtime"].str.split(pat=" ", n=1, expand=True)[0]
    frame2["time"] = frame2["logtime"].str.split(pat=" ", n=1, expand=True)[1]
    del frame2['logtime']

    if frame3['logtype'].iloc[0] == 'rr':
        # del frame2["wl_data"]
        # frame3 = frame2.pivot(index='time', columns='date', values='value')
        frame2['time'] = pd.to_datetime(frame2["time"], format='%H:%M:%S')
        frame2['time'] = [item + pd.Timedelta(minutes=10) for item in frame2['time']]
        frame2['time'] = [item.time() for item in frame2['time']]
        frame2['time'] = [item.strftime("%H:%M:%S") for item in frame2['time']]
        frame2['time'] = [item.replace('00:00:00', '24:00:00') for item in frame2['time']]

        frame4 = frame2.pivot_table(index='time', columns='date', values='value', aggfunc='first')
        frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'utc-monthly-table-')))
    else:
        # del frame2["value"]
        # frame3 = frame2.pivot(index='time', columns='date', values='wl_data')
        frame2['time'] = pd.to_datetime(frame2["time"], format='%H:%M:%S')
        frame2['time'] = [item + pd.Timedelta(minutes=10) for item in frame2['time']]
        frame2['time'] = [item.time() for item in frame2['time']]
        frame2['time'] = [item.strftime("%H:%M:%S") for item in frame2['time']]
        frame2['time'] = [item.replace('00:00:00', '24:00:00') for item in frame2['time']]

        frame4 = frame2.pivot_table(index='time', columns='date', values='wl_data', aggfunc='first')
        frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'utc-monthly-table-')))

print('DONE')

"""
for csv in tqdm(csv_file_set):
    data_prev = pd.read_csv(csv)
    frame2 = pd.DataFrame(data_prev)
    del frame2["nodeid"]
    #del frame2["logtype"]
    del frame2["unit"]
    del frame2["logtime"]
    del frame2["recno"]
    if frame2.loc[0]["logtype"] == 'rr':
        del frame2["wl_data"]
        #frame3 = frame2.pivot(index='time', columns='date', values='value')
        frame3 = frame2.pivot_table(index='time', columns='date', values='value', aggfunc='first')
        frame3.to_csv(pathlib.Path(str(csv).replace('monthly-', 'monthly/monthly-table-')))
    else:
        del frame2["value"]
        #frame3 = frame2.pivot(index='time', columns='date', values='wl_data')
        frame3 = frame2.pivot_table(index='time', columns='date', values='wl_data', aggfunc='first')
        frame3.to_csv(pathlib.Path(str(csv).replace('monthly-', 'monthly/monthly-table-')))
print('done')

"""


"""
def summarize(frame, csv_file):
    data_prev = pd.read_csv(csv_file)     
    frame2 = pd.DataFrame(data_prev)
    del frame2["nodeid"]
    del frame2["logtype"]
    del frame2["unit"]
    del frame2["logtime"]
    del frame2["recno"]
    if frame.loc[row]["logtype"] == 'rr':
        del frame2["wl_data"]
        frame3 = frame2.pivot_table(index='time', columns='date', values='value', aggfunc='first')
        frame3.to_csv(csv_file_prev)
    else:
        del frame2["value"]
        frame3 = frame2.pivot_table(index='time', columns='date', values='wl_data', aggfunc='first')
        frame3.to_csv(csv_file)

summarize(frame, csv_file)
print("done")

list_times = [f'{i:02d}:{j:02d}:00' for i in range(24) for j in range(0, 60, 10)]
print(list_times)
from datetime import datetime
dates = pd.to_datetime(list_times, format='%H:%M:%S')
dates.time
"""
#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from tqdm import tqdm
import pathlib

station = input("Station-stationtype: ")


#station = "LUBA-rr"


file_path = station + '.csv'
data = pd.read_csv("outputs/" + file_path)

frame = pd.DataFrame(data)
frame["date"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[0]
frame["time"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[1]

csv_file_set = set()

for row in tqdm(range(len(frame.recno))):
    csv_file = pathlib.Path("outputs/" + "monthly-" + frame.loc[row]["date"][:7] + "-" + station + ".csv")
    csv_file_set.add(csv_file)

    datarow = frame.loc[row].to_frame().T
    # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
    datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)

print("DONE")



for csv in tqdm(csv_file_set):
    print(csv)
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
        frame3.to_csv(csv)
    else:
        del frame2["value"]
        #frame3 = frame2.pivot(index='time', columns='date', values='wl_data')
        frame3 = frame2.pivot_table(index='time', columns='date', values='wl_data', aggfunc='first')
        frame3.to_csv(csv)
print('done')




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
"""


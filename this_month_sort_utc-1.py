#!/usr/bin/env python
# coding: utf-8

"""

Script to sort data(waterlevel, raingauge) of various station in Abra River Basin into monthly UTC (8AM-8AM)

Author: Kaizer Macni


SW: 1.2
"""

import numpy as np
import pandas as pd
from tqdm import tqdm
import pathlib
from datetime import datetime
import os
import glob

#import warnings
#warnings.filterwarnings("ignore")  # Ignores all warnings

os.mkdir('outputs/monthly-table')
os.mkdir("outputs/monthly-table/waterlevel")
os.mkdir("outputs/monthly-table/rainfall")

def list_csv_files(folder_path):
    """
    Lists all CSV files in a given folder path.

    Args:
    folder_path (str): The path to the folder.

    Returns:
    list: A list of strings, each string is a path to a CSV file.
    """
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    return csv_files


folder_path = "outputs/"
csv_file_list = list_csv_files(folder_path)
print(csv_file_list)

#station = input("Station-stationtype: ")


#station = "LUBA-rr"

now = datetime.now()
#now = datetime.strptime("2025-04-01 08:05:00", "%Y-%m-%d %H:%M:%S")
print(now)
#file_path = station + '.csv'
#data = pd.read_csv("outputs/" + file_path)

for csv_orig in csv_file_list:
    station = csv_orig.replace('outputs/', '').replace('.csv', '')

    data = pd.read_csv(csv_orig)

    frame = pd.DataFrame(data)
    frame["date"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[0]
    frame["time"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[1]
    frame["timestamp"] = pd.to_datetime(frame["logtime"])
    frame["timestamp"] = frame["timestamp"] - pd.to_timedelta(8, unit='h')
    frame["timestamp1"] = [item.strftime("%Y-%m") for item in frame["timestamp"]] # TODO ADDED CODE

    csv_file_set_utc = set()
    csv_file_set_utc_1 = set()

    # UTC
    for row in tqdm(range(len(frame.recno))):
        if now.strftime("%d") == '01':
            #if (now - pd.to_timedelta(9, unit='h')).strftime("%Y-%m") == frame.loc[row]['timestamp1'][:7] or now.strftime("%Y-%m") == frame.loc[row]['timestamp1'][:7]:
            if (now - pd.to_timedelta(9, unit='h')).strftime("%Y-%m") == frame.loc[row]['timestamp1'][:7]:
                # print(frame.loc[row]['timestamp1'][:7])
                # csv_file = pathlib.Path(
                #    "outputs/" + "monthly-" + str(frame.loc[row]["timestamp"])[:7] + "-" + station + "-utc" + ".csv")
                csv_file = pathlib.Path(
                    "outputs/" + "monthly-" + station + "-" + str(frame.loc[row]["timestamp"])[:7] + "-utc" + ".csv")
                # csv_file = pathlib.Path(
                #    "outputs/" + station + "-monthly-" + str(frame.loc[row]["timestamp"])[:7] + "-utc" + ".csv")

                csv_file_set_utc.add(csv_file)

                datarow = frame.loc[row].to_frame().T
                # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
                datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)
                # datarow.to_csv(pathlib.Path(str(csv_file).replace('outputs/', 'outputs/monthly/')), mode='a', header=not csv_file.exists(), index=False)

                # print("DONE")



        else:
            if now.strftime("%Y-%m") == frame.loc[row]['timestamp1'][:7]: # TODO ADDED CODE
                #print(frame.loc[row]['timestamp1'][:7])
                #csv_file = pathlib.Path(
                #    "outputs/" + "monthly-" + str(frame.loc[row]["timestamp"])[:7] + "-" + station + "-utc" + ".csv")
                csv_file = pathlib.Path(
                    "outputs/" + "monthly-" + station + "-" + str(frame.loc[row]["timestamp"])[:7] + "-utc" + ".csv")
                #csv_file = pathlib.Path(
                #    "outputs/" + station + "-monthly-" + str(frame.loc[row]["timestamp"])[:7] + "-utc" + ".csv")

                csv_file_set_utc.add(csv_file)

                datarow = frame.loc[row].to_frame().T
                # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
                datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)
                #datarow.to_csv(pathlib.Path(str(csv_file).replace('outputs/', 'outputs/monthly/')), mode='a', header=not csv_file.exists(), index=False)

        #print("DONE")

    # UTC
    for csv in tqdm(csv_file_set_utc):
        data_prev = pd.read_csv(csv)
        frame2 = pd.DataFrame(data_prev)

        frame2["timestamp"] = pd.to_datetime(frame2["timestamp"])
        frame2.set_index('timestamp', inplace=True)
        frame3 = frame2.copy()
        del frame2["recno"]
        del frame2["timestamp1"] # TODO ADDED CODE
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
            frame2['time'] = [item.replace('00:00:00', '24:00:00').replace(':', '')[:4] + 'Z' for item in frame2['time']]

            frame4 = frame2.pivot_table(index='time', columns='date', values='value', aggfunc='first')
            frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'monthly-table-')), index_label='time')
            #frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'utc-monthly-table-').replace('outputs/', 'outputs/monthly-table-utc/')))
        else:
            # del frame2["value"]
            # frame3 = frame2.pivot(index='time', columns='date', values='wl_data')
            frame2['time'] = pd.to_datetime(frame2["time"], format='%H:%M:%S')
            frame2['time'] = [item + pd.Timedelta(minutes=10) for item in frame2['time']]
            frame2['time'] = [item.time() for item in frame2['time']]
            frame2['time'] = [item.strftime("%H:%M:%S") for item in frame2['time']]
            frame2['time'] = [item.replace('00:00:00', '24:00:00').replace(':', '')[:4] + 'Z' for item in frame2['time']]

            frame4 = frame2.pivot_table(index='time', columns='date', values='wl_data', aggfunc='first')
            frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'monthly-table-')), index_label='time')
            #frame4.to_csv(pathlib.Path(str(csv).replace('monthly-', 'utc-monthly-table-').replace('outputs/', 'outputs/monthly-table-utc/')))

        csv_file_set_utc_1.add(pathlib.Path(str(csv).replace('monthly-', 'monthly-table-')))


    # UTC
    for csv in tqdm(csv_file_set_utc_1):
        data_prev = pd.read_csv(csv)
        frame5 = pd.DataFrame(data_prev)

        # create a list of days
        m_day = [frame5.columns[1][:8] + str(x + 1) for x in range(31)]
        m_day = pd.to_datetime(m_day, errors='coerce')
        m_day = m_day.dropna()
        m_day = [item.strftime("%Y-%m-%d") for item in m_day]

        # create a list of time with interval of 10mins
        list_10min = [f'{i:02d}:{j:02d}:00' for i in range(24) for j in range(0, 60, 10)]
        list_10min = pd.to_datetime(list_10min, format='%H:%M:%S')
        list_10min = [item + pd.Timedelta(minutes=10) for item in list_10min]
        list_10min = [item.time() for item in list_10min]
        list_10min = [item.strftime("%H:%M:%S") for item in list_10min]
        list_10min = [item.replace('00:00:00', '24:00:00').replace(':', '')[:4] + 'Z' for item in list_10min]

        frame6 = pd.DataFrame(index=list_10min, columns=m_day)  # create an empty dataframe with no values

        frame5.set_index('time', inplace=True)

        # temporary
        # TODO
        for time in frame5.index:
            for date in frame5.columns:
                # frame6.loc[x].loc[y] = frame5.loc[x].loc[y]
                frame6.at[time, date] = frame5.loc[time].loc[date]

        # frame7 = frame5.copy()
        # frame8 = frame6.copy()

        # frame7 = frame6.copy()
        # frame7 = frame7.merge(frame5, how='right').set_index(frame6.index)
        # frame7 = frame7.merge(frame5, how='right').reindex(frame6.index)
        sum_lst = []
        mean_lst = []
        max_lst = []
        min_lst = []
        for date in frame6.columns:
            sum_lst.append(frame6[date].sum())
            mean_lst.append(frame6[date].mean())
            max_lst.append(frame6[date].max())
            min_lst.append(frame6[date].min())
        frame6.loc['SUM'] = sum_lst
        frame6.loc['MEAN'] = mean_lst
        frame6.loc['MAX'] = max_lst
        frame6.loc['MIN'] = min_lst

        #frame6.to_csv(csv, index_label='time')
        #frame6.to_csv(pathlib.Path(str(csv).replace('outputs/', 'outputs/monthly-table')), index_label='time'))

        if 'wl' in str(csv):
            frame6.to_csv(pathlib.Path(str(csv).replace('outputs/', 'outputs/monthly-table/waterlevel/')), index_label='time')
        else:
            frame6.to_csv(pathlib.Path(str(csv).replace('outputs/', 'outputs/monthly-table/rainfall/')), index_label='time')

print('DONE')

"""
csv = 'outputs/monthly-table/rainfall/monthly-table-rr-DOLORES-1-2024-09-utc.csv'
data = pd.read_csv(csv)
df = pd.DataFrame(data)
df.set_index('time', inplace=True)
df.drop(labels='SUM', inplace=True)
df.drop(labels='MEAN', inplace=True)
df.drop(labels='MAX', inplace=True)
df.drop(labels='MIN', inplace=True)
time_utc = list(df.index)

# create a list of time with interval of 10mins
list_10min = [f'{i:02d}:{j:02d}:00' for i in range(24) for j in range(0, 60, 10)]
list_10min = pd.to_datetime(list_10min, format='%H:%M:%S')
#list_10min = [item + pd.Timedelta(minutes=10) for item in list_10min]
#list_10min = [item.time() for item in list_10min]
#list_10min = [item.strftime("%H:%M:%S") for item in list_10min]
#list_10min = [item.replace('00:00:00', '24:00:00').replace(':', '')[:4] + 'Z' for item in list_10min]
df['time_10mins'] = list_10min
df = df.resample('1h', on='time_10mins').sum()
#df.index
#year_month = df.columns[0][:8].replace('/', '-')

year_month = df.columns[0][:8]

if 'BANTAY' in csv:
    title = 'BANTAY RAINFALL'
    #title = 'BANTAY WATERLEVEL' if 'wl' in csv else 'BANTAY RAINFALL'
elif 'DOLORES' in csv:
    title = 'DOLORES RAINFALL'
    # title = 'DOLORES WATERLEVEL' if 'wl' in csv else 'DOLORES RAINFALL'

elif 'LUBA' in csv:
    title = 'LUBA RAINFALL'
    # title = 'LAPAZ WATERLEVEL' if 'wl' in csv else 'LAPAZ RAINFALL'

elif 'QUIRINO' in csv:
    title = 'QUIRINO RAINFALL'
    # title = 'SAN JULIAN WATERLEVEL' if 'wl' in csv else 'SAN JULIAN RAINFALL'
else:
    title = 'RAINFALL'
    #alert += 0
    #alarm += 0
    #critical += 0
    #title = 'SAN JULIAN WATERLEVEL'

# create the figure and axes
fig1, axes1 = plt.subplots(6, int(np.ceil(len(df.columns) / 6)), figsize=(18, 18), sharex=True, sharey=True)

# unpack all the axes subplots
axe1 = axes1.ravel()

# plt.title("BANTAY WATERLEVEL")
# plt.ylabel("Calorie Burnage")

# assign the plot to each subplot in axe
for i, c in enumerate(df.columns):
    #if 'wl' in csv:
    #axe[i].plot(time_utc, alert, label=c)
    df[c].plot(ax=axe1[i], kind='bar', color="blue")
    #df[c].bar(ax=axe1[i])
    # axe[i].legend(fontsize='small', loc='upper right')
    axe1[i].legend(loc='upper center', bbox_to_anchor=(0.5, 1.15))
    #else:
    #   pass
        # rr use bargraph

plt.suptitle(title, y=0.92, fontsize='xx-large')
plt.show()
fig1.savefig('outputs/monthly-table/rainfall/svg/' + year_month + title + '.svg')
fig1.savefig('outputs/monthly-table/rainfall/png/' + year_month + title + '.png', dpi=400, bbox_inches='tight')
#plt.close()
"""

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


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
from tqdm import tqdm
import os
import glob

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


global colors
def find_color(v):
    global colors
    if v > 30:
        colors = "purple"   # torrential
    elif 15 < v < 31:
        colors = "red"      # intense
    elif 7.5 < v < 16:
        colors = "orange"   # heavy
    elif 2.4 < v < 7.6:
        colors = "green"    # moderate
    elif v < 2.5:
        colors = "blue"     # light
    else:
        colors = "blue"

    return colors

folder_path_wl = "outputs/monthly-table/waterlevel"
folder_path_rr = "outputs/monthly-table/rainfall"
wl_file_list = list_csv_files(folder_path_wl)
rr_file_list = list_csv_files(folder_path_rr)
print(wl_file_list)
print(rr_file_list)

os.mkdir("outputs/monthly-table/waterlevel/png")
os.mkdir("outputs/monthly-table/waterlevel/svg")


os.mkdir("outputs/monthly-table/rainfall/png")
os.mkdir("outputs/monthly-table/rainfall/svg")


for csv in tqdm(wl_file_list):
    data = pd.read_csv(csv)
    df = pd.DataFrame(data)
    df.set_index('time', inplace=True)
    df.drop(labels='SUM', inplace=True)
    df.drop(labels='MEAN', inplace=True)
    df.drop(labels='MAX', inplace=True)
    df.drop(labels='MIN', inplace=True)
    time_utc = list(df.index)
    alert = np.zeros(len(df))
    alarm = np.zeros(len(df))
    critical = np.zeros(len(df))
    #year_month = df.columns[0][:8].replace('/', '-')
    year_month = df.columns[0][:8]

    if 'BANTAY' in csv:
        alert += 6.5
        alarm += 7.5
        critical += 9.5
        title = 'BANTAY WATERLEVEL'
        #title = 'BANTAY WATERLEVEL' if 'wl' in csv else 'BANTAY RAINFALL'
    elif 'DOLORES' in csv:
        alert += 53
        alarm += 54
        critical += 56
        title = 'DOLORES WATERLEVEL'
        # title = 'DOLORES WATERLEVEL' if 'wl' in csv else 'DOLORES RAINFALL'

    elif 'LAPAZ' in csv:
        alert += 39
        alarm += 40
        critical += 42
        title = 'LAPAZ WATERLEVEL'
        # title = 'LAPAZ WATERLEVEL' if 'wl' in csv else 'LAPAZ RAINFALL'

    elif 'SANJULIAN' in csv:
        alert += 0
        alarm += 0
        critical += 0
        title = 'SAN JULIAN WATERLEVEL'
        # title = 'SAN JULIAN WATERLEVEL' if 'wl' in csv else 'SAN JULIAN RAINFALL'
    else:
        pass
        #alert += 0
        #alarm += 0
        #critical += 0
        #title = 'SAN JULIAN WATERLEVEL'

    # create the figure and axes
    fig, axes = plt.subplots(6, int(np.ceil(len(df.columns) / 6)), figsize=(18, 18), sharex=True, sharey=True)

    # unpack all the axes subplots
    axe = axes.ravel()

    # plt.title("BANTAY WATERLEVEL")
    # plt.ylabel("Calorie Burnage")

    # assign the plot to each subplot in axe
    for i, c in enumerate(df.columns):
        #if 'wl' in csv:
        #axe[i].plot(time_utc, alert, label=c)
        axe[i].plot(alert, color="yellow", linestyle="dashed")
        axe[i].plot(alarm, color="orange", linestyle="dashed")
        axe[i].plot(critical, color="red", linestyle="dashed")
        df[c].plot(ax=axe[i], color="blue")
        # axe[i].legend(fontsize='small', loc='upper right')
        axe[i].legend(loc='upper center', bbox_to_anchor=(0.5, 1.15))
        #else:
        #   pass
            # rr use bargraph

    plt.suptitle(title, y=0.92, fontsize='xx-large')
    # plt.show()
    fig.savefig('outputs/monthly-table/waterlevel/svg/' + year_month + title + '.svg')
    fig.savefig('outputs/monthly-table/waterlevel/png/' + year_month + title + '.png', dpi=400, bbox_inches='tight')
    plt.close()





for csv in tqdm(rr_file_list):
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
    df['time_10mins'] = list_10min
    df = df.resample('1h', on='time_10mins').sum()
    # create a list of time with interval of 1hour
    list_1H = [f'{i + 1:02d}:00' for i in range(24)]
    list_1H = [item.replace('00:00:00', '24:00:00').replace(':', '')[:4] + 'Z' for item in list_1H]

    df['time_utc'] = list_1H
    df.set_index('time_utc', inplace=True)

    year_month = df.columns[0][:8]

    if 'BANTAY' in csv:
        title = 'BANTAY RAINFALL'
        # title = 'BANTAY WATERLEVEL' if 'wl' in csv else 'BANTAY RAINFALL'
    elif 'DOLORES' in csv:
        title = 'DOLORES RAINFALL'
        # title = 'DOLORES WATERLEVEL' if 'wl' in csv else 'DOLORES RAINFALL'

    elif 'LUBA' in csv:
        title = 'LUBA RAINFALL'
        # title = 'LAPAZ WATERLEVEL' if 'wl' in csv else 'LAPAZ RAINFALL'

    elif 'QUIRINO' in csv:
        title = 'QUIRINO RAINFALL'
        # title = 'SAN JULIAN WATERLEVEL' if 'wl' in csv else 'SAN JULIAN RAINFALL'

    elif 'LAGAYAN' in csv:
        title = 'LAGAYAN RAINFALL'

    else:
        title = 'RAINFALL'

    # create the figure and axes
    fig1, axes1 = plt.subplots(6, int(np.ceil(len(df.columns) / 6)), figsize=(18, 18), sharex=True, sharey=True)

    # unpack all the axes subplots
    axe1 = axes1.ravel()


    # assign the plot to each subplot in axe
    for i, c in enumerate(df.columns):

        colors = [find_color(v) for v in df[c]]
        df[c].plot(ax=axe1[i], kind='bar', color=colors)
        axe1[i].legend(loc='upper center', bbox_to_anchor=(0.5, 1.15))


    plt.suptitle(title, y=0.92, fontsize='xx-large')
    # plt.xticks(ticks=df.index[::10])
    plt.locator_params(axis='x', tight=True, nbins=10)
    #plt.show()
    fig1.savefig('outputs/monthly-table/rainfall/svg/' + year_month + title + '.svg')
    fig1.savefig('outputs/monthly-table/rainfall/png/' + year_month + title + '.png', dpi=400, bbox_inches='tight')
    plt.close()

print("DONE")
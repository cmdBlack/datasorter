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

folder_path = "outputs/waterlevel"
csv_file_list = list_csv_files(folder_path)
print(csv_file_list)

os.mkdir("outputs/waterlevel/png")
os.mkdir("outputs/waterlevel/svg")

for csv in tqdm(csv_file_list):
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
    fig.savefig('outputs/waterlevel/svg/' + year_month + title + '.svg')
    fig.savefig('outputs/waterlevel/png/' + year_month + title + '.png', dpi=400, bbox_inches='tight')
    plt.close()

print("DONE")
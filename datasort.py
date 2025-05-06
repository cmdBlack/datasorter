"""

Script to sort data(waterlevel, raingauge) of various station in Abra River Basin

Author: Kaizer Macni

"""




import numpy as np
import pandas as pd
import pathlib
#from pandas import Series, DataFrame
from tqdm import tqdm


station_number = {"BANTAY": "639992238167", 
                  "LUBA-1": "639617841939", 
                  "LUBA": "639998853840",
                  "DOLORES-1": "639616101598",
                  "DOLORES": "639998853841",
                  "LAPAZ-1": "639617138156",
                  "LAPAZ": "639992238148",
                  "SANJULIAN": "639088872285",
                  "LAGAYAN": "639088872279",
                  "QUIRINO": "639992238150"
                 }

wl_datum = {"BANTAY": 29.63,
            "DOLORES": 61.84,
            "LAPAZ": 50.49,
            "SANJULIAN": 0,
            "LUBA-1": 100,
            "DOLORES-1": 61.84,
            "LAPAZ-1": 50.49
            }

error_list = ["wltimeout", "wlerror"]
data = pd.read_csv("datalogs.csv", delimiter=';')
data = data.replace(error_list, np.nan)
frame = pd.DataFrame(data)


del frame["description"]
del frame["outbound"]
del frame["recvtime"]
del frame["seqno"]


frame = frame.dropna()
frame2 = frame.copy()
frame2["wl_data"] = 100
frame2.drop_duplicates()

def find_key_by_value(dictionary, target_value):
    """
    Finds the key in a dictionary associated with a given value.

    Args:
        dictionary: The dictionary to search.
        target_value: The value to find.

    Returns:
        The key if found, otherwise None.
    """
    for key, value in dictionary.items():
        if value == target_value:
            return key
    return None


for row in tqdm(frame2.index):
    # for row in tqdm(range(len(frame2.recno))):
    # row += 1
    # print(row)
    # print(type(frame.loc[row-1]))
    # print(frame.loc[row-1]["nodeid"])
    # print(frame.loc[row-1]["logtype"])
    # print(frame.loc[row-1]["nodeid"] in station_number.values())
    if str(frame2.loc[row]["nodeid"]) in station_number.values():

        station_name = find_key_by_value(station_number, str(frame2.loc[row]["nodeid"]))
        csv_file = pathlib.Path("outputs/" + str(frame2.loc[row]["logtype"]) + "-" + station_name + ".csv")

        if frame2.loc[row]["logtype"] == 'wl':
            frame2["wl_data"] = wl_datum[station_name] - frame2.loc[row]["value"]
        else:
            frame2["wl_data"] = ""

        datarow = frame2.loc[row].to_frame().T
        # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
        datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)

    # print(str(int((row-1)/len(frame.recno) * 100)) + '%')

print("DONEs")


"""
for row in tqdm(frame2.recno):

    if str(frame2.loc[row - 1]["nodeid"]) in station_number.values():

        station_name = find_key_by_value(station_number, str(frame2.loc[row - 1]["nodeid"]))
        csv_file = pathlib.Path("outputs/" + station_name + "-" + str(frame2.loc[row - 1]["logtype"]) + ".csv")

        if frame2.loc[row - 1]["logtype"] == 'wl':
            frame2["wl_data"] = wl_datum[station_name] - frame2.loc[row - 1]["value"]
        else:
            frame2["wl_data"] = ""

        datarow = frame2.loc[row - 1].to_frame().T
        # datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
        datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)

print("DONEs")
"""
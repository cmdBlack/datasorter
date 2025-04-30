import numpy as np
import pandas as pd
from pandas import Series, DataFrame
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



error_list = ["wltimeout", "wlerror"]
data = pd.read_csv("datalogs.csv", delimiter=';')
data = data.replace(error_list, np.nan)
frame = pd.DataFrame(data)


del frame["description"]
del frame["outbound"]
del frame["recvtime"]
del frame["seqno"]


frame = frame.dropna()



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




for row in tqdm(frame.recno):

    if str(frame.loc[row-1]["nodeid"]) in station_number.values():
        station_name = find_key_by_value(station_number, str(frame.loc[row-1]["nodeid"]))
        datarow = frame.loc[row-1].to_frame().T
        #datarow.to_csv(str(frame.loc[row-1]["nodeid"]) + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)
        datarow.to_csv("outputs/" + station_name + "-" +  str(frame.loc[row-1]["logtype"]) + ".csv", mode='a', header=False)



print("DONEs")
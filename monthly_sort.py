import numpy as np
import pandas as pd
from tqdm import tqdm
import pathlib



station = input("Station-stationtype: ")
file_path = station + '.csv'
data = pd.read_csv("outputs/" + file_path)

frame = pd.DataFrame(data)
frame["date"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[0]
frame["time"] = frame["logtime"].str.split(pat=" ",n=1,expand=True)[1]

frame2 = frame.copy()

for row in tqdm(range(len(frame2.recno))):
    csv_file = pathlib.Path("outputs/" + "monthly-" + frame2.loc[row]["date"][:7] + "-" + station + ".csv")
    datarow = frame.loc[row].to_frame().T
    datarow.to_csv(csv_file, mode='a', header=not csv_file.exists(), index=False)
print("done")


#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
from pandas import Series, DataFrame


data = pd.read_csv("datalogs.csv", delimiter=';')
frame = pd.DataFrame(data)


for row in frame.recno:
    #print(type(frame.loc[row-1]))
    #print(frame.loc[row-1]["nodeid"])
    frame.loc[row-1].to_frame().T.to_csv(str(frame.loc[row-1]["nodeid"]) + ".csv", mode='a', header=False)

print("DONE DONE DONE DONE DONE DONE DONE DONE DONE")










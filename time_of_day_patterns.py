import os
from tqdm import tqdm
import numpy as np
import pandas as pd
import zipfile
from datetime import date
import plotly.graph_objects as go

from histdata import download_hist_data as dl
from histdata.api import Platform as p, TimeFrame as tf

path_zips = "/data/zips/"
path_csvs = "/data/csvs/"

# lol

os.chdir(path_zips)

years_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
              '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']

months_list = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']

# for y in years_list:
#     dl(year=y, month=None, pair='eurusd', platform=p.GENERIC_ASCII, time_frame=tf.ONE_MINUTE)

zip_files = os.listdir(path_zips)

print("Extracting zip files")
for fl in tqdm(zip_files):
    with zipfile.ZipFile(path_zips+fl, 'r') as zp:
        zp.extract(fl[:-4]+".csv", path_csvs)


os.chdir(path_csvs)

csv_files = os.listdir(path_csvs)

list_df = []
print("Saving DataFrames inside list_df")
for f in tqdm(range(len(csv_files))):
    df = pd.read_csv(csv_files[f][:-8]+years_list[f]+'.csv', sep=';', header=None)
    df.rename(columns={0: 'Date', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', }, inplace=True)
    df.drop(columns=['Volume'], inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    list_df.append(df)

date_from = pd.Timestamp(date(2000, 8, 1))
date_to = pd.Timestamp(date(2000, 8, 2))

x = list_df[0][(list_df[0]['Date'] > date_from) & (list_df[0]['Date'] < date_to)]


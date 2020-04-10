import os
from tqdm import tqdm
import numpy as np
import pandas as pd
import zipfile
from datetime import date
import plotly.graph_objects as go
from histdata import download_hist_data as dl
from histdata.api import Platform as p, TimeFrame as tf

path_zips = "/home/pfl-desktop/PycharmProjects/FX/fx_times_of_day_patterns/data/zips/"
path_csvs = "/home/pfl-desktop/PycharmProjects/FX/fx_times_of_day_patterns/data/csvs/"

years_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
              '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']

months_list = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']


def get_pair_currency_selected_years(lst, pair):
    for y in lst:
        dl(year=y, month=None, pair=pair, platform=p.GENERIC_ASCII, time_frame=tf.ONE_MINUTE)


def extract_zip_files(p_z, p_c):
    zip_files = os.listdir(p_z)
    print("Extracting zip files")
    for fl in tqdm(zip_files):
        with zipfile.ZipFile(path_zips+fl, 'r') as zp:
            zp.extract(fl[:-4]+".csv", p_c)


def transform_df_and_save_it_in_a_list_of_df(p_c):
    list_df = []
    csv_files = os.listdir(p_c)
    print("Saving DataFrames inside list_df")
    for f in tqdm(range(len(csv_files))):
        df = pd.read_csv(csv_files[f][:-8]+years_list[f]+'.csv', sep=';', header=None)
        df.rename(columns={0: 'Date', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', }, inplace=True)
        df.drop(columns=['Volume'], inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        list_df.append(df)

    return list_df


if __name__ == "__main__":
    os.chdir(path_zips)
    get_pair_currency_selected_years(lst=years_list, pair='eurusd')
    extract_zip_files(p_z=path_zips, p_c=path_csvs)
    os.chdir(path_csvs)
    list_of_dfs = transform_df_and_save_it_in_a_list_of_df(p_c=path_csvs)

    date_from = pd.Timestamp(date(2000, 9, 20))
    date_to = pd.Timestamp(date(2000, 9, 22))

    x = list_of_dfs[0][(list_of_dfs[0]['Date'] > date_from) & (list_of_dfs[0]['Date'] < date_to)]


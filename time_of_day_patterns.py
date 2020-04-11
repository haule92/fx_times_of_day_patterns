import os
from tqdm import tqdm
import numpy as np
import pandas as pd
import zipfile
from datetime import date, datetime, timedelta, timezone
import plotly.graph_objects as go
from histdata import download_hist_data as dl
from histdata.api import Platform as p, TimeFrame as tf

path_zips = "/home/pfl/PycharmProjects/fx_times_of_day_patterns/data/zips/"
path_csvs = "/home/pfl/PycharmProjects/fx_times_of_day_patterns/data/csvs/"

years_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
              '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']

months_list = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']


def get_pair_currency_selected_years(y_lst, pair):
    for y in tqdm(y_lst):
        #for m in m_lst:
        dl(year=y, month=None, pair=pair, platform=p.GENERIC_ASCII, time_frame=tf.ONE_MINUTE)


def extract_zip_files(p_z, p_c):
    zip_files = os.listdir(p_z)
    print("Extracting zip files")
    for fl in tqdm(zip_files):
        with zipfile.ZipFile(path_zips+fl, 'r') as zp:
            zp.extract(fl[:-4]+".csv", p_c)


def _times_manipulation(datetimecolumns, time_zone: str):
    datetimecolumns[f'year_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].year)
    datetimecolumns[f'quarter_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].quarter)
    datetimecolumns[f'month_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].month)
    datetimecolumns[f'weekofyear_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].weekofyear)
    datetimecolumns[f'dayofweek_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].dayofweek)
    datetimecolumns[f'day_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].day)
    datetimecolumns[f'hour_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].hour)
    datetimecolumns[f'minute_{time_zone}'] = pd.to_datetime(datetimecolumns[f'datetime_{time_zone}'].minute)

    return datetimecolumns

def _cleaning_df(df):
    df.rename(columns={0: 'Date', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', }, inplace=True)
    df.drop(columns=['Volume'], inplace=True)
    df['datetime_EST'] = pd.to_datetime(df['Date'])
    # df['year_EST'] = df['datetime_EST'].year
    # df = _times_manipulation(datetimecolumns=df, time_zone='EST')
    df['datetime_GMT'] = df['datetime_EST'] + timedelta(hours=5)
    # df = _times_manipulation(datetimecolumns=df, time_zone='GMT')

    return df


def transform_df_and_save_it_in_a_list_of_df(p_c, concrete=None):
    list_df = []
    csv_files = os.listdir(p_c)
    print("Saving DataFrames inside list_df")
    for f in tqdm(range(len(csv_files))):
        df = pd.read_csv(csv_files[f][:-8]+years_list[f]+'.csv', sep=';', header=None)
        df = _cleaning_df(df=df)
        list_df.append(df)
        df.to_csv(csv_files[f][:-8]+years_list[f]+'.csv', index=False)
    if concrete is not None:
        pass

    return list_df


def plot_min_candle(df):
    fig = go.Figure(data=go.Ohlc(x=df['Date'],
                                 open=df['Open'],
                                 high=df['High'],
                                 low=df['Low'],
                                 close=df['Close']))
    fig.show()


#def filter_data_per_months(df, ):


if __name__ == "__main__":
    os.chdir(path_zips)
    # get_pair_currency_selected_years(y_lst=years_list, pair='eurusd')
    extract_zip_files(p_z=path_zips, p_c=path_csvs)
    os.chdir(path_csvs)
    list_of_dfs = transform_df_and_save_it_in_a_list_of_df(p_c=path_csvs)

    df = pd.read_csv(path_csvs+'DAT_ASCII_EURUSD_M1_2002.csv')

    #date_from = pd.Timestamp(date(2000, 9, 20))
    #date_to = pd.Timestamp(date(2000, 9, 22))

    #x = list_of_dfs[0][(list_of_dfs[0]['Date'] > date_from) & (list_of_dfs[0]['Date'] < date_to)]

    #plot_min_candlechart(x)
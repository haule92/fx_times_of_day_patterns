import os
from tqdm import tqdm
import numpy as np
import pandas as pd
import zipfile
from datetime import date, datetime, timedelta, timezone
import plotly.graph_objects as go
from histdata import download_hist_data as dl
from histdata.api import Platform as p, TimeFrame as tf
from numba import jit

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


def _times_manipulation(df, tz: str):
    for i in tqdm(range(df.shape[0])):
        df.loc[i, f'year_{tz}'] = df[f'datetime_{tz}'].loc[i].year
        df.loc[i, f'quarter_{tz}'] = df[f'datetime_{tz}'].loc[i].quarter
        df.loc[i, f'month_{tz}'] = df[f'datetime_{tz}'].loc[i].month
        df.loc[i, f'weekofyear_{tz}'] = df[f'datetime_{tz}'].loc[i].weekofyear
        df.loc[i, f'dayofweek_{tz}'] = df[f'datetime_{tz}'].loc[i].dayofweek
        df.loc[i, f'day_{tz}'] = df[f'datetime_{tz}'].loc[i].day
        df.loc[i, f'hour_{tz}'] = df[f'datetime_{tz}'].loc[i].hour
        df.loc[i, f'minute_{tz}'] = df[f'datetime_{tz}'].loc[i].minute

    return df


def _times_manipulation2(df, tz: str):
    df[f'year_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.year)
    df[f'quarter_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.quarter)
    df[f'month_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.month)
    df[f'weekofyear_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.weekofyear)
    df[f'dayofweek_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.dayofweek)
    df[f'day_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.day)
    df[f'hour_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.hour)
    df[f'minute_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.minute)

    return df


def _times_manipulation3(df, tz: str):
    datetime_array = df[f'datetime_{tz}'].to_numpy()
    datetime_array = datetime_array.astype('M8[s]')

    year_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).year
        year_array = np.append(year_array, tmp)
    df[f'year_{tz}'] = year_array

    # df.loc[f'quarter_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.quarter)

    month_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).month
        month_array = np.append(month_array, tmp)
    df[f'month_{tz}'] = month_array

    # df.loc[f'weekofyear_{tz}'] = df[f'datetime_{tz}'].apply(lambda r: r.weekofyear)

    weekday_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).weekday()
        weekday_array = np.append(weekday_array, tmp)
    df[f'dayofweek_{tz}'] = weekday_array

    day_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).day
        day_array = np.append(day_array, tmp)
    df[f'day_{tz}'] = day_array

    hour_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).hour
        hour_array = np.append(hour_array, tmp)
    df[f'hour_{tz}'] = hour_array

    minute_array = np.array([])
    for i in tqdm(datetime_array):
        tmp = i.astype(object).minute
        minute_array = np.append(minute_array, tmp)
    df[f'minute_{tz}'] = minute_array

    return df


def _cleaning_df(df):
    df.rename(columns={0: 'Date', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', }, inplace=True)
    df.drop(columns=['Volume'], inplace=True)
    df['datetime_EST'] = pd.to_datetime(df['Date'])
    # df = _times_manipulation(df=df, tz='EST')
    df = _times_manipulation2(df=df, tz='EST')
    # df = _times_manipulation3(df=df, tz='EST')
    df['datetime_GMT'] = df['datetime_EST'] + timedelta(hours=5)
    # df = _times_manipulation(df=df, tz='GMT')
    df = _times_manipulation2(df=df, tz='GMT')
    # df = _times_manipulation3(df=df, tz='GMT')

    df = df[['datetime_EST',
           'year_EST',
           'quarter_EST',
           'month_EST',
           'weekofyear_EST',
           'dayofweek_EST',
           'day_EST',
           'hour_EST',
           'minute_EST',
           'datetime_GMT',
           'year_GMT',
           'quarter_GMT',
           'month_GMT',
           'weekofyear_GMT',
           'dayofweek_GMT',
           'day_GMT',
           'hour_GMT',
           'minute_GMT',
           'Open',
           'High',
           'Low',
           'Close']]


    return df


def transform_df_and_save_it_in_a_list_of_df(p_c, concrete=None):
    # list_df = []
    csv_files = os.listdir(p_c)
    print("Saving DataFrames inside list_df")
    for f in tqdm(range(len(csv_files))):
        df = pd.read_csv(csv_files[f][:-8]+years_list[10:][f]+'.csv', sep=';', header=None)
        df = _cleaning_df(df=df)
        # list_df.append(df)
        df.to_csv(csv_files[f][:-8]+years_list[10:][f]+'.csv', index=False)
    if concrete is not None:
        pass

    # return list_df


def plot_min_candle(df):
    fig = go.Figure(data=go.Ohlc(x=df['Date'],
                                 open=df['Open'],
                                 high=df['High'],
                                 low=df['Low'],
                                 close=df['Close']))
    fig.show()


if __name__ == "__main__":
    os.chdir(path_zips)
    get_pair_currency_selected_years(y_lst=years_list, pair='eurusd')
    extract_zip_files(p_z=path_zips, p_c=path_csvs)
    os.chdir(path_csvs)
    transform_df_and_save_it_in_a_list_of_df(p_c=path_csvs)

    # df = pd.read_csv(path_csvs+'DAT_ASCII_EURUSD_M1_2002.csv', sep=';', header=None)

    #date_from = pd.Timestamp(date(2000, 9, 20))
    #date_to = pd.Timestamp(date(2000, 9, 22))
    #x = list_of_dfs[0][(list_of_dfs[0]['Date'] > date_from) & (list_of_dfs[0]['Date'] < date_to)]
    #plot_min_candlechart(x)


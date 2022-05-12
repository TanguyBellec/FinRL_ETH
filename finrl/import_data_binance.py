import pandas as pd
import datetime as dt
import requests, json


from binance_mod.client import Client

# client configuration
api_key = 'lszgqK2jecdLCgoIU4BELKOHkhwkXbVtfTB0lYeXtwATtKMBS584tsA4tAbUeS74' 
api_secret = 'ctcdtdi9mMONEoV3UVo8ZdQu0DNrbXhv1NWHnmZlOydPkSZMNi6rc2C40qgba2Gl'

client = Client(api_key, api_secret)

def get_data_binance(symbol, interval, start_date, end_date):
    
    Client.KLINE_INTERVAL_4HOUR 
    # start_date = "1 Jan,2021" for example
    klines = client.get_historical_klines(symbol, interval, start_date, end_date)
    data = pd.DataFrame(klines)
    
    data.columns = ['open_time','open', 'high', 'low', 'close', 'volume','close_time', 'qav',
                    'num_trades','taker_base_vol','taker_quote_vol', 'ignore']
    data['date_close_time'] = [dt.datetime.fromtimestamp(x/1000.0) for x in data.close_time]
    data = data.drop(columns = ['open_time', 'close_time', 'ignore'])
    
    # change date to first column
    cols = data.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    data = data[cols]
    
    # add ticker
    symbols = []
    for i in range(len(data)):
        symbols.append(symbol)
    data['tic'] = symbols
    
    return data


def rename_columns_symbol(df_symbol, symbol):
    for indic in df_symbol.drop(columns = ['date_close_time', 'tic']):
        df_symbol = df_symbol.rename(columns = {indic : f'{df_symbol.tic.iloc[0]}_{indic}'})
    return df_symbol.drop(columns = ['tic'])


def add_symbols_indicators_to_df(df, symbols, interval, start_date, end_date):
    
    for symbol in symbols:
        df_symbol = get_data_binance(symbol, interval, start_date, end_date)
        df_symbol = rename_columns_symbol(df_symbol, symbol)

        df = pd.merge(df, df_symbol, on = "date_close_time")
    return df


def import_BVOL(time_start, time_end):

    data = json.loads(requests.get(f'https://coincodex.com/api/coincodexcoins/get_historical_data_by_slug/ftx-btc-vol-token/{time_start}/{time_end}/1?t=5459791').text)
    df = pd.DataFrame(data['data'])

    df = df[['time_start', "price_avg_usd"]]
    for i in range(len(df)):
        df['time_start'].iloc[i] = df['time_start'].iloc[i][0:10]
    df = df.rename(columns = {'time_start' : 'date', 'price_avg_usd' : 'BVOL_avg'})
    
    return df


def add_bvol_index(df, start_date, end_date):
    
    # Cell to keep only dates to allow to calculate VIX on preprocessing

    df = df.astype({'date_close_time' : str})
    df['date_close_time'].iloc[0][0:10]
    df_date_close_time = df['date_close_time']
    dates = []
    for i in range(len(df)):
        dates.append(df['date_close_time'].iloc[i][0:10])
    df['date'] = dates
    
    bvol = import_BVOL(start_date, end_date)

    bvol_index = []

    for counter_bvol in range(len(bvol)):
        current_date = bvol['date'][counter_bvol]
        while(df['date'][len(bvol_index)] == current_date):
            if(len(bvol_index) + 1 < len(df)):
                bvol_index.append(bvol['BVOL_avg'][counter_bvol])
            else:
                break

    bvol_index.append(bvol['BVOL_avg'].iloc[-1])
    df['BVOL_avg'] = bvol_index
    
    df = df.drop(columns = ['date'])
    
    return df
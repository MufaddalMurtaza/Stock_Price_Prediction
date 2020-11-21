import os
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import pandas_datareader.data as web
#style.use('ggplot')
import bs4 as bs
import pickle
import requests

#Function 1:
#Scraping sp500 tickers and saving to pickle
def save_sp500_tickers():

    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text)
    table = soup.find('table', {'class': 'wikitable sortable'}, {'id': 'constituents'})

    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        ticker = ticker.replace('\n', '')
        tickers.append(ticker)

    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers, f)

    return tickers

#Function 2:
#Getting the data from yahoo for sp500 companies
def get_data_from_yahoo(reload_sp500 = False):

    #Loading the list of tickers
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)

    if not os.path.exists('stocks_dfs'):
        os.makedirs('stocks_dfs')

    start = dt.datetime(2000, 1, 1)
    end = dt.datetime(2020, 12, 31)

    count = 0
    for ticker in tickers:
        if not os.path.exists('stocks_dfs/{}.csv'.format(ticker)):
            try:
                df = web.DataReader(ticker, 'yahoo', start, end)
                df.to_csv('stocks_dfs/{}.csv'.format(ticker))
                count += 1
                print('Progress : {} / {} , Ticker: {} is done'.format(count, len(tickers), ticker))
            except KeyError:
                print('This {} ticker cannot be scraped'.format(ticker))
        else:
            print('Already have {} ticker'.format(ticker))

#Function 3:
#Compiling the adj_close column of all the tickers
def compile_data_adj_close():

    with open("sp500tickers.pickle", "rb") as f:
        tickers = pickle.load(f)

    sp500_adjclose_df = pd.DataFrame()

    for count, ticker in enumerate(tickers):

        try:
            df = pd.read_csv('stocks_dfs/{}.csv'.format(ticker))
        except FileNotFoundError:
            print('This {} ticker does not exist'.format(ticker))
            continue

        df.set_index('Date', inplace = True)
        df.rename(columns = {'Adj Close':ticker}, inplace = True)
        df.drop(['High','Low','Open','Close','Volume'],
                axis = 1 #axis = 1 indicates column
                ,inplace = True)

        if sp500_adjclose_df.empty:
            sp500_adjclose_df = df
        else:
            sp500_adjclose_df = sp500_adjclose_df.join(df , how = 'outer')

        if count%10 == 0:
            print('{} files have been joined'.format(count))

    print(sp500_adjclose_df.head())
    sp500_adjclose_df.to_csv('sp500_adjclose_df.csv')


save_sp500_tickers()
get_data_from_yahoo(reload_sp500 = False)
compile_data_adj_close()



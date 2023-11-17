import pandas as pd
import numpy as np
import requests
import datetime
import time
import os

import warnings
warnings.filterwarnings('ignore')

project_dir = os.getcwd().rsplit('\\',1)[0]
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

class Scraper:
    def __init__(self, period_start, period_end):
        self.period_start = period_start
        self.period_end = period_end

    def get_yahoo(self):
        '''
        https://finance.yahoo.com
        '''
        tickers = {
            'NASDAQ Composite':'%5EIXIC',
            'Bitcoin':'BTC-USD',
            'Brent Crude Oil':'BZ%3DF'
            }
        start = int(datetime.datetime.timestamp(self.period_start))
        end = int(datetime.datetime.timestamp(self.period_end))

        df = pd.DataFrame()
        for ticker in tickers:
            url = \
                f'https://query1.finance.yahoo.com/v7/finance/download/{tickers[ticker]}' +\
                f'?period1={start}' +\
                f'&period2={end}' +\
                '&interval=1d' +\
                '&events=history' +\
                '&includeAdjustedClose=true'

            response = requests.get(url, headers=headers)
            data = response.text.splitlines()
            data = pd.DataFrame([z.split(',') for z in data])

            table = pd.DataFrame(data.values[1:], columns=pd.Series(data.iloc[0]).str.lower())

            table['ticker'] = ticker

            df = pd.concat([df, table], ignore_index=True)
            df = df.drop(columns=['adj close'])

            time.sleep(2)

        return df

    def get_mfd(self):
        '''
        https://mfd.ru
        '''
        tickers = [
            2287, # USD
            140335, # ММВБ Индексы
            174541 # ММВБ Бонды
            ]
        start = self.period_start.strftime("%d.%m.%Y")
        end = self.period_end.strftime("%d.%m.%Y")
        tickers_list = ",".join([str(elem) for elem in tickers])

        url = \
            f'https://mfd.ru/export/handler.ashx/export.txt?TickerGroup=11&Tickers={tickers_list}' +\
            '&Alias=false' +\
            '&Period=7' +\
            '&timeframeValue=1&timeframeDatePart=day' +\
            f'&StartDate={start}' +\
            f'&EndDate={end}' +\
            '&SaveFormat=0' +\
            '&SaveMode=0&FileName=export.txt' +\
            '&FieldSeparator=%3b' +\
            '&DecimalSeparator=.' +\
            "&DateFormat=dd'/'MM'/'yy" +\
            '&TimeFormat=HH:mm' +\
            '&DateFormatCustom=' +\
            '&TimeFormatCustom=' +\
            '&AddHeader=true' +\
            '&RecordFormat=3' +\
            '&Fill=false'

        response = requests.post(url)
        data = response.text.splitlines()
        data = pd.DataFrame([i.split(';') for i in data])

        header = pd.Series(data.iloc[0]).str.lower().str.replace('<','').str.replace('>','')
        df = pd.DataFrame(data.values[1:], columns=header)

        df['date'] = pd.to_datetime(df['date'], dayfirst=True, format='%d/%m/%y')

        cols = ['open', 'high', 'low', 'close', 'amount', 'volume']
        df[cols] = df[cols].astype('float64')

        df = df.drop(columns=['per','time','amount'])
        df['volume'] = df['volume'] / 1000000

        return df

    def get_cbr(self):
        '''
        https://www.cbr.ru
        '''
        start = self.period_start.strftime("%m.%d.%Y").replace(".","%2F")
        end = self.period_end.strftime("%m.%d.%Y").replace(".","%2F")

        url = \
            'https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/14315?Posted=True&' +\
            f'FromDate={start}&' +\
            f'ToDate={end}'

        df = pd.read_excel(url)
        df = df[['DT','ruo','vol']].rename(columns={'DT':'date', 'ruo':'close', 'vol':'volume'})

        df['ticker'] = 'RUONIA'
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)

        return df
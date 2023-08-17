#libraries
import pandas as pd
import numpy as np
import requests
import datetime
import time
import warnings
import io
import os

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import telegram # python-telegram-bot
from telegram.ext import CommandHandler, Updater

#options
pd.options.display.float_format = '{:,.3f}'.format
warnings.filterwarnings("ignore")

#variables
end_period = pd.to_datetime( datetime.date.today() )
start_period = end_period - np.timedelta64(370,'D')
project_dir = 'D:\\Data\\Projects\\Telegram_informer'
subscribers_file_link = '\\subscribers.csv'
token_file_link = '\\telegram_token.txt'

tickers_yahoo = {
    'NASDAQ Composite':'%5EIXIC',
    'Bitcoin':'BTC-USD',
    'Brent Crude Oil':'BZ%3DF'}

tickers_mfd = [
    2287, # USD
    140335, # ММВБ Индексы
    174585] # ММВБ Бонды

token_id = open(project_dir + token_file_link,'r').read() #telegram bot id / формат 1234567890:AAGdsdf2XgfgfwzuZg5541m443nVwl551joo

################################################

def scrape_yahoo(tickers, start, end):
    '''Парсинг сайта finance.yahoo.com'''
    yahoo = pd.DataFrame()

    for i in tickers:
        start = int(datetime.datetime.timestamp(start_period))
        end = int(datetime.datetime.timestamp(end_period))

        yahoo_url = f'https://query1.finance.yahoo.com/v7/finance/download/{tickers[i]}' +\
                    f'?period1={start}' +\
                    f'&period2={end}' +\
                    f'&interval=1d' +\
                    f'&events=history' +\
                    f'&includeAdjustedClose=true'
        #print(yahoo_url)

        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}
        response = requests.get(yahoo_url, headers=headers, verify=False)

        data = response.text.splitlines()
        data = pd.DataFrame([z.split(',') for z in data])

        table = pd.DataFrame(data.values[1:], columns=pd.Series(data.iloc[0]).str.lower())

        table['ticker'] = i

        yahoo = yahoo.append(table)
        time.sleep(5)
    
    yahoo = yahoo.replace('null',np.nan)
    yahoo = yahoo.drop(columns=['adj close'])

    yahoo['date'] = pd.to_datetime(yahoo['date'], dayfirst=True, format='%Y-%m-%d')

    yahoo['open'] = yahoo['open'].astype('float64')
    yahoo['high'] = yahoo['high'].astype('float64')
    yahoo['low'] = yahoo['low'].astype('float64')
    yahoo['close'] = yahoo['close'].astype('float64')
    yahoo['volume'] = yahoo['volume'].astype('float64') / 1000000

    #print(yahoo)
    return yahoo

def scrape_mfd(tickers, start, end):
    '''Парсинг сайта https://mfd.ru'''

    start = start.strftime("%d.%m.%Y")
    end = end.strftime("%d.%m.%Y")

    mfd_url = f'https://mfd.ru/export/handler.ashx/export.txt?TickerGroup=11&Tickers={",".join([str(elem) for elem in tickers])}' +\
              f'&Alias=false' +\
              f'&Period=7' +\
              f'&timeframeValue=1&timeframeDatePart=day' +\
              f'&StartDate={start}' +\
              f'&EndDate={end}' +\
              f'&SaveFormat=0' +\
              f'&SaveMode=0&FileName=export.txt' +\
              f'&FieldSeparator=%3b' +\
              f'&DecimalSeparator=.' +\
              "&DateFormat=dd'/'MM'/'yy" +\
              '&TimeFormat=HH:mm' +\
              '&DateFormatCustom=' +\
              '&TimeFormatCustom=' +\
              '&AddHeader=true' +\
              '&RecordFormat=3' +\
              '&Fill=false'

    response = requests.post(mfd_url)
    data = response.text.splitlines()
    data = pd.DataFrame([i.split(';') for i in data])

    header = pd.Series(data.iloc[0]).str.lower().str.replace('<','').str.replace('>','')
    mfd = pd.DataFrame(data.values[1:], columns=header)

    mfd['date'] = pd.to_datetime(mfd['date'], dayfirst=True, format='%d/%m/%y')

    mfd['open'] = mfd['open'].astype('float64')
    mfd['high'] = mfd['high'].astype('float64')
    mfd['low'] = mfd['low'].astype('float64')
    mfd['close'] = mfd['close'].astype('float64')
    mfd['amount'] = mfd['amount'].astype('float64')/1000000
    mfd['volume'] = mfd['volume'].astype('float64')/1000000

    mfd = mfd.drop(columns=['per','time','amount'])

    #print(mfd)
    return mfd

def calculate_rolling_mean(dataset):
    '''Расчет скользящих средних в разрезе каждого тикера'''

    tickers = list( dataset['ticker'].unique() )
    all_slices = pd.DataFrame()
    dataset['close'] = dataset['close'].fillna(method='ffill')

    for x in tickers:
        sliced_df = dataset[ (dataset['ticker'] == x) ]
        
        sliced_df['30d_ma'] = sliced_df['close'].rolling(30).mean()
        sliced_df['90d_ma'] = sliced_df['close'].rolling(90).mean()
        
        all_slices = all_slices.append(sliced_df)
        
    dataset = pd.merge(dataset,
                       all_slices[['date','ticker','30d_ma','90d_ma']],
                       how='left',
                       on=['date','ticker'])
    return dataset

def draw_plot(dataset, ticker):
    '''Визуализация графиков изменения цены'''
    fig, ax = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [4, 1]})

    fig.set_figwidth(15)
    fig.set_figheight(7)

    sample = dataset[ dataset['ticker'] == ticker ]

    days_range = 180
    period_limit = sample['date'].max() - np.timedelta64(days_range,'D')
    
    sample  = sample [ sample ['date'] >= period_limit]
    sample = sample.set_index('date')

    # Свечи

    #define width of candlestick elements
    width = 0.8
    width2 = .1

    #define up and down df
    up = sample[ sample['close'] >= sample['open'] ]
    down = sample[ sample['close'] < sample['open'] ]

    #define colors to use
    rise = '#2e9448'
    fall = '#db2328'

    #plot up df
    ax[0].bar(up.index, up['close']-up['open'], width, bottom=up['open'], color=rise)
    ax[0].bar(up.index, up['high']-up['close'], width2, bottom=up['close'], color=rise)
    ax[0].bar(up.index, up['low'] - up['open'], width2, bottom=up['open'], color=rise)

    #plot down df
    ax[0].bar(down.index,down['close']-down['open'],width,bottom=down['open'],color=fall)
    ax[0].bar(down.index,down['high']-down['open'],width2,bottom=down['open'],color=fall)
    ax[0].bar(down.index,down['low']-down['close'],width2,bottom=down['close'],color=fall)

    # Скользящие средние
    ax[0].plot(sample['30d_ma'], linestyle=':')
    ax[0].plot(sample['90d_ma'], linestyle=':')

    ax[0].xaxis.set_tick_params(rotation=0, reset=True)
    ax[0].set_xlabel('')
    ax[0].grid(axis='both')
    ax[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax[0].minorticks_off()
    #ax[0].margins(x=0.01)

    # Объемы
    ax[1].bar(sample.index, sample['volume']/1000)
    ax[1].tick_params(axis='x')
    ax[1].grid(axis='both')
    #ax[1].margins(x=0)

    plt.setp(ax[1].get_xticklabels(), visible=False)
    plt.suptitle(ticker, fontsize=20)

    #plt.savefig('picture.png', format='png')
    #plt.show()

    #сохранить график в переменную
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    plot = buf.getvalue()
    return plot

def check_for_new_subscribers(update, context):
    subscribers = pd.read_csv(project_dir + subscribers_file_link, sep=';')

    chat_id = update.message.chat.id
    username = update.message.chat.username
    first_name = update.message.chat.first_name
    last_name = update.message.chat.last_name

    if chat_id not in list( subscribers['chat_id'] ):
        print('Новый подписчик:', username)
        subscribers = subscribers.append({'chat_id':chat_id, 
                                          'username':username, 
                                          'first_name':first_name, 
                                          'last_name':last_name}, 
                                          ignore_index=True)
        subscribers.to_csv(project_dir + subscribers_file_link, encoding='utf-8-sig', sep=';', na_rep='NaN', index=False)
        update.message.reply_text('Подписка подтверждена')
    else:
        pass

def activate_bot(token):
    bot = telegram.Bot(token=token)

    print('Telegram бот активирован')
    print(bot.get_me())

    updater = Updater(token=token, use_context=True)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", check_for_new_subscribers))
    updater.start_polling()

    time.sleep(5)

    updater.stop()
    updater.is_idle = False

    return bot

def calculate_report_dates():
    today = datetime.date.today()
    day_of_week = today.weekday()+1

    if day_of_week > 5:
        report_period_start = today - datetime.timedelta(days=today.weekday())
        report_period_end = report_period_start + datetime.timedelta(days=6)
    else:
        report_period_start = today - datetime.timedelta(days=today.weekday(), weeks=1)
        report_period_end = report_period_start + datetime.timedelta(days=6)

    if report_period_end > today:
        report_day = today
    else:
        report_day = report_period_end

    return report_period_start, report_period_end, report_day


def send_reports(dataset):
    try:
        subscribers = pd.read_csv(project_dir + subscribers_file_link, sep=';')
    except FileNotFoundError:
        subscribers = pd.DataFrame(columns=['chat_id', 'username', 'first_name', 'last_name'])
        subscribers.to_csv(project_dir + subscribers_file_link, encoding='utf-8-sig', sep=';', na_rep='NaN', index=False)

    bot = activate_bot(token_id)
    report_period_start, report_period_end, report_day = calculate_report_dates()

    print('user list:')
    for user in subscribers['chat_id']:
        print(user)

        #Вступительное сообщение рассылки
        intro_message = f'❗️Итоги недели №{ report_period_end.isocalendar()[1] }❗️' +\
                        f'\n { report_period_start.strftime("%d.%m.%Y") } - { report_period_end.strftime("%d.%m.%Y") }'

        bot.send_message(chat_id=user, text=intro_message)

        for i in dataset['ticker'].unique():
            #Создание графика
            image = draw_plot(dataset, i)

            #Расчет дат (точек отсчета) для изменений за период
            base_slice_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] <= pd.to_datetime(report_day)) ]['date'].max()
            day7_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=6))) ]['date'].max()
            day30_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=29))) ]['date'].max()
            day90_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=89))) ]['date'].max()
            ytd_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] >= pd.to_datetime(report_day.replace(day=1,month=1))) ]['date'].min()
            month12_date = dataset[ (dataset['ticker'] == i) & (dataset['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=364))) ]['date'].max()

            #Расчет изменений за период
            day7_diff = dataset[ (dataset['ticker'] == i) & (dataset['date'] == base_slice_date) ]['close'].max() / dataset[ (dataset['ticker'] == i) & (dataset['date'] == day7_date) ]['close'].max() - 1
            day30_diff = dataset[ (dataset['ticker'] == i) & (dataset['date'] == base_slice_date) ]['close'].max() / dataset[ (dataset['ticker'] == i) & (dataset['date'] == day30_date) ]['close'].max() - 1
            day90_diff = dataset[ (dataset['ticker'] == i) & (dataset['date'] == base_slice_date) ]['close'].max() / dataset[ (dataset['ticker'] == i) & (dataset['date'] == day90_date) ]['close'].max() - 1
            ytd_diff = dataset[ (dataset['ticker'] == i) & (dataset['date'] == base_slice_date) ]['close'].max() / dataset[ (dataset['ticker'] == i) & (dataset['date'] == ytd_date) ]['close'].max() - 1
            month12_diff = dataset[ (dataset['ticker'] == i) & (dataset['date'] == base_slice_date) ]['close'].max() / dataset[ (dataset['ticker'] == i) & (dataset['date'] == month12_date) ]['close'].max() - 1

            #Текст сообщения
            description_message = f'🔴{i} ' +\
                                  f'\n Изменения за период ' +\
                                  f'\n Неделя: { "{:+.1%}".format(day7_diff) }' +\
                                  f'\n 30 дней: { "{:+.1%}".format(day30_diff) }' +\
                                  f'\n 90 дней: { "{:+.1%}".format(day90_diff) }' +\
                                  f'\n С начала года: { "{:+.1%}".format(ytd_diff) }' +\
                                  f'\n 12 месяцев: { "{:+.1%}".format(month12_diff) }'

            #Рассылка сообщений
            bot.send_photo(chat_id=user,
                           photo=image,
                           caption=description_message,
                           disable_notification=True)
            time.sleep(2)
        time.sleep(5)


def main():
    df = pd.concat([
        scrape_yahoo(tickers_yahoo, start_period, end_period),
        scrape_mfd(tickers_mfd, start_period, end_period)
        ])
    df = calculate_rolling_mean(df)
    send_reports(df)

if __name__ == '__main__':
    main()

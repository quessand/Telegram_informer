import pandas as pd
import numpy as np
import datetime
import io

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def calculate_report_dates():
    '''
    Calculate report dates to determine previous business week (Mon-Fri) dates in case script was executed not according to standart schedule
    '''
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


def calculate_rolling_mean(dataset):
    '''
    30-days and 90-days rolling means
    '''
    tickers = list( dataset['ticker'].unique() )
    dataset['close'] = dataset['close'].fillna(method='ffill')
    all_slices = pd.DataFrame()

    for i in tickers:
        sliced_df = dataset[ (dataset['ticker'] == i) ]
        sliced_df = sliced_df.sort_values(by='date', ascending=True)
        
        sliced_df['30d_ma'] = sliced_df['close'].rolling(30, min_periods=None).mean()
        sliced_df['90d_ma'] = sliced_df['close'].rolling(90, min_periods=None).mean()
        
        all_slices = pd.concat([all_slices,sliced_df], ignore_index=True)
        
    dataset = pd.merge(
        dataset,
        all_slices[['date','ticker','30d_ma','90d_ma']],
        how='left',
        on=['date','ticker'])
        
    return dataset

def draw_plot(dataset, ticker):
    '''
    Visualization of scraped data
    '''
    sample = dataset[ dataset['ticker'] == ticker ]

    fig, ax = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [4, 1]})

    fig.set_figwidth(15)
    fig.set_figheight(7)

    days_range = 180
    period_limit = sample['date'].max() - np.timedelta64(days_range,'D')
    
    sample  = sample [ sample ['date'] >= period_limit]
    sample = sample.set_index('date')

    if sample['open'].sum() > 0:
        # Candles (upper plot)
        width = 0.8
        width2 = 0.1

        # define up and down 
        up = sample[ sample['close'] >= sample['open'] ]
        down = sample[ sample['close'] < sample['open'] ]

        # define colors to use
        rise = '#2e9448'
        fall = '#db2328'

        # plot upper candle
        ax[0].bar(up.index, up['close']-up['open'], width, bottom=up['open'], color=rise)
        ax[0].bar(up.index, up['high']-up['close'], width2, bottom=up['close'], color=rise)
        ax[0].bar(up.index, up['low'] - up['open'], width2, bottom=up['open'], color=rise)

        # plot lower candle
        ax[0].bar(down.index,down['close']-down['open'],width,bottom=down['open'],color=fall)
        ax[0].bar(down.index,down['high']-down['open'],width2,bottom=down['open'],color=fall)
        ax[0].bar(down.index,down['low']-down['close'],width2,bottom=down['close'],color=fall)

    else:
        # Line (upper plot)
        ax[0].plot(sample['close'], linewidth=2, color='#C70039')
    
    # Rolling mean lines
    ax[0].plot(sample['30d_ma'], linestyle=':')
    ax[0].plot(sample['90d_ma'], linestyle=':')

    # Plot properties
    ax[0].xaxis.set_tick_params(rotation=0, reset=True)
    ax[0].set_xlabel('')
    ax[0].grid(axis='both')
    ax[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax[0].tick_params(labelright=True)
    ax[0].minorticks_off()


    if sample['volume'].sum() > 0:
        # Volumes (lower plot)
        ax[1].bar(sample.index, sample['volume']/1000)
        ax[1].grid(axis='both')
        ax[1].tick_params(axis='x', labelright=True)

    plt.suptitle(ticker, fontsize=20)

    # save plot as variable
    buf = io.BytesIO()
    fig.savefig(buf, format='png')

    plot = buf.getvalue()
    return plot
import pandas as pd
import numpy as np
import datetime
import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from aiogram.types import BufferedInputFile

from scraper import Scraper
import data_processing

project_dir = 'D:\\Data\\Projects\\Telegram_informer'
token_file_link = '\\telegram_token.txt'
subscribers_file_link = '\\subscribers.csv'
token = open(project_dir + token_file_link,'r').read()

bot = Bot(token=token)
dp = Dispatcher()

async def get_updates(dp, bot):
    loop = asyncio.get_event_loop()

    task = loop.create_task(dp.start_polling(bot))
    await asyncio.sleep(5)

    task.cancel()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    subscribers = pd.read_csv(project_dir + subscribers_file_link, sep=';')

    chat_id = message.chat.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    if chat_id not in list( subscribers['chat_id'] ):
        print('Новый подписчик:', username)
        new_user = pd.DataFrame(data=[[chat_id,username,first_name,last_name]], columns=['chat_id','username','first_name','last_name'])
        subscribers = pd.concat([subscribers, new_user], ignore_index=True)

        subscribers.to_csv(project_dir + subscribers_file_link, encoding='utf-8-sig', sep=';', na_rep='NaN', index=False)
        await message.answer('Подписка подтверждена')
    else:
        await message.answer(f'{message.from_user.username}: Уже есть в рассылке')

@dp.message(Command("test"))
async def cmd_test(message: types.Message):
    await message.answer(f'Test reply: {message.from_user.username}')

@dp.message()
async def send_reports(message: types.Message):
    period_end = pd.to_datetime( datetime.date.today() )
    period_start = period_end - np.timedelta64(380,'D')

    scraper = Scraper(period_start, period_end)
    class_methods = [method for method in list(Scraper.__dict__) if not method.startswith('__')]

    df = pd.DataFrame()
    for method in class_methods:
        func=getattr(scraper, method)
        data = func()
        df = pd.concat([df, data], ignore_index=True)

    df['date'] = pd.to_datetime(df['date'])
    cols = ['open', 'high', 'low', 'close', 'volume']
    df[cols] = df[cols].astype('float64').round(2)

    df = data_processing.calculate_rolling_mean(df)
    report_period_start, report_period_end, report_day = data_processing.calculate_report_dates()

    try:
        subscribers = pd.read_csv(project_dir + subscribers_file_link, sep=';')
    except FileNotFoundError:
        subscribers = pd.DataFrame(columns=['chat_id', 'username', 'first_name', 'last_name'])
        subscribers.to_csv(project_dir + subscribers_file_link, encoding='utf-8-sig', sep=';', na_rep='NaN', index=False)

    # Intro message
    intro_message = \
        f'❗️Итоги недели №{ report_period_end.isocalendar()[1] }❗️' +\
        f'\n { report_period_start.strftime("%d.%m.%Y") } - { report_period_end.strftime("%d.%m.%Y") }'
    
    print('Sending messages:')
    for user in subscribers['chat_id']:
        print(f'>>> {user}')
        await bot.send_message(chat_id=user, text=intro_message)

        for i in df['ticker'].unique():
            slice = df[ df['ticker'] == i ]
            image = data_processing.draw_plot(df, i)

            # Calculating key dates in the past
            base_slice_date = slice[ slice['date'] <= pd.to_datetime(report_day) ]['date'].max()

            day7_date = slice[ slice['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=6)) ]['date'].max()
            day30_date = slice[ slice['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=29)) ]['date'].max()
            day90_date = slice[ slice['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=89)) ]['date'].max()
            ytd_date = slice[ slice['date'] >= pd.to_datetime(report_day.replace(day=1,month=1)) ]['date'].min()
            month12_date = slice[ slice['date'] <= pd.to_datetime(report_day - datetime.timedelta(days=364)) ]['date'].max()

            # Calculating percentage difference 
            base_slice_value = slice[ slice['date'] == base_slice_date ]['close'].max()

            day7_diff = base_slice_value / slice[ slice['date'] == day7_date ]['close'].max() - 1
            day30_diff = base_slice_value / slice[ slice['date'] == day30_date ]['close'].max() - 1
            day90_diff = base_slice_value / slice[ slice['date'] == day90_date ]['close'].max() - 1
            ytd_diff = base_slice_value / slice[ slice['date'] == ytd_date ]['close'].max() - 1
            month12_diff = base_slice_value / slice[ slice['date'] == month12_date ]['close'].max() - 1

            # Main message
            description_message = \
                f'🔴{i} ' +\
                f'\n Изменения за период ' +\
                f'\n Неделя: { "{:+.1%}".format(day7_diff) }' +\
                f'\n 30 дней: { "{:+.1%}".format(day30_diff) }' +\
                f'\n 90 дней: { "{:+.1%}".format(day90_diff) }' +\
                f'\n С начала года: { "{:+.1%}".format(ytd_diff) }' +\
                f'\n 12 месяцев: { "{:+.1%}".format(month12_diff) }'

            # Sending message
            await bot.send_photo(
                chat_id=user,
                photo=BufferedInputFile(file=image, filename=i),
                caption=description_message,
                disable_notification=True
                )

async def main():
    await get_updates(dp, bot)
    await asyncio.sleep(10)
    await send_reports(dp.message)

if __name__ == '__main__':
    asyncio.run(main())
import telegram
import pandahouse as ph
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import datetime, timedelta
import locale

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Параметры подключения к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20240120'
}

# Параметры DAG'a
default_args = {
    'owner': 's-c',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 12)
}


schedule_interval = '0 11 * * *'

# Зададим настройки бота
token = '1987162789:AAFgHNqBv-v5VXPQcS0btoxtXECUvw8akMs' 
bot = telegram.Bot(token=token)
chat_id = 303597246

locale.setlocale(locale.LC_NUMERIC, '')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report():
    
    @task
    # Выгружаем данные за прошлый день
    def extract_yesterday():
        q = """
        SELECT toDate(time) as date,
            count(DISTINCT user_id) as dau, 
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes, 
            likes/views as ctr
        FROM {db}.feed_actions
        WHERE date = yesterday()
        GROUP BY date
        """

        report_last_day = ph.read_clickhouse(q, connection=connection)
        return report_last_day
    
    
    @task
    # Выгружаем данные за прошлую неделею
    def extract_last_week():
        q = """
        SELECT toDate(time) as date,
            count(DISTINCT user_id) as dau, 
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes, 
            likes/views as ctr
        FROM {db}.feed_actions
        WHERE date between today() - 7 and yesterday()
        GROUP BY date
        """

        report_last_week = ph.read_clickhouse(q, connection=connection)
        return report_last_week
    
    
    @task
    # Подготовим метрики за прошлый день
    def transform_yesterday(report_last_day):
        dau = locale.format_string('%d', report_last_day['dau'].sum(), grouping=True).replace(',', ' ')
        views = locale.format_string('%d', report_last_day['views'].sum(), grouping=True).replace(',', ' ')
        likes = locale.format_string('%d', report_last_day['likes'].sum(), grouping=True).replace(',', ' ')
        ctr = report_last_day['ctr'].sum()

        message = 'Добрый день!' + '\n\n' + f'Статистика ленты новостей за вчера ({yesterday}):\n\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr:.2f}\n'
        
        #bot.sendMessage(chat_id=chat_id, text=message)
        return message
    
    
    @task
    # Подготовим графики за прошлые 7 дней
    def transform_last_week(report_last_week):
        fig, axes = plt.subplots(2, 2, figsize=(24, 14))

        fig.suptitle('Динамика показателей ленты за последние 7 дней', fontsize=26)

        sns.lineplot(ax = axes[0, 0], data = report_last_week, x = 'date', y = 'dau')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = report_last_week, x = 'date', y = 'ctr')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 0], data = report_last_week, x = 'date', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = report_last_week, x = 'date', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '7_days_stats_feed.png'
        plt.close()
        
        #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return plot_object
    
    
    @task
    # Отправим отчет
    def load_report(message, plot_object):
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=message)


        
    df_cube_yesterday = extract_yesterday()
    df_cube_last_week = extract_last_week()
    msg_yesterday = transform_yesterday(df_cube_yesterday)
    img_week = transform_last_week(df_cube_last_week)
    load_report(msg_yesterday, img_week)

    
feed_report = feed_report()

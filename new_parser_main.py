from pars_cls import  Parser_content, Parser_this_and_next_week
from datetime import datetime
import pytz
from updating_cls import Updating
import time
import requests
from dotenv import load_dotenv
load_dotenv()
import os 
simple_pars_weeks = Parser_this_and_next_week()
cont_hist_data = Parser_content()

import logging
logging.basicConfig(level=logging.DEBUG, filename='new_parser_logging.log', 
                    format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d)', 
                    datefmt='%d/%m/%Y %H:%M:%S',
                    encoding = 'utf-8', filemode='w')

API_TOKEN = os.getenv("API_TOKEN")
TG_CH = os.getenv("TG_CH")

def send_message(text:str):
    res = requests.get('https://api.telegram.org/bot{}/sendMessage'.format(API_TOKEN),
                       params={'chat_id': TG_CH, 'text': text})
    return res.json()

def job():
    send_message("Начинаем парсить эту и следующию неделю!")
    link_and_id = simple_pars_weeks.Parser_this_and_next_week_start() # (source,id_news)
    send_message("Начинаем парсить контент и исторические данные!")
    for i in range(0, len(link_and_id), 30):
        elements = link_and_id[i:i+30]
        print(i,i+30)
        cont_hist_data.start_pars_hist_and_content(elements)

if __name__ == "__main__":
    while True:
        job()#собираем основную информацию
        data_times = Updating.select_time_or_update_db(mod='select_time') # забираем время для обновления новостей
        send_message('Основной апроцесс парсинга завершен. Планавое обновление новостей в процессе.')
        try:
            for time_str in data_times:
                flag = True
                send_message(f'Обновление новости в {time_str}')
                while flag == True:
                    today = datetime.now(pytz.utc)
                    current_time = datetime.strftime(today,"%Y-%m-%d %H:%M")
                    if current_time > time_str:
                        '''Ждем пока новость появиться на сайте'''
                        time.sleep(60)
                        Updating.update_data_html()
                        send_message(f'Новость обновлена!')
                        time.sleep(60*4)
                        Updating.update_data_html()
                        time.sleep(60*3)
                        Updating.update_data_html()
                        flag = False
                    else:
                        time.sleep(30)
        except Exception as e:
            logging.exception(e)
            time.sleep(60)

        







from datetime import datetime, timedelta
import pytz
import sqlite3
from collections import OrderedDict
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
'''Данный класс обновляет ноости. А именно - Previous,Consensus,Actual'''
class Updating:
    @staticmethod
    def get_time():
        # Получаем текущую дату и время в UTC
        today = datetime.now(pytz.utc)
        # Определяем день недели, где понедельник - 0, воскресенье - 6
        weekday = today.weekday()
        # Определяем начало недели в UTC
        start_of_week = today - timedelta(days=weekday)
        # Определяем конец недели в UTC
        end_of_week = today + timedelta(days=7)
        # Отформатируем дату и время
        start_of_week_formatted = start_of_week.strftime("%Y-%m-%d")
        end_of_week_formatted = end_of_week.strftime("%Y-%m-%d") 
        today = today.strftime("%Y-%m-%d %H:%M")
        return  (today,end_of_week_formatted)
    
    @staticmethod
    def select_time_or_update_db(mod:str,data=None)->list:
        conn = sqlite3.connect('DB_ekonom_calndar.db')
        cursor = conn.cursor()
        if mod == 'select_time':
            today,end_of_week_formatted = Updating.get_time()
            sql = '''select Date from news 
                    where ?<= Date  AND  Date <? '''
            cursor.execute(sql,(today,end_of_week_formatted))
            results = cursor.fetchall()
            results = list(OrderedDict.fromkeys([i[0] for i in results]))
            return (results)
        
        if mod == 'update_db':
            sql = '''
                UPDATE news 
                SET Previous = ?, Consensus = ?, Actual = ?
                WHERE id_news = ?
                '''
            for Previous,Consensus,Actual,id_news in data: #data - это список список с нужными данными
                cursor.execute(sql, (Previous,Consensus,Actual,id_news))

        conn.commit()
        conn.close()
        
    
    @staticmethod
    def update_data_html():
        url = 'https://www.myfxbook.com/forex-economic-calendar'
        headers = {'User-Agent': UserAgent().random}
        response = requests.get(url=url,headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        chunks = soup.find_all('tr', class_='economicCalendarRow')
        result =[]
        for chunk in chunks:
            try:
                Previous = chunk.find('span', class_="previousCell").text.strip()
                if Previous == '':
                    Previous = "None"
            except:
                Previous = "None"
            try:
                Consensus = chunk.find_all('td', class_="calendarToggleCell")[-3].text.strip()
                if Consensus == '':
                    Consensus = "None"
            except:
                Consensus = "None"
            try:
                Actual = chunk.find('span', class_="eventTdPopover dotted").text.strip()
                if Actual == '':
                    Actual = "None"
            except:
                Actual = "None"
            try:    
                id_news = re.sub(r'\D', '', str(chunk).split('<')[1].split()[2]) # id
            except:
                id_news = re.sub(r'\D', '', str(chunk).split('<')[1].split()[3]) # id
            result.append((Previous,Consensus,Actual,id_news))
        Updating.select_time_or_update_db(mod = 'update_db',data=result)
        

# # print(Updating.select_time_or_update_db(mod='select_time'))
# print(Updating.get_time())
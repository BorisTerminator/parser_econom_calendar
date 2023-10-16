from fastapi import FastAPI
from datetime import datetime, timedelta
import json
from fake_useragent import UserAgent
import sqlite3
from datetime import datetime, date
from uvicorn import run
import pytz

def get_time():
    # Получаем текущую дату и время в UTC
    now = datetime.now(pytz.utc)

    # Определяем день недели, где понедельник - 0, воскресенье - 6
    weekday = now.weekday()

    # Определяем начало недели в UTC
    start_of_week = now - timedelta(days=weekday)

    # Определяем конец недели в UTC
    end_of_week = start_of_week + timedelta(days=7)

    # Отформатируем дату и время
    start_of_week_formatted = start_of_week.strftime("%d.%m.%Y")
    end_of_week_formatted = end_of_week.strftime("%d.%m.%Y")
    return (start_of_week_formatted,end_of_week_formatted)

ua = UserAgent()
app = FastAPI()


def select_news(start_of_week_formatted,end_of_week_formatted):
    conn = sqlite3.connect('DB_ekonom_calndar.db')
    cursor = conn.cursor()
    sql = '''
            select  * from news
            where  ?<=Date and Date<?
            '''
    cursor.execute(sql,(start_of_week_formatted,end_of_week_formatted))
    results = cursor.fetchall()
    conn.commit()
    conn.close()
    return (results)

def sql_select(time):
    # Установите соединение с базой данных SQLite
    conn = sqlite3.connect('DB_ekonom_calndar.db')
    cursor = conn.cursor()

    # # Получите текущую дату и время
    # now = datetime.now()

    # Выполните запрос к базе данных
    cursor.execute("SELECT * FROM news WHERE Date LIKE ?", (time.strftime('%Y-%m-%d') + '%',))

    # Получите все строки, соответствующие запросу
    rows = cursor.fetchall()

    # Получите имена столбцов
    names = [description[0] for description in cursor.description]

    # Преобразуйте список списков в список словарей
    result = [dict(zip(names, row)) for row in rows]
    conn.close()
    return result

@app.get('/parser_1/get_data/today')
def get_data_today():
    now = datetime.now()
    return sql_select(now)
    
@app.get('/parser_1/get_data/tommorow')
def get_data_tommorow():
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    return sql_select(tomorrow)

@app.get('/parser_1/get_data/this_week')
def get_data_this_week():
    monday,sunday = get_time()
    return (select_news(monday,sunday))
    
# Произвольный периуд
@app.get('/parser_1/get_data/arbitrary_time/{start_day}/{finish_day}')
def get_data_arbitrary_time(start_day:str,finish_day:str):
    # start_day,finish_day = datetime.strftime("%d.%m.%Y"),datetime.strftime("%d.%m.%Y")
    return select_news(start_day,finish_day)

@app.get('/parser_1/get_data/find_by_id/{id_list}')
def get_data_find_by_id(id_list:str):
    id_list = json.loads(id_list)
    conn = sqlite3.connect('DB_ekonom_calndar.db')
    cursor = conn.cursor()
    placeholders = ','.join('?' for _ in id_list)
    sql = f'''
            SELECT * FROM news
            WHERE id_news IN ({placeholders})
            '''
    cursor.execute(sql,id_list)
    results = cursor.fetchall()
    conn.commit()
    conn.close()
    return (results)
    


if __name__ == "__main__":
    run(app="main:app", reload=True, host="192.168.1.9", port=8080)
    #run(app="main:app", reload=True, host="localhost", port=80)

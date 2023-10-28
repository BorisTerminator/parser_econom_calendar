from fastapi import FastAPI
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import sqlite3
from datetime import datetime, date
from uvicorn import run
import logging
from pydantic import BaseModel
logging.basicConfig(level=logging.WARNING, filename='main.log', 
                    format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d)', 
                    datefmt='%d/%m/%Y %H:%M:%S',
                    encoding = 'utf-8', filemode='w')
ua = UserAgent()
app = FastAPI()

from typing import List

from fastapi.responses import JSONResponse
class RequestData(BaseModel):
    id: List[int]

def add_1_day(date_string):
    # Разделение строки на год, месяц и день
    year, month, day = date_string.split("-")

    # Преобразование значений в числовой формат
    year = int(year)
    month = int(month)
    day = int(day)

    # Создание даты из полученных значений
    date = datetime(year, month, day)

    # Прибавление одного дня
    new_date = date + timedelta(days=1)

    # Преобразование даты обратно в строку в формате "YYYY-MM-DD"
    new_date_string = new_date.strftime("%Y-%m-%d")
    return new_date_string
   

def select_news(start_of_week_formatted = None, end_of_week_formatted = None, id_list = None, mod = 0):
    conn = sqlite3.connect('DB_ekonom_calndar.db')
    cursor = conn.cursor()
    if mod == 0:
        sql = '''
                select  * from news
                where  ?<=Date and Date<?
                '''
        cursor.execute(sql,(start_of_week_formatted,end_of_week_formatted))
        rows = cursor.fetchall()
    else:
        placeholders = ','.join('?' for _ in id_list)
        sql = f'''
                SELECT * FROM news
                WHERE id_news IN ({placeholders})
                '''
        cursor.execute(sql,id_list)
        rows = cursor.fetchall()
    conn.commit()
    conn.close()
    # Получите имена столбцов
    names = [description[0] for description in cursor.description]
    # Преобразуйте список списков в список словарей
    results = [dict(zip(names, row)) for row in rows]
    return (results)

# Произвольный периуд           YYYY-MM-DD
@app.get('/econom_calendar/api/{start_day}/{finish_day}')
def get_data_arbitrary_time(start_day:str,finish_day:str) -> list:
    finish_day = add_1_day(finish_day)
    data = select_news(start_day,finish_day)
    return JSONResponse(data)


@app.post("/econom_calendar/find_by_id/api")
async def find_by_id(request_data: RequestData):
    id = request_data.id
    results = select_news(id_list=id, mod=1)
    return JSONResponse(results)




if __name__ == "__main__":
    # run(app="main:app", reload=True, host="192.168.1.9", port=8080)
    run(app="main:app", reload=True, host="localhost", port=80)

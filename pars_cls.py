from playwright.async_api import Playwright, async_playwright
import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
import re
import sqlite3
import json
from fake_useragent import UserAgent
"""Данный метод вставляет данные в БД"""
class insert_to_DB:
    @staticmethod
    def insert_to_DB_methods(mod:str,data:list)->None:
        conn = sqlite3.connect('DB_ekonom_calndar.db')
        cursor = conn.cursor()
        #======================================
        if mod == 'insert_content_and_hist_data': # функция вставляет данные контент и hist data
            sql = '''
                UPDATE news 
                SET Content = ?, Historical_data = ?
                WHERE id_news = ?
                '''
            for row in data: #data - это список список с нужными данными
                id_ = row[0]
                hist_dat = json.dumps(row[1])  # преобразование в строку
                content = row[2]
                cursor.execute(sql, (content, hist_dat, id_))
        if mod == 'insert_simple_data': # вставляет просто данные собранные во время работы обычного парсинга
            for row in data:
                id_to_check = row[8]
                # Проверка наличия id в столбце news_id
                cursor.execute("SELECT COUNT(*) FROM news WHERE id_news = ?", (id_to_check,))
                result = cursor.fetchone()
                if result[0] == 0:
                    # Вставка запроса
                    cursor.execute('''INSERT INTO news 
                                (Date, Country, Symbol, Event, Impact, Previous, Consensus, Actual, id_news, source) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)
        #======================================
        conn.commit()
        conn.close()


'''Данные класс парсит исторические данные и контент. Возвращает он списко списков вида [id: str,hist_data: list,content: str]'''
class Parser_content:
    @staticmethod
    async def analiis(html,id_):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            #===============
            try:
                soup2 = soup.find('div', class_='margin-top-15 margin-bottom-25')
                content = soup2.text.strip().replace("\n", "")
            except:
                content = 'None'
            #================
            rows = soup.find_all('tr', class_='eventHistoryRow')
            # Проходимся по каждой строке и извлекаем нужные данные
            massive = []
            for row in rows:
                date = row.find('td', class_='text-overflow-ellipsis').text
                try:
                    # Извлекаем значения из ячеек столбцов
                    date_object = datetime.strptime(date, "%b %d, %Y")
                    formatted_date = date_object.strftime('%Y-%m-%d')
                    release = row.find('td', class_='text-overflow-ellipsis text-left').text.strip()
                    previous = row.find('span', class_='font13 calendarToggleCell').text.strip()
                    consensus = row.find_all('td',class_='text-overflow-ellipsis text-center')[-1].text.strip()
                    try:
                        actual = row.find_all('span',class_='eventTdHistoryPopover dotted')[-1].text.strip()
                    except:
                        actual = row.find_all('td',class_='text-overflow-ellipsis text-center')[-1].text.strip()
                    # Выводим извлеченные данные
                    my_dict = {
                                'date': formatted_date,
                                'time': release,
                                'Previous_hist': previous,
                                'Consensus_hist': consensus,
                                'Actual_hist': actual }
                except:
                    my_dict = {
                                'date': date,
                                'time': 'None',
                                'Previous_hist': 'None',
                                'Consensus_hist': 'None',
                                'Actual_hist': 'None' }
                massive.append(my_dict)
            return (id_, massive, content)
        except Exception as e:
            print('Не получилось получить исторические данные', e)
            return ([id_,'None','None'])
        
    @staticmethod   
    async def test(context,url:str,id_:str,semaphore:asyncio.Semaphore):
        page = await context.new_page()
        await semaphore.acquire() # занимаем семафор 
        # установка таймаута ожидания загрузки страницы в 60 секунд
        page.set_default_timeout(60000 * 3) # время ожидания ответа страницы 3 мин
        try:
            await page.goto(url)
            await page.get_by_role("link", name="History").click()
            await asyncio.sleep(1)
            await page.get_by_role("link", name="History").click()
            # await page.wait_for_selector('.bold', timeout=10000)
            await asyncio.sleep(2)
            await page.get_by_role("link", name="History").click()
            await asyncio.sleep(4)
            html = await page.content()
            result = await Parser_content.analiis(html,id_)
            await page.close()
            semaphore.release() # отпускаем семафор 
            return (result) 
        except:
            semaphore.release() # отпускаем семафор 
            return ([id_,'None','None'])
            

    @staticmethod
    async def run(playwright, url_list):
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=UserAgent().random)
        m = []
        semaphore = asyncio.Semaphore(3)
        for i in range(0, len(url_list), 3):
            elements = url_list[i:i+3]
            for url,id_ in elements:
                m.append(Parser_content.test(context,url,id_,semaphore))
        results = await asyncio.gather(*m)
        # ---------------------
        await context.close()
        await browser.close()
        return results

    async def main(url_list:list):
        async with async_playwright() as playwright:
            results = await Parser_content.run(playwright, url_list)
            return results

    @staticmethod
    def start_pars_hist_and_content(url_list: list) -> list:
        data = asyncio.run(Parser_content.main(url_list))
        # return(data)
        insert_to_DB.insert_to_DB_methods('insert_content_and_hist_data', data)    # Вставляем данные в БД
        

'''Этот класс собирает доступную информацию на эту и след. неделю. Собирает информацию и вставляет в БД'''
class Parser_this_and_next_week:
    @staticmethod
    async def parser_wheek(mod:str)->list:
        url = 'https://www.myfxbook.com/forex-economic-calendar'
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(60000 * 3)
            await page.goto(url)
            await page.click(f'text= {mod}')
            await asyncio.sleep(1)
            await page.click(f'text= {mod}')
            await asyncio.sleep(10)
            html = await page.content()
            await context.close()
            await browser.close()
            return html
        
    @staticmethod
    async def main():
        tasks =[]
        mods = ['This Week','Next Week']
        for mod in mods:
            tasks.append(Parser_this_and_next_week.parser_wheek(mod=mod))
        results = await asyncio.gather(*tasks)
        return(results)
    
    @staticmethod
    def analisis_html(htmls:str)->None:
        result = []
        link_and_id = []
        for html in htmls:
            soup = BeautifulSoup(html, 'html.parser')
            chunks = soup.find_all('tr', class_='economicCalendarRow')
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
                
                date = chunk.find('div', class_='align-center calendarDateTd')['data-calendardatetd']
                # Преобразование строки в объект datetime
                date_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
                formatted_date = date_obj.strftime("%Y-%m-%d %H:%M")
                country = chunk.find_all('i')[2]['title']
                Symbol = chunk.find_all('td', class_="calendarToggleCell")[3].text.strip()
                Event = chunk.find('td', class_="calendarToggleCell text-left").text.strip().replace("\n", "")
                impact = chunk.find_all('div')[4].text.strip().replace("\n", "")
                try:
                    source = chunk.find("a", class_="calendar-event-link")['href']
                except:
                    source = 'None'     
                result.append([formatted_date,country,Symbol,Event,impact,Previous, Consensus,Actual,id_news,source])
                link_and_id.append((source,id_news))
        return(result,link_and_id)


    @staticmethod
    def Parser_this_and_next_week_start()->list:
        htmls = asyncio.run(Parser_this_and_next_week.main())
        result,link_and_id = Parser_this_and_next_week.analisis_html(htmls)
        insert_to_DB.insert_to_DB_methods('insert_simple_data', result)    # Вставляем данные в БД
        return link_and_id

# print(Parser_this_and_next_week.Parser_this_and_next_week_start())
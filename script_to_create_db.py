import sqlite3

def create_db():
    # Установка соединения с базой данных
    conn = sqlite3.connect('DB_ekonom_calndar.db')
    cursor = conn.cursor()

    # Создание таблицы
    cursor.execute('''
        CREATE TABLE news (
            id INTEGER PRIMARY KEY,
            Date DATETIME,
            Country VARCHAR(30),
            Symbol VARCHAR(30),
            Event VARCHAR(50),
            Impact VARCHAR(20),
            Previous VARCHAR(20),
            Consensus VARCHAR(20),
            Actual VARCHAR(20),
            Content TEXT,
            Historical_data TEXT,
            News_source VARCHAR(20),
            Link_to_the_source VARCHAR(30),
            id_news INTEGER,
            link_to_news VARCHAR(40)
        )
    ''')

    # Сохранение изменений и закрытие соединения
    conn.commit()
    conn.close()

create_db()
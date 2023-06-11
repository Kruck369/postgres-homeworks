"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv
from io import StringIO


def process_csv_content(csv_content, cursor, table_name):
    """Обрабатывает файлы csv, и добавляет информацию в таблицу postgres"""
    csv_file = StringIO(csv_content)
    reader = csv.reader(csv_file)
    headers = next(reader)

    for row in reader:
        placeholders = ', '.join(['%s'] * len(row))
        query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"
        cursor.execute(query, row)


def main():
    """Основной код программы"""
# Присоединяемся к базе данных
    conn = psycopg2.connect(
        host="localhost",
        database="north",
        user="postgres",
        password="65123891Sad@"
    )
    cur = conn.cursor()

# Создаем два списка с файлами и названиями таблиц
    csv_files = ['./north_data/employees_data.csv', './north_data/customers_data.csv', './north_data/orders_data.csv']
    table_names = ['employees', 'customers', 'orders']

# Открываем файлы и получаем информацию
    for csv_file_path, table_name in zip(csv_files, table_names):
        with open(csv_file_path, 'r', encoding='cp1251') as file:
            csv_content = file.read()

# Обрабатываем файлы с данными и добавляем информацию в таблицы
        process_csv_content(csv_content, cur, table_name)

# Отправляем данные в базу данных, закрываем курсор, закрываем соединение
    conn.commit()
    cur.close()
    conn.close()


# Выполняем основной код программы
if __name__ == '__main__':
    main()

import json
import psycopg2
import pandas as pd
import sys


def get_config(path='db_config.json'):
	"""Чтение файла с параметрами подключения к базе данных"""
	try:
		with open(path, 'r') as f:
			return json.load(f)
	except FileNotFoundError: 
		print(f'ОШИБКА: Файл конфигурации {path} не найден')
		sys.exit()


def get_connection(config):
	"""Подключение к базе данных"""
	try: 
		return psycopg2.connect(**config)
	except:
		print('ОШИБКА: Не удалось подключиться к базе данных')
		sys.exit()


def close(connection=None, cursor=None):
	"""Закрытие подключения к базе данных"""
	if cursor:
		cursor.close()
	if connection:
		connection.close()


def drop_table(connection, cursor, table_name):
	"""Удаление таблицы table_name в текущей схеме"""
	cursor.execute(f"""DROP TABLE IF EXISTS {table_name};""")
	connection.commit()


def drop_schema(connection, cursor, schema_name):
	"""Удаление схемы schema_name"""
	cursor.execute(f"""DROP SCHEMA IF EXISTS {schema_name} CASCADE;""")
	connection.commit()


def create_and_set_schema(connection, cursor, schema_name, replace=False):
	"""
	Создание схемы schema_name, если не существует.
	Установка схемы schema_name как схемы по умолчанию.

	Примечание: установить replace=True, чтобы сначала удалить схему, если она существует,
	а затем создать заново.
	"""
	if replace:
		drop_schema(connection, cursor, schema_name)
	cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
	cursor.execute(f"SET SEARCH_PATH TO {schema_name};")
	connection.commit()


def check_if_empty_table(cursor, table_name):
	"""
	Проверить, является ли таблица table_name пустой.
	Возвращает True, если пустая; иначе - False.
	"""
	cursor.execute(f"""SELECT * FROM {table_name} LIMIT 1;""")
	record = cursor.fetchone()
	return False if record else True


from py_scripts.database import *
from sqlalchemy import create_engine
from datetime import datetime
import os
import re
import shutil


def create_file_processing_log(connection, cursor, replace=False):
	"""Создание таблицы META_FILE_PROCESSING_LOG для работы приходящими файлами"""
	if replace:
		drop_table(connection, cursor, 'META_FILE_PROCESSING_LOG')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS META_FILE_PROCESSING_LOG (
			id SERIAL PRIMARY KEY,
			file_name VARCHAR(128), 
			info_type VARCHAR(128), 
			data_format VARCHAR(128), 
			file_date VARCHAR(8),
			file_date_computed DATE,
			create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			processing_dt TIMESTAMP DEFAULT NULL,
			error VARCHAR(128) DEFAULT NULL
		);
	""")
	connection.commit()


def set_error_unprocessed(connection, cursor):
	"""
	Проставление ошибки на записи в таблице META_FILE_PROCESSING_LOG, 
	которые не были обработаны во время предыдущей итерации.
	"""
	cursor.execute("""
		SELECT id
		FROM META_FILE_PROCESSING_LOG 
		WHERE processing_dt IS NULL 
			AND error IS NULL;
	""")
	records = cursor.fetchall()
	if records:
		for record in records:
			set_error(connection, cursor, error='Обработка была прервана во время предыдущей итерации', entity_id=record[0])


def add_file_entity(connection, cursor, name_obj, create_dt):
	"""Добавление информации об новом файле в таблицу META_FILE_PROCESSING_LOG"""
	cursor.execute("""
		INSERT INTO META_FILE_PROCESSING_LOG (file_name, info_type, data_format, file_date, create_dt)
		VALUES (%s, %s, %s, %s, %s)
		RETURNING id;
	""", [name_obj.group(0), name_obj.group(1), name_obj.group(3), name_obj.group(2), create_dt])
	record = cursor.fetchone()
	connection.commit()
	return record[0]


def set_file_date_computed(connection, cursor, date, entity_id):
	"""Запись обработанной даты файла (date) в таблицу META_FILE_PROCESSING_LOG по id сущности (entity_id)"""
	cursor.execute("""
		UPDATE META_FILE_PROCESSING_LOG
		SET file_date_computed = %s
		WHERE id = %s;
	""", [date, entity_id])
	connection.commit()


def set_processing_dt(connection, cursor, entity_id):
	"""Запись даты успешной обработки файла (date) в таблицу META_FILE_PROCESSING_LOG по id сущности (entity_id)"""
	cursor.execute("""
		UPDATE META_FILE_PROCESSING_LOG
		SET processing_dt = %s
		WHERE id = %s;
	""", [datetime.now(), entity_id])
	connection.commit()


def set_error(connection, cursor, error, entity_id):
	"""Запись текста ошибки (error) в таблицу META_FILE_PROCESSING_LOG по id сущности (entity_id)"""
	cursor.execute("""
		UPDATE META_FILE_PROCESSING_LOG
		SET error = %s, processing_dt = %s
		WHERE id = %s;
	""", [error, datetime.now(), entity_id])
	connection.commit()


def get_last_terminal_update_dt(cursor):
	"""Возвращает дату последнего апдейта таблицы DWH_DIM_TERMINALS_HIST"""
	cursor.execute("""SELECT MAX(effective_from) FROM DWH_DIM_TERMINALS_HIST;""")
	last_update = cursor.fetchone()[0]
	return last_update if last_update else datetime(1900, 1, 1)


def check_file_date(connection, cursor, file_info):
	"""
	Проверка корректности даты в названии файла. 
	Запись даты и/или запись ошибки в случае некорректной даты:
		-- набор цифр, не являющийся датой в установленном формате
		-- дата из будущего времени
		-- (для terminals) в таблице уже есть более актуальные данные
	"""
	try:
		# Попытка преобразовать число в названии файла в дату
		date_computed = datetime.strptime(file_info['file_date'], '%d%m%Y')
		set_file_date_computed(connection, cursor, date=date_computed, entity_id=file_info['id'])

		# Проверка на дату из будущего времени
		if date_computed > datetime.now():
			set_error(connection, cursor, error='Дата файла из будущего времени', entity_id=file_info['id'])
			move_to_archive(file_info['file_name'], archive_dir='archive\\error\\')

		# Проверка даты на актуальность (для таблицы terminals)
		if file_info['info_type'] == 'terminals':
			last_update = get_last_terminal_update_dt(cursor)
			if date_computed <= last_update:
				set_error(connection, cursor, 
					error='В таблице DWH_DIM_TERMINALS_HIST присутствуют более актуальные данные', 
					entity_id=file_info['id']
					)
				move_to_archive(file_info['file_name'], archive_dir='archive\\error\\')

	except ValueError as e:
		set_error(connection, cursor, error='Неверная дата в названии файла', entity_id=file_info['id'])
		move_to_archive(file_info['file_name'], archive_dir='archive\\error\\')


def get_file_info_from_dir(connection, cursor, mask, path='data'):
	"""
	Запись в таблицу META_FILE_PROCESSING_LOG информации о новых файлах для обработки из директории path.
	По умолчанию path = папка data
	Отбор происходит по маске (mask).
	Если дата в названии файла не соответствует требованиям, 
	в таблице META_FILE_PROCESSING_LOG фиксируется соответствующая ошибка.
	"""
	files = os.listdir(path)
	create_dt = datetime.now()

	for file_name in files:
		name_obj = re.fullmatch(mask, file_name)
		if name_obj:
			file_info = {
				'id': add_file_entity(connection, cursor, name_obj, create_dt),
				'file_name': name_obj.group(0),
				'info_type': name_obj.group(1),
				'data_format': name_obj.group(3),
				'file_date': name_obj.group(2)
			}
			check_file_date(connection, cursor, file_info)


def data2sql(connection, cursor, file_info, config, table_name, schema_name, data_path='data\\'):
	"""Считывание данных из csv/txt/xlsx и сохранение во временную таблицу"""
	try:
		connect_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
		alch_connection = create_engine(connect_str)

		file_path = data_path + file_info['file_name']
		if file_info['data_format'] == 'xlsx':
			df = pd.read_excel(file_path)
		elif file_info['data_format'] == 'txt' or file_info['data_format'] == 'csv':
			df = pd.read_csv(file_path, sep=';')
		else:
			raise Exception
		df.to_sql(name=table_name, con=alch_connection, schema=schema_name, if_exists="replace", index=False)
	except Exception:
		set_error(connection, cursor, error='Не удалось считать данные', entity_id=file_info['id'])
		move_to_archive(file_path, archive_dir='archive\\error\\')


def move_to_archive(file_name, archive_dir='archive\\', data_dir='data\\'):
	"""
	Перемещение файла в архив. 
	При наличии файла с таким же названием файл будет перезаписан.
	Простанство для доработки: 
		- добавить постфикс для нумерации файлов с одинаковыми названиями.
	"""
	if not os.path.exists(archive_dir):
		os.makedirs(archive_dir)

	from_path = data_dir + file_name
	to_path = archive_dir + file_name + '.backup'
	try:
		shutil.move(from_path, to_path)
		print(f'Файл {file_name} перемещен в папку {archive_dir}')
	except Exception:
		print(f'Файл {file_name} не удалось переместить в архив: файл не найден или произошла другая непредвиденная ошибка')
	

def get_candidate_to_process(cursor):
	"""
	Получение кандидата для последующей обработки.
	Выбирается необработанный файл с самой ранней датой, по алфавиту.
	"""
	cursor.execute("""
		SELECT *
		FROM META_FILE_PROCESSING_LOG 
		WHERE processing_dt IS NULL 
			AND error IS NULL
		ORDER BY file_date_computed, file_name;
	""")
	record = cursor.fetchone()
	return {
		'id': record[0],
		'file_name': record[1],
		'info_type': record[2],
		'data_format': record[3],
		'file_date': record[5]
	} if record else None


def show_latest_errors(cursor, start_dt):
	"""Вывести в консоль ошибки при последней загрузки файлов"""
	cursor.execute("""
		SELECT file_name, error
		FROM META_FILE_PROCESSING_LOG
		WHERE error IS NOT NULL 
			AND create_dt = (SELECT MAX(create_dt) FROM META_FILE_PROCESSING_LOG)
			AND create_dt >= %s::TIMESTAMP
		ORDER BY file_name;
	""", [start_dt])
	records = cursor.fetchall()
	if records:
		print('Ошибки, возникшие в процессе обработки файлов:')
		for record in records:
			print(f'\tФайл {record[0]}: {record[1]}')
	else:
		print('Ошибок в процессе обработки файлов не возникло')


def find_files_to_process(connection, cursor, file_mask):
	"""
	Поиск новых файлов для обработки + запись найденной информации в таблицу META_FILE_PROCESSING_LOG

	Примечание: 
		-- если таблицы META_FILE_PROCESSING_LOG не существует, она будет создана;
		-- если с прошлого выполнения программы остались необработанные файлы, им будет выставлена ошибка.
	"""
	create_file_processing_log(connection, cursor, replace=False)
	set_error_unprocessed(connection, cursor)

	get_file_info_from_dir(connection, cursor, mask=file_mask)


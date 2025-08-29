from py_scripts.database import *
from py_scripts.file_processing import *
from datetime import datetime


def update_passport_blacklist(connection, cursor):
	"""Обновленые данных о 'черном списке'"""
	cursor.execute("""
		INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (passport_num, entry_dt)
		SELECT 
			t1.passport::VARCHAR, 
			t1.date::DATE
		FROM STG_TMP_LOADED t1
		LEFT JOIN DWH_FACT_PASSPORT_BLACKLIST t2
			ON t1.passport::VARCHAR = t2.passport_num
		WHERE t2.passport_num IS NULL;
	""")
	connection.commit()


def update_transactions(connection, cursor):
	"""Обновленые данных о транзакциях"""
	cursor.execute("""
		INSERT INTO DWH_FACT_TRANSACTIONS (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
		SELECT 
			t1.transaction_id::VARCHAR, 
			t1.transaction_date::TIMESTAMP, 
			t1.card_num::VARCHAR, 
			t1.oper_type::VARCHAR, 
			replace(t1.amount, ',', '.')::NUMERIC, 
			t1.oper_result::VARCHAR, 
			t1.terminal::VARCHAR
		FROM STG_TMP_LOADED t1
		LEFT JOIN DWH_FACT_TRANSACTIONS t2
			ON t1.transaction_id::VARCHAR = t2.trans_id
		WHERE t2.trans_id IS NULL;
	""")
	connection.commit()


def get_new_terminals(connection, cursor):
	"""
	Поиск информации о новых терминалах по следующим условиям:
		-- terminal_id есть в STG_TMP_LOADED;
		-- terminal_id отсутствует в DWH_DIM_TERMINALS_HIST
	"""
	drop_table(connection, cursor, 'STG_TMP_NEW_TERMINALS')
	cursor.execute("""
		CREATE TABLE STG_TMP_NEW_TERMINALS AS
			SELECT 
				t1.terminal_id::VARCHAR, 
				t1.terminal_type::VARCHAR,
				t1.terminal_city::VARCHAR,
				t1.terminal_address::VARCHAR
			FROM STG_TMP_LOADED t1
			LEFT JOIN DWH_DIM_TERMINALS_HIST t2
				ON t1.terminal_id::VARCHAR = t2.terminal_id
			WHERE t2.terminal_id IS NULL;
	""")
	connection.commit()


def get_updated_terminals(connection, cursor):
	"""
	Поиск терминалов, информация о которых изменилась по следующим условиям:
		-- terminal_id есть в STG_TMP_LOADED;
		-- terminal_id есть в DWH_DIM_TERMINALS_HIST, при этом: 
			-- effective_to = '2999-12-31 23:59:59';
			-- хотя бы одно из бизнес-полей поменяло значение 
				ИЛИ актуальная запись имеет deleted_flg = 1
	"""
	drop_table(connection, cursor, 'STG_TMP_UPDATED_TERMINALS')
	cursor.execute("""
		CREATE TABLE STG_TMP_UPDATED_TERMINALS AS
			SELECT 
				t1.terminal_id::VARCHAR, 
				t1.terminal_type::VARCHAR,
				t1.terminal_city::VARCHAR,
				t1.terminal_address::VARCHAR
			FROM STG_TMP_LOADED t1
			INNER JOIN DWH_DIM_TERMINALS_HIST t2
				ON t1.terminal_id::VARCHAR = t2.terminal_id
			WHERE t2.effective_to = '2999-12-31 23:59:59'
				AND (
					t1.terminal_type::VARCHAR <> t2.terminal_type
					OR t1.terminal_city::VARCHAR <> t2.terminal_city
					OR t1.terminal_address::VARCHAR <> t2.terminal_address
					OR t2.deleted_flg = 1
				);
	""")
	connection.commit()


def get_deleted_terminals(connection, cursor):
	"""
	Поиск информации об удаленных терминалах по следующим условиям:
		-- terminal_id отсутствует в STG_TMP_LOADED;
		-- terminal_id есть в DWH_DIM_TERMINALS_HIST, при этом:
			-- effective_to = '2999-12-31 23:59:59';
			-- актуальная запись имеет deleted_flg = 0
	"""
	drop_table(connection, cursor, 'STG_TMP_DELETED_TERMINALS')
	cursor.execute("""
		CREATE TABLE STG_TMP_DELETED_TERMINALS AS
			SELECT 
				t1.terminal_id, 
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM DWH_DIM_TERMINALS_HIST t1
			LEFT JOIN STG_TMP_LOADED t2
				ON t1.terminal_id = t2.terminal_id::VARCHAR
			WHERE t1.effective_to = '2999-12-31 23:59:59'
				AND t1.deleted_flg = 0
				AND t2.terminal_id IS NULL;
	""")
	connection.commit()


def add_terminals_records(connection, cursor, file_date, source_table_name, deleted_flg=0):
	"""
	Добавление новых строк в DWH_DIM_TERMINALS_HIST из таблицы source_table_name.
	Дата effective_from проставляется исходя из даты файла file_date.
	"""
	cursor.execute(f"""
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id, 
			terminal_type, 
			terminal_city, 
			terminal_address, 
			effective_from, 
			deleted_flg)
		SELECT 
			terminal_id, 
			terminal_type, 
			terminal_city, 
			terminal_address, 
			'{file_date.strftime('%Y-%m-%d')}'::TIMESTAMP, 
			{deleted_flg}
		FROM {source_table_name};
	""")
	connection.commit()


def set_terminals_effective_to(connection, cursor, file_date, source_table_name):
	"""
	Проставление даты effective_to строкам таблицы DWH_DIM_TERMINALS_HIST.
	Выбор строк происходит по terminal_id из таблицы source_table_name.
	"""
	cursor.execute(f"""
		UPDATE DWH_DIM_TERMINALS_HIST
		SET effective_to = '{file_date.strftime('%Y-%m-%d')}'::TIMESTAMP - INTERVAL '1 second'
		WHERE effective_to = '2999-12-31 23:59:59'
			AND terminal_id in (SELECT terminal_id FROM {source_table_name});
	""")
	connection.commit()


def update_terminals(connection, cursor, file_date):
	"""Обновленые данных о терминалах"""

	# Выбор данных: новые, измененные, удаленные
	get_new_terminals(connection, cursor)
	get_updated_terminals(connection, cursor)
	get_deleted_terminals(connection, cursor)

	# Добавление новых записей в таблицу DWH_DIM_TERMINALS_HIST
	add_terminals_records(connection, cursor, file_date, source_table_name='STG_TMP_NEW_TERMINALS')

	# Обновление измененных записей в таблице DWH_DIM_TERMINALS_HIST
	set_terminals_effective_to(connection, cursor, file_date, source_table_name='STG_TMP_UPDATED_TERMINALS')
	add_terminals_records(connection, cursor, file_date, source_table_name='STG_TMP_UPDATED_TERMINALS')

	# Удаление записей из таблицы DWH_DIM_TERMINALS_HIST
	set_terminals_effective_to(connection, cursor, file_date, source_table_name='STG_TMP_DELETED_TERMINALS')
	add_terminals_records(connection, cursor, file_date, source_table_name='STG_TMP_DELETED_TERMINALS', deleted_flg=1)
	
	# Удаление временных таблиц
	drop_table(connection, cursor, 'STG_TMP_NEW_TERMINALS')
	drop_table(connection, cursor, 'STG_TMP_UPDATED_TERMINALS')
	drop_table(connection, cursor, 'STG_TMP_DELETED_TERMINALS')


def update_dwh_table_from_tmp(connection, cursor, file_info):
	"""
	Обновление рабочих таблиц на основе временной таблицы stg_tmp_loaded, 
	в которую помещается информация из обрабатываемого на данный момент файла.
	"""
	if file_info['info_type'] == 'passport_blacklist':
		update_passport_blacklist(connection, cursor)

	elif file_info['info_type'] == 'transactions':
		update_transactions(connection, cursor)

	elif file_info['info_type'] == 'terminals':
		update_terminals(connection, cursor, file_date=file_info['file_date'])

	print(f'Файл {file_info['file_name']} обработан')

	set_processing_dt(connection, cursor, entity_id=file_info['id'])
	move_to_archive(file_info['file_name'])
	drop_table(connection, cursor, 'stg_tmp_loaded')


def update_from_files(connection, cursor, config, schema_name):
	"""
	Обработка новых файлов:
		-- Фиксирование кандидатов на обработку в таблицу META_FILE_PROCESSING_LOG
		-- Выбор кандидата на обработку и обработка соответствующего файла
			-- Получение информации о кандидате для обработки из таблицы META_FILE_PROCESSING_LOG
			-- Сохранение данных из файла во временную таблицу
			-- Обновление соответствующей таблицы
	"""
	file_mask = r'(transactions|passport_blacklist|terminals)_(\d{8})\.(txt|csv|xlsx)'
	find_files_to_process(connection, cursor, file_mask=file_mask)

	while True:
		file_info = get_candidate_to_process(cursor)
		if file_info:
			data2sql(connection, cursor, file_info, config, table_name='stg_tmp_loaded', schema_name=schema_name)
			update_dwh_table_from_tmp(connection, cursor, file_info)
		else:
			break


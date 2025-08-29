import io
from py_scripts.database import *


def read_sql(path):
	"""Считать текст из txt/sql файла"""
	with io.open(path, encoding='utf-8') as file:
		text = file.read()
	return text


def execute_from_file(connection, cursor, path):
	"""Выполнить sql-запрос из файла"""
	text = read_sql(path)
	cursor.execute(text)
	connection.commit()


def create_transactions(connection, cursor, replace=False):
	"""Создание таблицы с информацией о транзакциях"""
	if replace:
		drop_table(connection, cursor, 'DWH_FACT_TRANSACTIONS')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS (
			trans_id VARCHAR(128),
			trans_date TIMESTAMP,
			card_num VARCHAR(128),
			oper_type VARCHAR(128),
			amt DECIMAL(14,2),
			oper_result VARCHAR(128),
			terminal VARCHAR(128)
		);
	""")
	connection.commit()


def create_passport_blacklist(connection, cursor, replace=False):
	"""Создание таблицы с информацией о паспортах, находящихся в черном списке"""
	if replace:
		drop_table(connection, cursor, 'DWH_FACT_PASSPORT_BLACKLIST')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS DWH_FACT_PASSPORT_BLACKLIST (
			passport_num VARCHAR(128),
			entry_dt DATE
		);
	""")
	connection.commit()


def create_terminals(connection, cursor, replace=False):
	"""Создание таблицы с информацией о терминалах"""
	if replace:
		drop_table(connection, cursor, 'DWH_DIM_TERMINALS_HIST')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS DWH_DIM_TERMINALS_HIST (
			terminal_id VARCHAR(128),
			terminal_type VARCHAR(128),
			terminal_city VARCHAR(128),
			terminal_address VARCHAR(128),
			effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			effective_to TIMESTAMP DEFAULT '2999-12-31 23:59:59',
			deleted_flg NUMERIC(1) DEFAULT 0
		);
	""")
	connection.commit()


def create_cards(connection, cursor, replace=False):
	"""Создание таблицы с информацией о картах клиентов"""
	if replace:
		drop_table(connection, cursor, 'STG_CARDS')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS STG_CARDS (
			card_num VARCHAR(128), 
			account VARCHAR(128), 
			create_dt DATE,
			update_dt DATE
		);
	""")
	connection.commit()


def create_accounts(connection, cursor, replace=False):
	"""Создание таблицы с информацией о счетах клиентов"""
	if replace:
		drop_table(connection, cursor, 'STG_ACCOUNTS')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS STG_ACCOUNTS (
			account VARCHAR(128), 
			valid_to DATE, 
			client VARCHAR(128),
			create_dt DATE, 
			update_dt DATE
		);
	""")
	connection.commit()


def create_clients(connection, cursor, replace=False):
	"""Создание таблицы с информацией о клиентах"""
	if replace:
		drop_table(connection, cursor, 'STG_CLIENTS')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS STG_CLIENTS (
			client_id VARCHAR(128), 
			last_name VARCHAR(128), 
			first_name VARCHAR(128), 
			patronymic VARCHAR(128), 
			date_of_birth DATE, 
			passport_num VARCHAR(128), 
			passport_valid_to DATE, 
			phone VARCHAR(128),
			create_dt DATE, 
			update_dt DATE
		);
	""")
	connection.commit()


def create_rep_fraud(connection, cursor, replace=False):
	"""Создание таблицы для хранения отчета о мошеннических операциях"""
	if replace:
		drop_table(connection, cursor, 'REP_FRAUD')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS REP_FRAUD (
			event_dt TIMESTAMP, 
			passport VARCHAR(128),
			fio VARCHAR(128),
			phone VARCHAR(128),
			event_type VARCHAR(128),
			report_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	""")
	connection.commit()


def create_fraud_types(connection, cursor, replace=False):
	"""Создание таблицы с описанием типов мошенничества"""
	if replace:
		drop_table(connection, cursor, 'META_FRAUD_TYPES')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS META_FRAUD_TYPES (
			fraud_type_id INT,
			fraud_type VARCHAR(128)
		);
	""")
	connection.commit()


def create_fraud_hist(connection, cursor, replace=False):
	"""Создание таблицы с историей найденных мошеннических транзакций"""
	if replace:
		drop_table(connection, cursor, 'DWH_DIM_FRAUD')
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS DWH_DIM_FRAUD (
			trans_id VARCHAR(128),
			trans_date TIMESTAMP,
			fraud_type_id INT,
			create_dt DATE DEFAULT CURRENT_TIMESTAMP, 
			update_dt DATE
		);
	""")
	connection.commit()


def recreate_test_data(connection, cursor, schema_name, replace=False):
	"""
	Создание схемы и всех необходимых таблиц проекта:
		Заполняются из файлов .sql:
			STG_CARDS
			STG_ACCOUNTS
			STG_CLIENTS
		Создаются пустыми:
			DWH_FACT_TRANSACTIONS
			DWH_FACT_PASSPORT_BLACKLIST
			DWH_DIM_TERMINALS_HIST
			REP_FRAUD
	+ типы мошенничества META_FRAUD_TYPES
	+ история поиска мошеннических транзакций DWH_DIM_FRAUD
	"""
	create_and_set_schema(connection, cursor, schema_name, replace=replace)

	create_clients(connection, cursor, replace=replace)
	create_cards(connection, cursor, replace=replace)
	create_accounts(connection, cursor, replace=replace)

	if check_if_empty_table(cursor, 'STG_CARDS'):
		execute_from_file(connection, cursor, path='sql_scripts\\insert_cards.sql')
	if check_if_empty_table(cursor, 'STG_ACCOUNTS'):
		execute_from_file(connection, cursor, path='sql_scripts\\insert_accounts.sql')
	if check_if_empty_table(cursor, 'STG_CLIENTS'):
		execute_from_file(connection, cursor, path='sql_scripts\\insert_clients.sql')
	
	create_transactions(connection, cursor, replace=replace)
	create_passport_blacklist(connection, cursor, replace=replace)
	create_terminals(connection, cursor, replace=replace)

	create_fraud_hist(connection, cursor, replace=replace)
	create_rep_fraud(connection, cursor, replace=replace)
	create_fraud_types(connection, cursor, replace=replace)
	if check_if_empty_table(cursor, 'META_FRAUD_TYPES'):
		execute_from_file(connection, cursor, path='sql_scripts\\insert_fraud_types.sql')
	

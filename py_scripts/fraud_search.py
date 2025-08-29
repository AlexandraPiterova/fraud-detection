from py_scripts.database import drop_table
import datetime
import json


def find_passport_expired(connection, cursor, start_dt, end_dt, fraud_type_id=1):
	"""
	Поиск операций, совершенных при просроченном паспорте.
	Подразумевается, что на дату, указанную в поле passport_valid_to, паспорт еще действителен.
	"""
	cursor.execute("""
		INSERT INTO DWH_DIM_FRAUD (trans_id, trans_date, fraud_type_id)
		SELECT t1.trans_id, t1.trans_date, %s
		FROM DWH_FACT_TRANSACTIONS t1
		INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
		INNER JOIN STG_ACCOUNTS t3 ON t2.account = t3.account
		INNER JOIN STG_CLIENTS t4 ON t3.client = t4.client_id
		WHERE t1.trans_date >= %s::TIMESTAMP 
			AND t1.trans_date < %s::TIMESTAMP
			AND t1.trans_date >= t4.passport_valid_to + INTERVAL '1 day'
			AND t1.trans_id NOT IN (SELECT trans_id FROM DWH_DIM_FRAUD WHERE fraud_type_id = %s);
	""", [fraud_type_id, start_dt, end_dt, fraud_type_id])
	connection.commit()


def find_passport_blocked(connection, cursor, start_dt, end_dt, fraud_type_id=1):
	"""Поиск операций, совершенных при заблокированном паспорте"""
	cursor.execute("""
		INSERT INTO DWH_DIM_FRAUD (trans_id, trans_date, fraud_type_id)
		SELECT t1.trans_id, t1.trans_date, %s
		FROM DWH_FACT_TRANSACTIONS t1
		INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
		INNER JOIN STG_ACCOUNTS t3 ON t2.account = t3.account
		INNER JOIN STG_CLIENTS t4 ON t3.client = t4.client_id
		INNER JOIN DWH_FACT_PASSPORT_BLACKLIST t5 ON t4.passport_num = t5.passport_num
		WHERE t1.trans_date >= %s::TIMESTAMP 
			AND t1.trans_date < %s::TIMESTAMP
			AND t1.trans_id NOT IN (SELECT trans_id FROM DWH_DIM_FRAUD WHERE fraud_type_id = %s);
	""", [fraud_type_id, start_dt, end_dt, fraud_type_id])
	connection.commit()


def find_contract_expired(connection, cursor, start_dt, end_dt, fraud_type_id=2):
	"""
	Поиск операций, совершенных при недействующем договоре.
	Подразумевается, что на дату, указанную в поле valid_to, договор еще действителен.
	"""
	cursor.execute("""
		INSERT INTO DWH_DIM_FRAUD (trans_id, trans_date, fraud_type_id)
		SELECT t1.trans_id, t1.trans_date, %s
		FROM DWH_FACT_TRANSACTIONS t1
		INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
		INNER JOIN STG_ACCOUNTS t3 ON t2.account = t3.account
		WHERE t1.trans_date >= %s::TIMESTAMP 
			AND t1.trans_date < %s::TIMESTAMP
			AND t1.trans_date >= t3.valid_to + INTERVAL '1 day'
			AND t1.trans_id NOT IN (SELECT trans_id FROM DWH_DIM_FRAUD WHERE fraud_type_id = %s);
	""", [fraud_type_id, start_dt, end_dt, fraud_type_id])
	connection.commit()


def find_different_cities(connection, cursor, start_dt, end_dt, fraud_type_id=3):
	"""Поиск операций совершенных в разных городах в течение одного часа"""
	cursor.execute("""
		INSERT INTO DWH_DIM_FRAUD (trans_id, trans_date, fraud_type_id)
		SELECT t.trans_id, t.trans_date, %s
		FROM (
			SELECT 
				t1.trans_id, 
				t2.account,
				t1.trans_date, 
				LAG(t1.trans_date) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) previous_trans_date,
				t3.terminal_city, 
				LAG(t3.terminal_city) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) previous_terminal_city	
			FROM DWH_FACT_TRANSACTIONS t1
			INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
			INNER JOIN DWH_DIM_TERMINALS_HIST t3
				ON t1.terminal = t3.terminal_id 
					AND t1.trans_date BETWEEN t3.effective_from AND t3.effective_to
			WHERE t1.trans_date >= %s::TIMESTAMP - INTERVAL '1 hour' 
				AND t1.trans_date < %s::TIMESTAMP
		) t 
		WHERE t.terminal_city <> t.previous_terminal_city
			AND EXTRACT(EPOCH FROM (trans_date - previous_trans_date))/60 <= 60
			AND t.trans_id NOT IN (SELECT trans_id FROM DWH_DIM_FRAUD WHERE fraud_type_id = %s);
	""", [fraud_type_id, start_dt, end_dt, fraud_type_id])
	connection.commit()


def find_amt_selection(connection, cursor, start_dt, end_dt, fraud_type_id=4):
	"""Поиск ряда из трех операций, похожих на подбор суммы при снятии или оплате"""
	cursor.execute("""
		INSERT INTO DWH_DIM_FRAUD (trans_id, trans_date, fraud_type_id)
		SELECT t.trans_id, t.trans_date_3, %s
		FROM (
			SELECT 
				t1.trans_id,
				t2.account,
				LAG(t1.trans_date, 2) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) trans_date_1,
				LAG(t1.trans_date, 1) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) trans_date_2,
				t1.trans_date trans_date_3, 
				LAG(t1.amt, 2) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) amt_1,
				LAG(t1.amt, 1) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) amt_2,
				t1.amt amt_3, 
				LAG(t1.oper_type, 2) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) oper_type_1,
				LAG(t1.oper_type, 1) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) oper_type_2,
				t1.oper_type oper_type_3,
				LAG(t1.oper_result, 2) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) oper_result_1,
				LAG(t1.oper_result, 1) OVER (PARTITION BY t2.account ORDER BY t2.account, t1.trans_date) oper_result_2,
				t1.oper_result oper_result_3
			FROM DWH_FACT_TRANSACTIONS t1
			INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
			WHERE t1.oper_type IN ('PAYMENT', 'WITHDRAW')
				AND t1.trans_date >= %s::TIMESTAMP - INTERVAL '20 minutes' 
				AND t1.trans_date < %s::TIMESTAMP
		) t 
		WHERE EXTRACT(EPOCH FROM (trans_date_3 - trans_date_1))/60 <= 20
			AND t.amt_1 > t.amt_2 AND t.amt_2 > t.amt_3
			AND t.oper_result_1 = 'REJECT' AND t.oper_result_2 = 'REJECT' AND t.oper_result_3 = 'SUCCESS'
			AND t.trans_id NOT IN (SELECT trans_id FROM DWH_DIM_FRAUD WHERE fraud_type_id = %s);		
	""", [fraud_type_id, start_dt, end_dt, fraud_type_id])
	connection.commit()


def delete_rep_fraud_records(connection, cursor, start_dt, end_dt):
	"""Удаление записей из таблицы REP_FRAUD за заданный период"""
	cursor.execute("""
		DELETE FROM REP_FRAUD
		WHERE event_dt >= %s::TIMESTAMP
			AND event_dt < %s::TIMESTAMP;
	""", [start_dt, end_dt])
	connection.commit()


def add_rep_fraud_records(connection, cursor, start_dt, end_dt):
	"""Добавление новых записей в таблицу REP_FRAUD из таблицы DWH_DIM_FRAUD"""
	cursor.execute("""
		INSERT INTO REP_FRAUD (event_dt, passport, fio, phone, event_type)
		SELECT 
			t1.trans_date, 
			t4.passport_num,
			CONCAT_WS(' ', t4.last_name, t4.first_name, t4.patronymic), 
			t4.phone,
			t5.fraud_type
		FROM DWH_DIM_FRAUD t0
		INNER JOIN DWH_FACT_TRANSACTIONS t1 ON t0.trans_id = t1.trans_id 
		INNER JOIN STG_CARDS t2 ON t1.card_num = t2.card_num
		INNER JOIN STG_ACCOUNTS t3 ON t2.account = t3.account
		INNER JOIN STG_CLIENTS t4 ON t3.client = t4.client_id
		INNER JOIN META_FRAUD_TYPES t5 ON t0.fraud_type_id = t5.fraud_type_id
		WHERE t0.trans_date >= %s::TIMESTAMP 
			AND t0.trans_date < %s::TIMESTAMP;
	""", [start_dt, end_dt])
	connection.commit()


def get_last_transaction_update_dt(cursor):
	"""Возвращает дату последнего апдейта таблицы DWH_FACT_TRANSACTIONS"""
	cursor.execute("""SELECT MAX(trans_date)::DATE::TIMESTAMP FROM DWH_FACT_TRANSACTIONS;""")
	last_update = cursor.fetchone()[0]
	return last_update


def get_rep_fraud_time_period(cursor, path='date_settings.json'):
	"""Выбор промежутка времени для обновления таблицы REP_FRAUD"""
	last_update = get_last_transaction_update_dt(cursor)
	if not last_update:
		print('Данные о транзакциях отсутствуют, отчет построен не будет')
		return [-1]
	else:
		try:
			with open(path, 'r') as f:
				date_settings = json.load(f)
			if date_settings['is_active'] == '1':
				start_dt = datetime.datetime.strptime(date_settings['start_dt'], '%Y-%m-%d %H:%M:%S')
				end_dt = datetime.datetime.strptime(date_settings['end_dt'], '%Y-%m-%d %H:%M:%S')
				if start_dt < end_dt and start_dt <= last_update:
					if end_dt > last_update + datetime.timedelta(days=1):
						end_dt = last_update + datetime.timedelta(days=1)
					return [start_dt, end_dt]
				else:
					raise Exception
			else:
				raise Exception
		except Exception as e:
			print(f'Файл {path} не найден, отключен или задан некорректно')

			start_dt = last_update
			end_dt = last_update + datetime.timedelta(days=1)
			return [start_dt, end_dt]
		finally:
			print(f'Задан временной период: {start_dt} - {end_dt}')
			

def update_rep_fraud(connection, cursor):
	"""Поиск мошеннических операций и обновление таблицы REP_FRAUD за заданный промежуток времени"""
	time_period = get_rep_fraud_time_period(cursor)

	if time_period[0] != -1:
		start_dt, end_dt = time_period
		find_passport_expired(connection, cursor, start_dt, end_dt)
		find_passport_blocked(connection, cursor, start_dt, end_dt)
		find_contract_expired(connection, cursor, start_dt, end_dt)
		find_different_cities(connection, cursor, start_dt, end_dt)
		find_amt_selection(connection, cursor, start_dt, end_dt)

		delete_rep_fraud_records(connection, cursor, start_dt, end_dt)
		add_rep_fraud_records(connection, cursor, start_dt, end_dt)


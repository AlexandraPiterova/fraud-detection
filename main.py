from py_scripts.database import get_config, get_connection, close
from py_scripts.test_data import recreate_test_data
from py_scripts.dwh_data_update import update_from_files
from py_scripts.file_processing import show_latest_errors
from py_scripts.fraud_search import update_rep_fraud
from datetime import datetime


START_TIMESTAMP = datetime.now()

# Подключиться к БД
config = get_config()
connection = get_connection(config)
cursor = connection.cursor()
schema_name = 'bank'

# Создать/установить схему и создать все основные таблицы, если их еще нет
# Установить replace=True, чтобы дропнуть схему и пересоздать всё с нуля
recreate_test_data(connection, cursor, schema_name, replace=False)

print('>>> ОБРАБОТКА ФАЙЛОВ')
update_from_files(connection, cursor, config, schema_name)
print()
print('Обработка файлов завершена')

# Вывести в консоль ошибки обработки файлов
show_latest_errors(cursor, start_dt=START_TIMESTAMP)
print()

print('>>> ПОСТРОЕНИЕ ОТЧЕТА')
update_rep_fraud(connection, cursor)
print(f"Данные REP_FRAUD обновлены")

# закрыть подключение
close(connection, cursor)


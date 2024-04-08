# Периодичность автоматической синхронизации курса. Cron-формат
# <Минуты> <Часы> <Дни_месяца> <Месяцы> <Дни_недели>
UPDATE_PERIOD = '* * * * *'

# Валюты для которых производится синхронизация.
# Формат: set[cur: str], cur - код валюты прописными латинскими буквами
CURRENCIES = {'rub', 'eur', 'usd', 'jpy'}

# Строка подключения к базе данных
HOST = 'localhost'
PORT = '5432'
DATABASE = 'exchangerates'
TABLE = 'exchanges'
dbURL = 'postgresql+psycopg2://postgres:postgres@{}:{}/{}'.format(HOST, PORT, DATABASE)

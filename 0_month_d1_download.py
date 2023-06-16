from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import time
from datetime import datetime, timedelta
import requests
import io
import yfinance as yf

#Подключение к SQL
POSTGRES_ADDRESS = '87.239.111.194'
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'postgres'
POSTGRES_PASSWORD = 'cCChgl3QJxbbduH3G6bvndPz7hY4zXucWyuB'
POSTGRES_DBNAME = 'PostgreSQL-6115'
engine = create_engine(f'postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DBNAME}')
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}')
connection = psycopg2.connect(user=POSTGRES_USERNAME,
                                  password=POSTGRES_PASSWORD,
                                  host=POSTGRES_ADDRESS,
                                  port=POSTGRES_PORT,
                                  database=POSTGRES_DBNAME)
                                  
connection.autocommit = False

#Переопределение фреймоф
df = pd.DataFrame(columns=['name_t', 'code_t', 'status_t'])
df_yahoo = pd.DataFrame(columns=['name_t', 'code_t', 'status_t'])
df_finam = pd.DataFrame(columns=['name_t', 'code_t', 'status_t'])
df_finam = pd.DataFrame(columns=['name_t', 'code_t', 'status_t'])

#Выгрузим тикеры
query_quotes = 'select name_t, code_t, status_t from dim_quotes where actual_status = 2'
df = pd.read_sql(query_quotes, engine)
df_yahoo = df.query("status_t in ['usa_act', 'china', 'japan', 'singapore', 'hong_kong']")[['name_t', 'code_t', 'status_t']]
df_finam = df.query("status_t in ['russian_act', 'russian_f', 'valut', 'product']")[['name_t', 'code_t', 'status_t']]
df_yahoo.reset_index(drop=True, inplace=True)
df_finam.reset_index(drop=True, inplace=True)

#определяем даты
# Set start and end dates
end = datetime.today().date() + timedelta(days=5)
start = end - timedelta(days=65)

# Convert dates to strings
start_str = start.strftime("%d.%m.%Y")
end_str = end.strftime("%d.%m.%Y")

start_yahoo = datetime.strptime(start_str, '%d.%m.%Y').strftime('%Y-%m-%d')
end_yahoo = datetime.strptime(end_str, '%d.%m.%Y').strftime('%Y-%m-%d')

# Get day, month, and year for start and end dates
start_date = datetime.strptime(start_str, "%d.%m.%Y").date()
end_date = datetime.strptime(end_str, "%d.%m.%Y").date()

day_start = str(start_date.day)
month_start = str(start_date.month - 1)
year_start = str(start_date.year)
day_end = str(end_date.day)
month_end = str(end_date.month - 1)
year_end = str(end_date.year)


# Зададим параметры барузера
header = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0'
    }

#Зададим строку Finam
str1 = "http://export.finam.ru/"
str2 = ".txt?market=1&em="
str3 = "&code="
str4 = "&apply=0&df="
str5 = "&mf="
str6 = "&yf="
str7 = "&from="
str8 = "&dt="
str9 = "&mt="
str10 = "&yt="
str11 = "&to="
str12 = "&p=8&f="
str13 = "&e=.txt&cn="
str14 = "&dtf=1&tmf=1&MSOR=1&mstime=on&mstimever=1&sep=3&sep2=1&datf=1&at=1"


#Запустим скрипты на очистку таблицы
query_trunc = "truncate table quotes_d1_upd"
query_start_upd = "insert into procedure_log values ('start_d1_update',current_timestamp)"
cursor = connection.cursor()
cursor.execute(query_trunc)
cursor.execute(query_start_upd)
connection.commit()


for i in range(len(df_finam)):
    url2 = str1 + str(df_finam.name_t[i]) + str2 + str(df_finam.code_t[i]) + str3 + str(
        df_finam.name_t[i]) + str4 + str(day_start) + str5 + str(month_start) + str6 + str(
        year_start) + str7 + str(start_date) + str8 + str(day_end) + str9 + str(month_end) + str10 + str(
        year_end) + str11 + str(end_date) + str12 + str(df_finam.name_t[i]) + str13 + str(
        df_finam.name_t[i]) + str14
    req = requests.get(url2, headers=header).content

    if not req:
        k = 1
        time.sleep(1.5)
    else:
        df_q = pd.read_csv(io.StringIO(req.decode('utf-8')), sep=';', on_bad_lines='skip')
        time.sleep(1.5)
        df_q.rename(columns={
            '<TICKER>': 'ticker',
            '<PER>': 'per',
            '<DATE>': 'date_t',
            '<TIME>': 'time_t',
            '<OPEN>': 'open_t',
            '<HIGH>': 'high_t',
            '<LOW>': 'low_t',
            '<CLOSE>': 'close_t',
            '<VOL>': 'vol_t'
        }, inplace=True)
        df_q['open_t'] = df_q['open_t'].apply(str).str.replace(',', '')
        df_q['high_t'] = df_q['high_t'].apply(str).str.replace(',', '')
        df_q['low_t'] = df_q['low_t'].apply(str).str.replace(',', '')
        df_q['close_t'] = df_q['close_t'].apply(str).str.replace(',', '')
        df_q['vol_t'] = df_q['vol_t'].apply(str).str.replace(',', '')
        df_q['status'] = df_finam['status_t'][i]
        df_q['date_t'] = pd.to_datetime(df_q['date_t'], format='%Y%m%d')
        df_q = df_q.drop(columns=['per'], axis=1)
        df_q = df_q.drop(columns=['time_t'], axis=1)
        df_q.to_sql(
            name='quotes_d1_upd',
            con=engine,
            index=False,
            if_exists='append'
        )
        time.sleep(0.2)
    engine.dispose()


for i in range(len(df_yahoo)):
    try:
        data = yf.download(df_yahoo['code_t'][i], start_yahoo, end_yahoo, progress = False)
    except:
        k=0
    df_asia = pd.DataFrame.from_dict(data)
    df_asia.reset_index(inplace=True)
    df_asia.rename(columns={
            'Date': 'date_t',
            'Open': 'open_t',
            'High': 'high_t',
            'Low': 'low_t',
            'Close': 'close_t',
            'Adj Close': 'close_t_dop',
            'Volume': 'vol_t'
            }, inplace=True)
    df_asia['ticker'] = df_yahoo['name_t'][i]
    df_asia['status'] = df_yahoo['status_t'][i]
    df_asia = df_asia.drop(columns=['close_t_dop'], axis=1)
    df_asia = df_asia.reindex(columns=['ticker', 'date_t', 'open_t', 'high_t', 'low_t', 'close_t', 'vol_t', 'status'])
    df_asia.to_sql(
        name='quotes_d1_upd',
        con=engine,
        index=False,
        if_exists='append')
connection.commit()

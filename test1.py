from sqlalchemy import create_engine
import psycopg2

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

query_d1_upd = "truncate table test_vps_server1;"

#процедуры расчета
cursor = connection.cursor()
queries_m = [query_d1_upd]
for query in queries_m:
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

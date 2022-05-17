import pymysql
import psycopg2
import pandas as pd

def insert():
    #connection to MySQL
    my_conn = pymysql.connect(host='host.docker.internal',
                             port=3306,
                             user='admin',
                             password='admin',
                             db='db_covid')
    
    #read data from MySQL (data staging)
    temp = pd.read_sql("""
                        select distinct(prov_id), prov_name
                        from tb_covid
                   """, my_conn)
    
    #query clear data in tb_prov
    reset = """
                TRUNCATE TABLE tb_prov CASCADE;
            """

    #query insert data to tb_prov
    insert = """
                insert into tb_prov (prov_id, prov_name)
                values (%s, %s)
             """

    #load data to tb_prov
    conn = None
    try:
        #connection to PostgreSQL
        conn = psycopg2.connect(host='host.docker.internal',
                                dbname="covid",
                                user="airflow",
                                password="airflow",
                                port=5432)

        #run PostgreSQL query
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(reset)
            cursor.executemany(insert, temp.values.tolist())
    finally:
        if conn:
            conn.close()

    print('table "tb_prov" successfully inserted')
    
if __name__ == "__main__":
    insert()
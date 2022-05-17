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
    
    #read data from data staging
    temp = pd.read_sql("""
                            select distinct(district_id), district_name, prov_id
                            from tb_covid
                            order by district_id asc
                       """, my_conn)
    
    #query clear data in tb_district
    reset = """
                TRUNCATE TABLE tb_district CASCADE;
            """

    #query insert data to tb_district
    insert = """
                insert into tb_district (district_id, district_name, prov_id)
                values (%s, %s, %s)
             """

    #load data to tb_district
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

    print('table "tb_district" successfully inserted')
    
if __name__ == "__main__":
    insert()
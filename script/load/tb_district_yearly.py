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
                            select district_id, case_id, year, total
                            from (
                                    select district_id, 1 as case_id, date_format(date, "%Y") as year, sum(suspect) as total
                                    from tb_covid
                                    group by year, district_id 
                                    union
                                    select district_id, 2 as case_id, date_format(date, "%Y") as year, sum(closecontact) as total
                                    from tb_covid
                                    group by year, district_id
                                    union
                                    select district_id, 3 as case_id, date_format(date, "%Y") as year, sum(probable) as total
                                    from tb_covid
                                    group by year, district_id
                                    union
                                    select district_id, 4 as case_id, date_format(date, "%Y") as year, sum(confirmation) as total
                                    from tb_covid
                                    group by year, district_id
                                 ) derivedTable
                            order by year asc
                       """, my_conn)
    
    #query clear data in tb_district_yearly
    reset = """
                TRUNCATE TABLE tb_district_yearly;
                ALTER SEQUENCE tb_district_yearly_id_seq RESTART;
            """
    
    #query insert data to tb_district_yearly
    insert = """
                insert into tb_district_yearly (district_id, case_id, year, total)
                values (%s, %s, %s, %s)
             """

    #load data to tb_district_yearly
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

    print('table "tb_district_yearly" successfully inserted')
    
if __name__ == "__main__":
    insert()
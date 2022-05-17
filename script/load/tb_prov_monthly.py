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
                            select prov_id, case_id, month, total
                            from (
                                    select prov_id, 1 as case_id, date_format(date, "%Y-%m") as month, sum(suspect) as total
                                    from tb_covid
                                    group by month, prov_id 
                                    union
                                    select prov_id, 2 as case_id, date_format(date, "%Y-%m") as month, sum(closecontact) as total
                                    from tb_covid
                                    group by month, prov_id
                                    union
                                    select prov_id, 3 as case_id, date_format(date, "%Y-%m") as month, sum(probable) as total
                                    from tb_covid
                                    group by month, prov_id
                                    union
                                    select prov_id, 4 as case_id, date_format(date, "%Y-%m") as month, sum(confirmation) as total
                                    from tb_covid
                                    group by month, prov_id
                                 ) derivedTable
                            order by month asc
                       """, my_conn)
    
    #query clear data in tb_prov_monthly
    reset = """
                TRUNCATE TABLE tb_prov_monthly;
                ALTER SEQUENCE tb_prov_monthly_id_seq RESTART;
            """
    
    #query insert data to tb_prov_monthly
    insert = """
                insert into tb_prov_monthly (prov_id, case_id, month, total)
                values (%s, %s, %s, %s)
             """

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

    print('table "tb_prov_monthly" successfully inserted')
    
if __name__ == "__main__":
    insert()
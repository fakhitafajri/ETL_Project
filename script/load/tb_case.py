import psycopg2

def insert():    
    #query clear data in tb_case
    reset = """
                TRUNCATE TABLE tb_case CASCADE;
                ALTER SEQUENCE tb_case_id_seq RESTART;
            """
    
    #query insert data to tb_case
    insert = """
                insert into tb_case(status_name)
                values ('suspect'),
                       ('closecontact'), 
                       ('probable'), 
                       ('confirmation')            
             """
    
    #load data to tb_case
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
            cursor.execute(insert)
    finally:
        if conn:
            conn.close()

    print('table "tb_case" successfully inserted')
    
if __name__ == "__main__":
    insert()
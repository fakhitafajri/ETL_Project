import psycopg2

def create_tables():
    commands = ("""
                    DROP DATABASE IF EXISTS covid WITH (FORCE);
                """,
                """
                    CREATE DATABASE covid;
                """
               )
    
    conn = None
    try:
        conn = psycopg2.connect(
            user="airflow",
            password="airflow",
            host='host.docker.internal',
            port=5432
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
    finally:
        if conn:
            conn.close()
    
    print('all table successfully created')
    
if __name__ == "__main__":
    create_tables()
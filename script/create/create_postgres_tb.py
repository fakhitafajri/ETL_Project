import psycopg2

def create_tables():
    #query for create table
    commands = ("""
                    create table if not exists tb_prov(
                        prov_id smallint not null primary key,
                        prov_name varchar(50) not null)
                """,
                """
                    create table if not exists tb_district(
                        district_id smallint not null primary key,
                        district_name varchar(50) not null,
                        prov_id smallint references tb_prov (prov_id))
                """,
                """
                    create table if not exists tb_case(
                        id serial not null primary key,
                        status_name varchar(50) not null,
                        status_detail varchar(50) default null)
                """,
                """
                    create table if not exists tb_prov_daily(
                        id serial not null primary key,
                        prov_id smallint references tb_prov (prov_id),
                        case_id smallint references tb_case (id),
                        date date not null,
                        total int not null)
                """,
                """
                    create table if not exists tb_prov_monthly(
                        id serial not null primary key,
                        prov_id smallint references tb_prov (prov_id),
                        case_id smallint references tb_case (id),
                        month varchar(8) not null,
                        total int not null)
                """,
                """
                    create table if not exists tb_prov_yearly(
                        id serial not null primary key,
                        prov_id smallint references tb_prov (prov_id),
                        case_id smallint references tb_case (id),
                        year varchar(5) not null,
                        total int not null)
                """,
                """
                    create table if not exists tb_district_monthly(
                        id serial not null primary key,
                        district_id smallint references tb_district (district_id),
                        case_id smallint references tb_case (id),
                        month varchar(8) not null,
                        total int not null)
                """,
                """
                    create table if not exists tb_district_yearly(
                        id serial not null primary key,
                        district_id smallint references tb_district (district_id),
                        case_id smallint references tb_case (id),
                        year varchar(5) not null,
                        total int not null)
                """
               )

    conn = None
    try:
        #connection to PostgreSQL
        conn = psycopg2.connect(
            user="airflow",
            password="airflow",
            dbname="covid",
            host='host.docker.internal',
            port=5432
        )

        #run PostgreSQL query
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
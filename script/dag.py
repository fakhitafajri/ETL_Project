from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


with DAG(
    dag_id="dag_fakhita",
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 5, 5)
) as dag:
    
    start = DummyOperator(
        task_id="start",
    )
    
    create_db = BashOperator(
        task_id='create_db',
        bash_command=f"""python3 /opt/airflow/dags/create/db_postgres.py"""
    )

    create_tb_pg = BashOperator(
        task_id='create_tb_pg',
        bash_command=f"""python3 /opt/airflow/dags/create/create_postgres_tb.py"""
    )

    create_tb_mysql = BashOperator(
        task_id='create_tb_mysql',
        bash_command=f"""python3 /opt/airflow/dags/create/create_mysql_tb.py"""
    )
    
    insert_staging = BashOperator(
        task_id='insert_staging',
        bash_command=f"""python3 /opt/airflow/dags/staging/staging.py"""
    )

    load_prov = BashOperator(
        task_id='load_prov',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_prov.py"""
    )

    load_district = BashOperator(
        task_id='load_district',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_district.py"""
    )

    load_case = BashOperator(
        task_id='load_case',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_case.py"""
    )

    load_prov_daily = BashOperator(
        task_id='load_prov_daily',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_prov_daily.py"""
    )

    load_prov_monthly = BashOperator(
        task_id='load_prov_monthly',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_prov_monthly.py"""
    )

    load_prov_yearly = BashOperator(
        task_id='load_prov_yearly',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_prov_yearly.py"""
    )

    load_district_monthly = BashOperator(
        task_id='load_district_monthly',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_district_monthly.py"""
    )

    load_district_yearly = BashOperator(
        task_id='load_district_yearly',
        bash_command=f"""python3 /opt/airflow/dags/load/tb_district_yearly.py"""
    )

    start >> create_db >> [create_tb_mysql, create_tb_pg] >> \
        insert_staging >> [load_prov, load_case] >> \
            load_district >> [load_prov_daily, load_prov_monthly, load_prov_yearly, \
                    load_district_monthly, load_district_yearly]
    
    
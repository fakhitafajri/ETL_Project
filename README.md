# Description

This project is about Dockerize ETL Pipeline using ETL tools Airflow that extract Public API data from PIKOBAR (for area Jawa Barat province), then load into MySQL (Staging Area) and finally aggregate the data and save into PostgreSQL.

## ETL Architecture Diagram and Integration Design Diagram

![image](https://user-images.githubusercontent.com/99057183/168715843-cb12f9cc-6849-4f03-bd38-49fd15fceee8.png)

![image](https://user-images.githubusercontent.com/99057183/168715888-20a300cd-6ffb-42ed-80b4-7efeca887d88.png)

API Specification:

![image](https://user-images.githubusercontent.com/99057183/168716322-c2ce6d4f-333e-4f85-a324-4ae503e52164.png)


API Request Example:

```curl -X GET "https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab" -H "accept: application/json"```

API response example:

![image](https://user-images.githubusercontent.com/89019518/152144976-19b7863f-cb18-49f7-859a-63445d10ad86.png)

Project Steps:

1. Create Docker (MySQL, Airflow and PostgreSQL)
2. Create Database in MySQL and PostgreSQL
3. Create DAG
4. Create DDL in MySQL
5. Get data from Public API covid19 and load data to MySQL
6. Create DDL in PostgreSQL for Fact table and Dimensiontable
7. Create aggregate Province Daily save to Province Daily Table
8. Create aggregate Province Monthly save to Province MonthlyTable
9. Create aggregate Province Yearly save to ProvinceYearly
10. Create aggregate District Monthly save to District Monthly
11. Create aggregate District Yearly save to District Yearly
12. Schedule the DAG daily

Schema in datawarehouse PostgreSQL:

![image](https://user-images.githubusercontent.com/89019518/152148177-b0f25bb4-b633-402d-aff3-1e0bdcdbd37f.png)

Schema in data staging MySQL:

![image](https://user-images.githubusercontent.com/89019518/152148252-e0ab9760-58a6-44df-b70b-9b1b9ee42bfd.png)

# How to run:

Run this command in terminal
```
mkdir -p ./dags ./logs ./plugins
```
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
```
docker-compose up airflow-init
```
```
docker-compose up
```

Copy and paste all the contents of the `scripts` folder to `dags` folder 

The folder structure will look like this: 

![Capture](https://user-images.githubusercontent.com/89019518/152134533-3836f03a-43ea-445f-ad7c-a30f4a98ce00.PNG)

import requests
import pandas as pd
import pymysql

def load():
    #read data from API 
    data_api = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab')
    
    #transform data
    data = pd.DataFrame(data_api.json()['data']['content'])
    data['tanggal'] = pd.to_datetime(data['tanggal']).dt.date
    data = data.drop(['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_dikarantina',
                  'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh',
                  'confirmation_meninggal', 'suspect_meninggal', 'suspect_meninggal', 'probable_meninggal', 
                  'closecontact_meninggal'], axis=1)
    
    #connection to MySQL database
    conn = pymysql.connect(host='localhost',
                       port=3306,
                       user='admin', 
                       password='admin123',  
                       db='finalproject')

    #query reset data
    reset = """
                TRUNCATE TABLE tb_covid;
            """
    
    #query insert data
    insert = """
                insert into tb_covid (date, prov_id, prov_name, district_id, 
                                      district_name, suspect, closecontact, probable, confirmation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
             """

    #load data to MySQL (data staging)
    cursor = conn.cursor()
    cursor.execute(reset)
    cursor.executemany(insert, data.values.tolist())
    conn.commit()
    cursor.close()

    print('proses load berhasil')
    
if __name__ == "__main__":
    load()
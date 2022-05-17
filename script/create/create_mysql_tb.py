import pymysql

def create_table():
    #query for create table
    command = ("""            
                CREATE TABLE IF NOT EXISTS `tb_covid` (
                    `date` date DEFAULT NULL,
                    `prov_id` smallint DEFAULT NULL,
                    `prov_name` varchar(50) DEFAULT NULL,
                    `district_id` smallint DEFAULT NULL,
                    `district_name` varchar(50) DEFAULT NULL,
                    `suspect` int DEFAULT NULL,
                    `closecontact` int DEFAULT NULL,
                    `probable` int DEFAULT NULL,
                    `confirmation` int DEFAULT NULL
                    )
              """)

    #connection to mysql
    conn = pymysql.connect(host='host.docker.internal',
                           port=3306,
                           user='admin', 
                           password='admin',  
                           db='db_covid')
    
    #run mysql query
    cur = conn.cursor()
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

    print('table "tb_covid" successfully created')

if __name__=='__main__':
    create_table()
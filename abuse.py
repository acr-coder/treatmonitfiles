import psycopg2
from psycopg2 import Error
import requests

key_1 = '71bbea197af446cf26f0c20cc7c149cccb729b7bed0cfe13ac32325ad21f73e279314c42fd1f8703'
key_2 = '821a4ccd58efc5fde0ed41035c96ec594c2b87528c6e8731d827ad215afc998ec4b2a27b44740f1b'
key_3 = '61e0bb15dbbd295de062896356b8b969874daf1f855754b8a655eb653e6844d7298ee825518e698f'
key_4 = 'f51086b1f0e7cd9d7a7dcd0fe756f05410e4a3bb38966c774a9e9b9a7b9befa74dc892f647ef69ed'


try:
    
    connection = psycopg2.connect(user="postgres",
                        password = "wD5pem$n",
                        host= "localhost",
                        port = "5432",
                        database="mywork")
    

    print("Database'e Bağlandı")
    print("********************")
    cursor = connection.cursor()   
    # cursor.execute(""" CREATE TABLE IF NOT EXISTS abuseNotUniq(id SERIAL  PRIMARY KEY,
    #                 ipAddress VARCHAR(50), abuseConfidenceScore VARCHAR(50), lastReportedAt VARCHAR(50)); """)

    # connection.commit()
    cursor.execute("SELECT COUNT(*) FROM abusenotuniq")
    recent_data = cursor.fetchone()
    print(f"Toplam Kayıt Sayısı: {recent_data[0]} ")
    print("********************")

    url = 'https://api.abuseipdb.com/api/v2/blacklist'

    # querystring = {
    #     'confidenceMinimum':'30'
    # }

    headers = {
        'Accept': 'application/json',
        'Key': key_2
    }

    response = requests.request(method='GET', url=url, headers=headers)
    r = response.json()
    result_data = r["data"]

    for result in result_data:
        try:
            cursor.execute(f"""INSERT INTO abusenotuniq(ipAddress,abuseConfidenceScore,lastReportedAt) VALUES('{result["ipAddress"]}','{result["abuseConfidenceScore"]}','{result["lastReportedAt"]}')""")
            connection.commit()
        except(Exception, Error) as error:
            print("Hata", error)
    
    cursor.execute("SELECT COUNT(*) FROM abusenotuniq")
    new_full_data = cursor.fetchone()
    print(f"Yeni Kayıt Sayısı: {new_full_data[0] - recent_data[0]} ")
    print("********************")
    print(f"Toplam Kayıt Sayısı: {new_full_data[0]} ")
    print("********************")
    cursor.execute("SELECT COUNT(DISTINCT ipaddress) FROM abusenotuniq")
    data = cursor.fetchone()
    print(f"Unique Veri Sayısı: {data[0]}")
    
    # cursor.execute("DROP TABLE IF EXISTS abuse_uniq;")
    
    # cursor.execute("CREATE TABLE abuse_uniq AS TABLE abusenotuniq;")
    
    # cursor.execute("DELETE FROM abuse_uniq a USING abuse_uniq b WHERE a.id < b.id AND a.ipaddress = b.ipaddress;")
    
    
    
    
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("Database Kapandı")
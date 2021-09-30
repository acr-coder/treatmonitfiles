import psycopg2
from psycopg2 import Error


try:
    
    connection = psycopg2.connect(user="postgres",
                        password = "wD5pem$n",
                        host= "localhost",
                        port = "5432",
                        database="mywork")
    

    print("Database'e Bağlandı")
    print("******************************")
    cursor = connection.cursor()   

    cursor.execute("drop table if exists abuse_uniq")
    print("abuse_uniq silindi")
    cursor.execute("create table abuse_uniq as table abusenotuniq")
    print("abuse_uniq oluştu")
    cursor.execute("delete from abuse_uniq a using abuse_uniq b where a.id < b.id and a.ipaddress = b.ipaddress")
    
    #cursor.execute("SELECT * FROM abusenotuniq")
    print("********* abuse_uniq table *********")
    cursor.execute("SELECT COUNT(*) FROM abuse_uniq")
    number_of_rows_for_abuse_uniq = cursor.fetchone()
    print("Toplam Veri Sayısı: "+str(number_of_rows_for_abuse_uniq[0]))
    print("******************************")    
    cursor.execute("SELECT COUNT(DISTINCT ipaddress) FROM abuse_uniq")
    number_of_uniq_rows_for_abuse_uniq = cursor.fetchone()
    print("Unique Veri Sayısı: "+str(number_of_uniq_rows_for_abuse_uniq[0]))
    print("******************************")
    print("********* abusenotuniq table *********")
    cursor.execute("SELECT COUNT(*) FROM abusenotuniq")
    number_of_rows_for_abusenotuniq = cursor.fetchone()
    print("Toplam Veri Sayısı: "+str(number_of_rows_for_abusenotuniq[0]))
    print("******************************")
    cursor.execute("SELECT COUNT(DISTINCT ipaddress) FROM abusenotuniq")
    number_of_uniq_rows_for_abusenotuniq = cursor.fetchone()
    print("Unique Veri Sayısı: "+str(number_of_uniq_rows_for_abusenotuniq[0]))
    print("******************************")
    
   
    
    
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("Database Kapandı")
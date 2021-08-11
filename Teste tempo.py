import psycopg2
import csv
from ferramentas.funcoes import unicos, formata_data, formata_hora, devolve_null, ler_csv, cadastrados

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"





conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


with conn.cursor() as cur:
    geolocation = ler_csv('update_order_items.csv')
    print(geolocation)

    conn.commit()
    conn.close()
"""



"""



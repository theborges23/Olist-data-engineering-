import psycopg2
import csv

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"

def formata_data(data: str) -> str:
    """
    Função que recebe uma string contendo a data, formata e devolve uma string pronta para inserção no banco de dados
    """
    return data[8:10] + '/' + data[5:7] + '/' + data[2:4]

def formata_hora(hora: str) -> str:
    """
    Função que recebe uma string contendo a hora, formata e devolve uma string pronta para inserção no banco de dados
    """
    return hora[11:19]



conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


with conn.cursor() as cur:

    # lendo arquivo de pedidos

    with open('olist_orders_dataset.csv', 'r') as pedidos:
        scv_reader = csv.reader(pedidos)
        next(scv_reader)
        for linha in scv_reader:
            cur.execute("INSERT INTO pedidos(order_id, customer_id, order_status, order_purchase_date, "
                        "order_purchase_time, order_approved_date, order_approved_time, "
                        "order_delivered_carrier_date, order_delivered_carrier_time, order_delivered_customer_date, "
                        "order_delivered_customer_time, order_estimated_delivery_date, order_estimated_delivery_time) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (linha[0], linha[1], linha[2],
                        formata_data(linha[3]), formata_hora(linha[3]), formata_data(linha[4]), formata_hora(linha[4]),
                        formata_data(linha[5]), formata_hora(linha[5]), formata_data(linha[6]), formata_hora(linha[6]),
                        formata_data(linha[7]), formata_hora(linha[7])))

    conn.commit()
    conn.close()





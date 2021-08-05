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
    if data == '':
        return None
    return data[8:10] + '/' + data[5:7] + '/' + data[2:4]

def formata_hora(hora: str) -> str:
    """
    Função que recebe uma string contendo a hora, formata e devolve uma string pronta para inserção no banco de dados
    """
    if hora == '':
        return None
    return hora[11:19]


def unicos(values: csv.reader, position: int) -> list:
    """
    recebe um objecto csv.reader e uma posição int e devolve uma lista contendo apenas valores únicos.
    """
    unicos = []
    for linha in values:
        if linha[position] not in unicos:
            unicos.append(linha[position])

    return unicos

def devolve_null(valor: str) -> None:
    if valor == '':
        return None
    else:
        return valor



conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


with conn.cursor() as cur:
    with open('olist_order_items_dataset.csv', 'r') as pedidos_itens:
        order_items_reader = csv.reader(pedidos_itens)
        order_items = list(order_items_reader)
        cur.execute("SELECT * FROM pedidos;")
        leitura = cur.fetchall()
        existentes = []
        for j in range(len(leitura)):
            existentes.append(leitura[j][0])
        for i in range(1, len(order_items)):
            if order_items[i][0] in existentes:
                cur.execute("INSERT INTO order_items(order_id, order_item_id, product_id, seller_id, shipping_limit_date,"
                            " shipping_limit_time, price, freight_value)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (order_items[i][0], order_items[i][1],
                            order_items[i][2], order_items[i][3], formata_data(order_items[i][4]),
                            formata_hora(order_items[i][4]), order_items[i][5], order_items[i][6]))

    conn.commit()
    conn.close()

"""



"""



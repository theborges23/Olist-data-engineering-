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



conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


with conn.cursor() as cur:
    with open('olist_order_reviews_dataset.csv', 'r', encoding="utf8") as reviews:
        reviews_reader = csv.reader(reviews)
        reviews = list(reviews_reader)
        cur.execute("SELECT * FROM pedidos;")
        leitura = cur.fetchall()
        existentes = []
        for j in range(len(leitura)):
            existentes.append(leitura[j][0])
        for i in range(1, len(reviews)):
            if reviews[i][1] in existentes:
                print('gravando')
                cur.execute("INSERT INTO reviews(review_id, order_id, review_score, review_comment_tittle, "
                            "review_comment_message, review_creation_date, review_creation_time, review_answer_date, "
                            "review_answer_time)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (reviews[i][0], reviews[i][1], reviews[i][2],
                                                                            reviews[i][3], reviews[i][4],
                                                                            formata_data(reviews[i][5]),
                                                                            formata_hora(reviews[i][5]),
                                                                            formata_data(reviews[i][6]),
                                                                            formata_hora(reviews[i][6])))
    conn.commit()
    conn.close()

"""


    with open('olist_order_reviews_dataset.csv', 'r', encoding="utf8") as reviews:
        reviews_reader = csv.reader(reviews)
        reviews = list(reviews_reader)
        cur.execute("SELECT * FROM pedidos;")
        leitura = cur.fetchall()
        print(leitura)
        existentes = []
        for j in range(len(leitura)):
            existentes.append(leitura[j][0])
            print(leitura[j][0])
        for i in range(1, len(reviews)):
            if reviews[i] in existentes:
                 print(f'{reviews[i][0]}, {reviews[i][1]}, {reviews[i][2]}, {reviews[i][3]}, {reviews[i][4]}, {formata_data(reviews[i][5])}, {formata_hora(reviews[i][5])},{formata_data(reviews[i][6])}, {formata_hora(reviews[i][6])}')

              cur.execute("INSERT INTO reviews(review_id, order_id, review_score, review_comment_tittle, "
                           "review_comment_message, review_creation_date, review_creation_time, review_answer_date, "
                           "review_answer_time)"
                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (reviews[i][0], reviews[i][1], reviews[i][2],
                            reviews[i][3], reviews[i][4], formata_data(reviews[i][5]), formata_hora(reviews[i][5]),
                            formata_data(reviews[i][6]), formata_hora(reviews[i][6])))
"""



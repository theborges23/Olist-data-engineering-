import psycopg2
import csv
from ferramentas.funcoes import unicos, formata_data, formata_hora, devolve_null, ler_csv, cadastrados

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


with conn.cursor() as cur:

    #preenchendo tabelas geolocalização

    cur.execute("INSERT INTO estados(estado) "
                "VALUES "
                "('RO'), "
                "('AC'), "
                "('AM'), "
                "('RR'), "
                "('PA'), "
                "('AP'), "
                "('TO'), "
                "('MA'), "
                "('PI'), "
                "('CE'), "
                "('RN'), "
                "('PB'), "
                "('PE'), "
                "('AL'), "
                "('SE'), "
                "('BA'), "
                "('MG'), "
                "('ES'), "
                "('RJ'), "
                "('SP'), "
                "('PR'), "
                "('SC'), "
                "('RS'), "
                "('MS'), "
                "('MT'), "
                "('GO'), "
                "('DF');")

    with open('olist_geolocation_dataset.csv', 'r') as geolocation:
        geo_reader = csv.reader(geolocation)
        next(geo_reader)
        cep = unicos(geo_reader, 0)
        for i in range(len(cep)):
            cur.execute("INSERT INTO zip_code(zip_code_prefix) "
                        "VALUES (%s)", (cep[i], ))

# Existiam alguns ceps do arquivo de clientes que não estavam cadastrados, abaixo fazemos a leitura e adição dos mesmos
    cur.execute("SELECT * FROM zip_code;")
    leitura = cur.fetchall()
    existentes = []
    for j in range(len(leitura)):
        existentes.append(leitura[j][1])
    with open('olist_customers_dataset.csv', 'r') as cliente:
        customer_reader = csv.reader(cliente)
        next(customer_reader)
        cep = unicos(customer_reader, 2)
        for i in range(len(cep)):
            if int(cep[i]) not in existentes:
                cur.execute("INSERT INTO zip_code(zip_code_prefix) "
                            "VALUES (%s)", (int(cep[i]),))

    with open('olist_geolocation_dataset.csv', 'r') as geolocation:
        geo_reader = csv.reader(geolocation)
        next(geo_reader)
        cit = unicos(geo_reader, 3)
        for i in range(len(cit)):
            cur.execute("INSERT INTO cidades(cidade) "
                        "VALUES (%s)", (cit[i], ))

    #INCLUINDO CIDADES QUE ESTAVAM NA NO ARQUIVO DE CLIENTES MAS NAO NO DE GEOLOCALIZAÇÃO
    cur.execute("SELECT * FROM cidades;")
    leitura = cur.fetchall()
    existentes = []
    for j in range(len(leitura)):
        existentes.append(leitura[j][1])
    with open('olist_customers_dataset.csv', 'r') as cliente:
        customer_reader = csv.reader(cliente)
        next(customer_reader)
        cit = unicos(customer_reader, 3)
        for i in range(len(cit)):
            if cit[i] not in existentes:
                cur.execute("INSERT INTO cidades(cidade) "
                            "VALUES (%s)", (cit[i],))

    #PREENCHENDO TABELA GEOLOCALIZAÇÃO

    geolocation = ler_csv('olist_geolocation_dataset.csv')
    for i in range(1, len(geolocation)):
        cur.execute("DO $$"
                    "BEGIN "
                    "IF EXISTS (select * from geolocation where zip_code_prefix = %s) THEN "
                    "UPDATE geolocation "
                    "SET cidade = %s, "
                    "estado = %s "
                    "WHERE zip_code_prefix = %s;"
                    "ELSE "
                    "INSERT INTO geolocation(zip_code_prefix, cidade, estado)"
                    "VALUES (%s, %s, %s);"
                    "END IF;"
                    "END"
                    "$$",
                    (geolocation[i][0], geolocation[i][3], geolocation[i][4], geolocation[i][0], geolocation[i][0],
                     geolocation[i][3], geolocation[i][4]))

    #limpando a tabela clientes, algumas chaves que deveriam ser unicas estavam repetidas
    with open('olist_customers_dataset.csv', 'r') as clientes:
        customers_reader = csv.reader(clientes)
        customers = list(customers_reader)
        contador = 0
        for linha in customers:
            contador = 0
            for comparacao in customers:
                if linha[1] == comparacao[1]:
                    contador += 1
                    if contador > 1:
                        customers.pop(customers.index(comparacao))
                        print("limpo")
        for i in range(1, len(customers)):
            print(f' {customers[i][1]}, {customers[i][0]}, {customers[i][2]}, {customers[i][3]}, {customers[i][4]}')
            cur.execute("INSERT INTO clientes(customer_unique_id, customer_id, customer_zip_code_prefix, customer_city,"
                        " customer_state)"
                        "VALUES (%s, %s, %s, %s, %s)",
                        (customers[i][1], customers[i][0], customers[i][2], customers[i][3], customers[i][4]))


    # preenchendo tabela de pedidos

    with open('olist_orders_dataset.csv', 'r') as pedidos:
        scv_reader = csv.reader(pedidos)
        orders = list(scv_reader)
        cur.execute("SELECT * FROM clientes;")
        leitura = cur.fetchall()
        existentes = []
        for j in range(len(leitura)):
            existentes.append(leitura[j][1])
        for i in range(1, len(orders)):
            if orders[i][1] in existentes:
                cur.execute("INSERT INTO pedidos(order_id, customer_id, order_status, order_purchase_date, "
                            "order_purchase_time, order_approved_date, order_approved_time, "
                            "order_delivered_carrier_date, order_delivered_carrier_time, order_delivered_customer_date, "
                            "order_delivered_customer_time, order_estimated_delivery_date, order_estimated_delivery_time) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (orders[i][0], orders[i][1],
                             orders[i][2],
                             formata_data(orders[i][3]),
                             formata_hora(orders[i][3]),
                             formata_data(orders[i][4]),
                             formata_hora(orders[i][4]),
                             formata_data(orders[i][5]),
                             formata_hora(orders[i][5]),
                             formata_data(orders[i][6]),
                             formata_hora(orders[i][6]),
                             formata_data(orders[i][7]),
                             formata_hora(orders[i][7])))

    #preenchendo tabela reviews

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

    #preenchendo a tabela pagamentos

    with open('olist_order_payments_dataset.csv', 'r', encoding="utf8") as pagamentos:
        payments_reader = csv.reader(pagamentos)
        payments = list(payments_reader)
        cur.execute("SELECT * FROM pedidos;")
        leitura = cur.fetchall()
        existentes = []
        for j in range(len(leitura)):
            existentes.append(leitura[j][0])
        for i in range(1, len(payments)):
            if payments[i][0] in existentes:
                cur.execute("INSERT INTO pagamentos(order_id, payment_sequential, payment_type, payment_installments,"
                            " payment_value)"
                            "VALUES (%s, %s, %s, %s, %s)", (payments[i][0], payments[i][1], payments[i][2], payments[i][3],
                            payments[i][4]))

    #preenchendo a tabela produtos

    with conn.cursor() as cur:
        with open('olist_products_dataset.csv', 'r') as produtos:
            products_reader = csv.reader(produtos)
            next(products_reader)
            for linha in products_reader:
                cur.execute("INSERT INTO produtos(product_id, product_category_name, "
                            "product_description_lenght, product_photos_qty, product_weight_g, product_lenght_cm, "
                            "product_width_cm)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (linha[0], linha[1], devolve_null(linha[3]), devolve_null(linha[4]),
                             devolve_null(linha[5]), devolve_null(linha[6]), devolve_null(linha[8])))

    # Existiam alguns ceps do arquivo de vendedores que não estavam cadastrados, abaixo fazemos a leitura e adição dos mesmos
    cur.execute("SELECT * FROM zip_code;")
    leitura = cur.fetchall()
    existentes = []
    for j in range(len(leitura)):
        existentes.append(leitura[j][1])
    with open('olist_sellers_dataset.csv', 'r') as vendedores:
        seller_reader = csv.reader(vendedores)
        next(seller_reader)
        cep = unicos(seller_reader, 1)
        for i in range(len(cep)):
            if int(cep[i]) not in existentes:
                cur.execute("INSERT INTO zip_code(zip_code_prefix) "
                            "VALUES (%s)", (int(cep[i]),))

    # INCLUINDO CIDADES QUE ESTAVAM NA NO ARQUIVO DE VENDEDORES MAS NAO NO DE GEOLOCALIZAÇÃO
    cur.execute("SELECT * FROM cidades;")
    leitura = cur.fetchall()
    existentes = []
    for j in range(len(leitura)):
        existentes.append(leitura[j][1])
    with open('olist_sellers_dataset.csv', 'r') as vendedores:
        sellers_reader = csv.reader(vendedores)
        next(sellers_reader)
        cit = unicos(sellers_reader, 2)
        for i in range(len(cit)):
            if cit[i] not in existentes:
                cur.execute("INSERT INTO cidades(cidade) "
                            "VALUES (%s)", (cit[i],))
    #preenchendo a tabela vendedores

    with open('olist_sellers_dataset.csv', 'r') as vendedores:
        sellers_reader = csv.reader(vendedores)
        next(sellers_reader)
        for linha in sellers_reader:
            cur.execute("INSERT INTO vendedores(seller_id, seller_zip_code_prefix, seller_city, seller_state)"
                        "VALUES (%s, %s, %s, %s)", (linha[0], linha[1], linha[2], linha[3]))

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





from ferramentas.funcoes import formata_data, formata_hora, ler_csv, devolve_null
import psycopg2



DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"


tipo: int = int(input("Informe a opção desejada:\n 1) Atualizar Clientes\n 2) Atualizar Geolocalizações\n 3) Atualizar Itens "
             "do Pedido\n 4) Atualizar Pagamentos\n 5) Atualizar Pedidos\n 6) Atualizar Produtos\n 7) Atualizar "
                 "Vendedores\n 8) Atulizar Reviews\n"))

arquivo: str = input("Informe o nome do arquivo: ")


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

e : int = 0

with conn.cursor() as cur:

    if tipo == 1:
        try:
            customers: list = ler_csv(arquivo)
            for i in range(1, len(customers)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from clientes where customer_id = %s) THEN "
                            "UPDATE clientes "
                            "SET customer_unique_id = %s, "
                            "customer_zip_code_prefix = %s, "
                            "customer_city = %s, "
                            "customer_state = %s "
                            "WHERE customer_id = %s;"
                            "ELSE "
                            "INSERT INTO clientes(customer_unique_id, customer_id, customer_zip_code_prefix, customer_city,"
                            " customer_state)"
                            "VALUES (%s, %s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (customers[i][0], customers[i][1], customers[i][2], customers[i][3], customers[i][4],
                            customers[i][0], customers[i][1], customers[i][0], customers[i][2], customers[i][3],
                            customers[i][4]))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto!")
            print(erro)
            e += 1

    if tipo == 2:
        try:
            geolocation: list = ler_csv(arquivo)
            for i in range(1, len(geolocation)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF NOT EXISTS (select * from cidades where cidade = %s) THEN "
                            "INSERT INTO cidades(cidade)"
                            "VALUES (%s);"
                            "END IF;"
                            "IF NOT EXISTS (select * from  zip_code where zip_code_prefix = %s) THEN "
                            "INSERT INTO zip_code(zip_code_prefix)"
                            "VALUES (%s);"
                            "END IF;"
                            "IF EXISTS (select * from geolocation where zip_code_prefix = %s) THEN "
                            "UPDATE geolocation "
                            "SET cidade = %s, "
                            "estado = %s "
                            "WHERE zip_code_prefix = %s;"
                            "ELSE "
                            "INSERT INTO geolocation(zip_code_prefix, cidade, estado) "
                            "VALUES (%s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (geolocation[i][3], geolocation[i][3], geolocation[i][0], geolocation[i][0], geolocation[i][0],
                                   geolocation[i][3], geolocation[i][4], geolocation[i][0], geolocation[i][0], geolocation[i][3],
                                   geolocation[i][4]))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto!")
            print(erro)
            e += 1

    if tipo == 3:
        try:
            ordered_items: list = ler_csv(arquivo)
            for i in range(1, len(ordered_items)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from order_items where order_id = %s) THEN "
                            "UPDATE order_items "
                            "SET order_item_id = %s, "
                            "product_id = %s, "
                            "seller_id = %s, "
                            "shipping_limit_date = %s,"
                            "shipping_limit_time = %s,"
                            "price = %s,"
                            "freight_value = %s "
                            "WHERE order_id = %s;"
                            "ELSE "
                            "INSERT INTO order_items(order_id, order_item_id, product_id, seller_id, "
                            "shipping_limit_date, shipping_limit_time, price, freight_value)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (ordered_items[i][0], ordered_items[i][1], ordered_items[i][2], ordered_items[i][3],
                                   formata_data(ordered_items[i][4]), formata_hora(ordered_items[i][4]),
                                   ordered_items[i][5], ordered_items[i][6], ordered_items[i][0],
                                   ordered_items[i][0], ordered_items[i][1], ordered_items[i][2], ordered_items[i][3],
                                   formata_data(ordered_items[i][4]), formata_hora(ordered_items[i][4]),
                                   ordered_items[i][5], ordered_items[i][6]))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto!")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1

    if tipo == 4:
        try:
            pagamentos: list = ler_csv(arquivo)
            for i in range(1, len(pagamentos)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from pagamentos where order_id = %s) THEN "
                            "UPDATE pagamentos "
                            "SET payment_sequential = %s, "
                            "payment_type = %s, "
                            "payment_installments = %s,"
                            "payment_value = %s"
                            "WHERE order_id = %s;"
                            "ELSE "
                            "INSERT INTO pagamentos(order_id, payment_sequential, payment_type, payment_installments, "
                            "payment_value)"
                            "VALUES (%s, %s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (pagamentos[i][0], pagamentos[i][1], pagamentos[i][2], pagamentos[i][3],
                                   pagamentos[i][4], pagamentos[i][0],
                                   pagamentos[i][0], pagamentos[i][1], pagamentos[i][2], pagamentos[i][3],
                                   pagamentos[i][4]))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1

    if tipo == 5:
        try:
            pedidos: list = ler_csv(arquivo)
            for i in range(1, len(pedidos)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from pedidos where order_id = %s) THEN "
                            "UPDATE pedidos "
                            "SET customer_id = %s, "
                            "order_status = %s, "
                            "order_purchase_date = %s,"
                            "order_purchase_time = %s,"
                            "order_approved_date = %s,"
                            "order_approved_time = %s,"
                            "order_delivered_carrier_date = %s,"
                            "order_delivered_carrier_time = %s,"
                            "order_delivered_customer_date = %s,"
                            "order_delivered_customer_time = %s,"
                            "order_estimated_delivery_date = %s,"
                            "order_estimated_delivery_time = %s"
                            "WHERE order_id = %s;"
                            "ELSE "
                            "INSERT INTO pedidos(order_id, customer_id, order_status, order_purchase_date, "
                            "order_purchase_time, order_approved_date, order_approved_time, "
                            "order_delivered_carrier_date, order_delivered_carrier_time, order_delivered_customer_date,"
                            "order_delivered_customer_time, order_estimated_delivery_date, order_estimated_delivery_time) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); "
                            "END IF;"
                            "END"
                            "$$", (pedidos[i][0], pedidos[i][1], pedidos[i][2], formata_data(pedidos[i][3]),
                                   formata_hora(pedidos[i][3]), formata_data(pedidos[i][4]), formata_hora(pedidos[i][4])
                                   , formata_data(pedidos[i][5]), formata_hora(pedidos[i][5]),
                                   formata_data(pedidos[i][6]), formata_hora(pedidos[i][6]), formata_data(pedidos[i][7])
                                   , formata_hora(pedidos[i][7]), pedidos[i][0],
                                   pedidos[i][0], pedidos[i][1], pedidos[i][2], formata_data(pedidos[i][3]),
                                   formata_hora(pedidos[i][3]), formata_data(pedidos[i][4]), formata_hora(pedidos[i][4]),
                                   formata_data(pedidos[i][5]), formata_hora(pedidos[i][5]), formata_data(pedidos[i][6]),
                                   formata_hora(pedidos[i][6]), formata_data(pedidos[i][7]), formata_hora(pedidos[i][7])
                                   ))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1

    if tipo == 6:
        try:
            produtos: list = ler_csv(arquivo)
            for i in range(1, len(produtos)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from produtos where product_id = %s) THEN "
                            "UPDATE produtos "
                            "SET product_category_name = %s, "
                            "product_description_lenght = %s, "
                            "product_photos_qty = %s,"
                            "product_weight_g = %s,"
                            "product_lenght_cm = %s,"
                            "product_width_cm = %s"
                            "WHERE product_id = %s;"
                            "ELSE "
                            "INSERT INTO produtos(product_id, product_category_name, "
                            "product_description_lenght, product_photos_qty, product_weight_g, product_lenght_cm, "
                            "product_width_cm)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (produtos[i][0], produtos[i][1], devolve_null(produtos[i][3]),
                                   devolve_null(produtos[i][4]), devolve_null(produtos[i][5]),
                                   devolve_null(produtos[i][6]), devolve_null(produtos[i][8]), produtos[i][0],
                                   produtos[i][0], produtos[i][1], devolve_null(produtos[i][3]),
                                   devolve_null(produtos[i][4]), devolve_null(produtos[i][5]),
                                   devolve_null(produtos[i][6]), devolve_null(produtos[i][8])))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1

    if tipo == 7:
        try:
            vendedores: list = ler_csv(arquivo)
            for i in range(1, len(vendedores)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from vendedores where seller_id = %s) THEN "
                            "UPDATE vendedores "
                            "SET seller_zip_code_prefix = %s, "
                            "seller_city = %s,"
                            "seller_state = %s"
                            "WHERE seller_id = %s;"
                            "ELSE "
                            "INSERT INTO vendedores(seller_id, seller_zip_code_prefix, seller_city, seller_state)"
                            "VALUES (%s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (vendedores[i][0], vendedores[i][1], vendedores[i][2], vendedores[i][3], vendedores[i][0],
                                   vendedores[i][0], vendedores[i][1], vendedores[i][2], vendedores[i][3]))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1

    if tipo == 8:
        try:
            reviews: list = ler_csv(arquivo)
            for i in range(1, len(reviews)):
                cur.execute("DO $$"
                            "BEGIN "
                            "IF EXISTS (select * from reviews where review_id = %s) THEN "
                            "UPDATE reviews "
                            "SET order_id = %s, "
                            "review_score = %s, "
                            "review_comment_tittle = %s,"
                            "review_comment_message = %s,"
                            "review_creation_date = %s,"
                            "review_creation_time = %s,"
                            "review_answer_date = %s,"
                            "review_answer_time = %s"
                            "WHERE review_id = %s;"
                            "ELSE "
                            "INSERT INTO reviews(review_id, order_id, review_score, _review_comment_tittle, "
                            "review_comment_message, review_creation_date, review_creation_time, review_answer_date,"
                            "review_answer_time)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                            "END IF;"
                            "END"
                            "$$", (reviews[i][0], reviews[i][1], reviews[i][2], reviews[i][3],
                                   reviews[i][4], formata_data(reviews[i][5]), formata_hora(reviews[i][5]),
                                   formata_data(reviews[i][6]), formata_hora(reviews[i][6]), reviews[i][0], reviews[i][0]
                                   , reviews[i][1], reviews[i][2], reviews[i][3], reviews[i][4],
                                   formata_data(reviews[i][5]), formata_hora(reviews[i][5]), formata_data(reviews[i][6]),
                                   formata_hora(reviews[i][6])
                            ))
        except psycopg2.DataError as erro:
            print("Um dos valores no CSV não está no formato correto")
            print(erro)
            e += 1
        except psycopg2.IntegrityError as erro:
            print("Provavelmente uma das chaves é inválida, verifique se o código do pedido e do produto estão corretos"
                  " e cadastrados\nDetalhes:")
            print(erro)
            e += 1



    conn.commit()
    conn.close()

if e == 0:
    print("Update concluído com sucesso")
else:
    print("Erro no update")
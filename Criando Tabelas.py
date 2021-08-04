DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"

import psycopg2


#CRIANDO TABELA PEDIDOS

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:
    cur.execute("CREATE TABLE pedidos("
                "order_id VARCHAR(35) PRIMARY KEY, "
                "customer_id VARCHAR(35) NOT NULL, "
                "order_status VARCHAR(30) NOT NULL, "
                "order_purchase_date VARCHAR(8) NOT NULL, "
                "order_purchase_time VARCHAR(8) NOT NULL, "
                "order_approved_date VARCHAR(8) NULL, "
                "order_approved_time VARCHAR(8) NULL, "
                "order_delivered_carrier_date VARCHAR(8) NULL, "
                "order_delivered_carrier_time VARCHAR(8) NULL, "
                "order_delivered_customer_date VARCHAR(8) NULL, "
                "order_delivered_customer_time VARCHAR(8) NULL, "
                "order_estimated_delivery_date VARCHAR(8) NULL, "
                "order_estimated_delivery_time VARCHAR(8) NULL, "
                "FOREIGN KEY (order_purchase_date) REFERENCES tempo(date), "
                "FOREIGN KEY (order_purchase_time) REFERENCES horas(hora), "
                "FOREIGN KEY (order_approved_date) REFERENCES tempo(date), "
                "FOREIGN KEY (order_approved_time) REFERENCES horas(hora), "
                "FOREIGN KEY (order_delivered_carrier_date) REFERENCES tempo(date), "
                "FOREIGN KEY (order_delivered_carrier_time) REFERENCES horas(hora), "
                "FOREIGN KEY (order_delivered_customer_date) REFERENCES tempo(date), "
                "FOREIGN KEY (order_delivered_customer_time) REFERENCES horas(hora), "
                "FOREIGN KEY (order_estimated_delivery_date) REFERENCES tempo(date), "
                "FOREIGN KEY (order_estimated_delivery_time) REFERENCES horas(hora) "
                ");")

    #Criando tabela reviews

    cur.execute("CREATE TABLE reviews("
                "review_id VARCHAR(35) PRIMARY KEY, "
                "order_id VARCHAR(35) NOT NULL UNIQUE, "
                "review_score SMALLINT NOT NULL, "
                "review_comment_tittle TEXT NULL, "
                "review_comment_message TEXT NULL, "
                "review_creation_date VARCHAR(8) NOT NULL, "
                "review_creation_time VARCHAR(8) NOT NULL, "
                "review_answer_date VARCHAR(8) NULL, "
                "review_answer_time VARCHAR(8) NULL, "
                "FOREIGN KEY (order_id) REFERENCES pedidos(order_id), "
                "FOREIGN KEY (review_creation_date) REFERENCES tempo(date), "
                "FOREIGN KEY (review_creation_time) REFERENCES horas(hora), "
                "FOREIGN KEY (review_answer_date) REFERENCES tempo(date), "
                "FOREIGN KEY (review_answer_time) REFERENCES horas(hora) "
                ");")

    # Criando tabela geolocation, pagamentos, clientes e produtos

    cur.execute("CREATE TABLE zip_code("
                "id SERIAL PRIMARY KEY,"
                "zip_code_prefix INT NOT NULL UNIQUE"
                ");")

    cur.execute("CREATE TABLE estados("
                "id SERIAL PRIMARY KEY,"
                "estado VARCHAR(2) NOT NULL UNIQUE"
                ");")

    cur.execute("CREATE TABLE cidades("
                "id SERIAL PRIMARY KEY,"
                "cidade TEXT NOT NULL UNIQUE"
                ");")

    cur.execute("CREATE TABLE pagamentos("
                "id SERIAL PRIMARY KEY,"
                "order_id VARCHAR(35) NOT NULL,"
                "payment_sequential SMALLINT NOT NULL,"
                "payment_type VARCHAR(30) NOT NULL,"
                "payment_installments SMALLINT NOT NULL,"
                "payment_value DECIMAL NOT NULL,"
                "FOREIGN KEY (order_id) REFERENCES pedidos(order_id)"
                ");")

    cur.execute("CREATE TABLE produtos("
                "product_id VARCHAR(35) PRIMARY KEY,"
                "product_category_name VARCHAR(50) NOT NULL,"
                "product_name_lenght SMALLINT NOT NULL,"
                "product_description_lenght SMALLINT NULL,"
                "product_photos_qty SMALLINT NULL,"
                "product_weight_g INT NULL,"
                "product_length_cm DECIMAL NULL,"
                "product_width_cm DECIMAL NULL"
                ");")

    cur.execute("CREATE TABLE clientes("
                "customer_unique_id VARCHAR(35) PRIMARY KEY,"
                "customer_id VARCHAR(35),"
                "customer_zip_code_prefix INT NOT NULL,"
                "customer_city TEXT NOT NULL,"
                "customer_state VARCHAR(2) NOT NULL,"
                "FOREIGN KEY (customer_zip_code_prefix) REFERENCES zip_code(zip_code_prefix),"
                "FOREIGN KEY (customer_city) REFERENCES cidades(cidade),"
                "FOREIGN KEY (customer_state) REFERENCES estados(estado)"
                ");")

    cur.execute("ALTER TABLE orders "
                "ADD FOREIGN KEY (customer_id) REFERENCES clientes(customer_unique_id)")

    cur.execute("CREATE TABLE vendedores("
                "seller_id VARCHAR(35) PRIMARY KEY,"
                "seller_zip_code_prefix INT NOT NULL,"
                "seller_city TEXT NOT NULL,"
                "seller_state VARCHAR(2) NOT NULL,"
                "FOREIGN KEY (seller_zip_code_prefix) REFERENCES zip_code(zip_code_prefix),"
                "FOREIGN KEY (seller_city) REFERENCES cidades(cidade),"
                "FOREIGN KEY (seller_state) REFERENCES estados(estado)"
                ");")

    cur.execute("CREATE TABLE order_items("
                "id SERIAL PRIMARY KEY,"
                "order_id VARCHAR(35) NOT NULL,"
                "order_item_id VARCHAR(35) NOT NULL,"
                "product_id VARCHAR(35) NOT NULL,"
                "seller_id VARCHAR(35) NOT NULL,"
                "shipping_limit_date VARCHAR(8) NOT NULL,"
                "shipping_limit_time VARCHAR(8) NOT NULL,"
                "price DECIMAL NOT NULL,"
                "freight_value DECIMAL NULL,"
                "FOREIGN KEY (order_id) REFERENCES pedidos(order_id),"
                "FOREIGN KEY (product_id) REFERENCES produtos(product_id),"
                "FOREIGN KEY (seller_id) REFERENCES vendedores(seller_id),"
                "FOREIGN KEY (shipping_limit_date) REFERENCES tempo(date),"
                "FOREIGN KEY (shipping_limit_time) REFERENCES horas(hora)"
                ");")



    conn.commit()
    conn.close()
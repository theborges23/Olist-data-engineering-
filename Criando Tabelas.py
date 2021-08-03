DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"

import psycopg2


#Criando tabela pedidos

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

cur = conn.cursor()

cur.execute("CREATE TABLE pedidos("
            "order_id VARCHAR(35) PRIMARY KEY, "
            "customer_id VARCHAR(35) NOT NULL,"
            "order_status VARCHAR(30) NOT NULL,"
            "order_purchase_timestamp VARCHAR(6) NOT NULL,"
            "order_approved_at VARCHAR(6) NULL,"
            "order_delivered_carrier_date VARCHAR(6) NULL,"
            "order_delivered_customer_date VARCHAR(6) NULL,"
            "order_estimated_delivery_date VARCHAR(6) NULL"
            ");")

conn.commit()

cur.close()

conn.close()
'''

#-----------------------------------------------------------------------------------------------------------------------

'''
#Criando tabela reviews

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

cur = conn.cursor()

cur.execute("CREATE TABLE reviews("
            "review_id VARCHAR(40) PRIMARY KEY, "
            "order_id VARCHAR(40) NOT NULL,"
            "review_score SMALLINT NOT NULL,"
            "review_comment_tittle TEXT NULL,"
            "review_comment_message TEXT NULL,"
            "review_creation_date VARCHAR(6) NOT NULL,"
            "review_answer_timestamp VARCHAR(6) NOT NULL,"
            "FOREIGN KEY (order_id) REFERENCES pedidos(order_id)"
            ");")

conn.commit()

cur.close()

conn.close()


#-----------------------------------------------------------------------------------------------------------------------

'''
#Criando tabela tempo

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

    cur.execute("CREATE TABLE dias_mes("
                "dia_mes_id SMALLINT PRIMARY KEY,"
                "dia_mes SMALLINT NOT NULL"
                ");")

    cur.execute("CREATE TABLE num_meses("
                "num_mes_id SMALLINT PRIMARY KEY,"
                "num_mes SMALLINT NOT NULL"
                ");")

    cur.execute("CREATE TABLE anos("
                "ano_id SMALLINT PRIMARY KEY,"
                "ano SMALLINT NOT NULL"
                ");")

    cur.execute("CREATE TABLE nome_meses("
                "mes_nome_id SMALLINT PRIMARY KEY, "
                "mes_nome VARCHAR(15) NOT NULL"
                ");")

    cur.execute("CREATE TABLE dias_semana("
                "dia_semana_id INT PRIMARY KEY,"
                "dia_semana VARCHAR(15) NOT NULL"
                ");")

    cur.execute("CREATE TABLE bimestres("
                "bimestre_id SMALLINT PRIMARY KEY,"
                "bimestre SMALLINT NOT NULL"
                ");")

    cur.execute("CREATE TABLE trimestres("
                "trimestre_id SMALLINT PRIMARY KEY,"
                "trimestre SMALLINT NOT NULL"
                ");")

    cur.execute("CREATE TABLE tempo("
                "tempo_id INT PRIMARY KEY, "
                "date VARCHAR(8) NOT NULL,"
                "dia_mes_id SMALLINT NOT NULL,"
                "num_mes_id SMALLINT NOT NULL,"
                "ano_id SMALLINT NOT NULL,"
                "mes_nome_id SMALLINT NOT NULL,"
                "dia_semana_id SMALLINT NOT NULL,"
                "bimestre_id SMALLINT NOT NULL,"
                "trimestre_id SMALLINT NOT NULL,"
                "FOREIGN KEY (dia_mes_id) REFERENCES dias_mes(dia_mes_id),"
                "FOREIGN KEY (num_mes_id) REFERENCES num_meses(num_mes_id),"
                "FOREIGN KEY (ano_id) REFERENCES anos(ano_id),"
                "FOREIGN KEY (mes_nome_id) REFERENCES nome_meses(mes_nome_id),"
                "FOREIGN KEY (dia_semana_id) REFERENCES dias_semana(dia_semana_id),"
                "FOREIGN KEY (bimestre_id) REFERENCES bimestres(bimestre_id),"
                "FOREIGN KEY (trimestre_id) REFERENCES trimestres(trimestre_id)"
                ");")


    conn.commit()

    conn.close()
'''
#-----------------------------------------------------------------------------------------------------------------------

'''
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

#ALTERANDO TIPO DE CAMPOS DE DATA DAS TABELAS PEDIDOS E REVIEWS PARA QUE SEJAM IGUAIS A CHAVE PRIMARIA DA TABELA TEMPO: 
#tempo_id PARA QUE AS REFERENCIAS POSSSAM SER ADICIONADAS

    cur.execute("ALTER TABLE pedidos "
                "ALTER COLUMN order_purchase_timestamp TYPE integer USING(order_purchase_timestamp::integer),"
                "ALTER COLUMN order_approved_at TYPE integer USING(order_approved_at::integer),"
                "ALTER COLUMN order_delivered_carrier_date TYPE integer USING(order_delivered_carrier_date::integer),"
                "ALTER COLUMN order_delivered_customer_date TYPE integer USING(order_delivered_customer_date::integer),"
                "ALTER COLUMN order_estimated_delivery_date TYPE integer USING(order_estimated_delivery_date::integer);")

    cur.execute("ALTER TABLE reviews "
                "ALTER COLUMN review_creation_date TYPE integer USING(review_creation_date::integer),"
                "ALTER COLUMN review_answer_timestamp TYPE integer USING(review_answer_timestamp::integer);")


#ADICIONANDO CHAVES ESTRANGEIRAS NAS TABELAS DE PEDIDOS E REVIEWS AGORA Q A TABELA TEMPO ESTÁ PRONTA

    cur.execute("ALTER TABLE pedidos "
                "ADD FOREIGN KEY (order_purchase_timestamp) REFERENCES tempo(tempo_id),"
                "ADD FOREIGN KEY (order_approved_at) REFERENCES tempo(tempo_id),"
                "ADD FOREIGN KEY (order_delivered_carrier_date) REFERENCES tempo(tempo_id),"
                "ADD FOREIGN KEY (order_delivered_customer_date) REFERENCES tempo(tempo_id),"
                "ADD FOREIGN KEY (order_estimated_delivery_date) REFERENCES tempo(tempo_id);")

    cur.execute("ALTER TABLE reviews "
                "ADD FOREIGN KEY (review_creation_date) REFERENCES tempo(tempo_id),"
                "ADD FOREIGN KEY (review_answer_timestamp) REFERENCES tempo(tempo_id)")


    conn.commit()

    conn.close()



#-----------------------------------------------------------------------------------------------------------------------



#Criando tabela geolocation, pagamentos, clientes e produtos

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

    cur.execute("CREATE TABLE geolocation("
                "id SERIAL PRIMARY KEY,"
                "geolocation_zip_code_prefix INT NOT NULL,"
                "geolocation_lat DECIMAL NOT NULL,"
                "geolocation_lng DECIMAL NOT NULL,"
                "geolocation_city VARCHAR(50) NOT NULL,"
                "geolocation_state VARCHAR(2) NOT NULL"
                ");")

    cur.execute("CREATE TABLE pagamentos("
                "order_id VARCHAR(40) PRIMARY KEY,"
                "payment_sequential SMALLINT NOT NULL,"
                "payment_type VARCHAR(30) NOT NULL,"
                "payment_installments SMALLINT NOT NULL,"
                "payment_value DECIMAL NOT NULL"
                ");")

    cur.execute("CREATE TABLE produtos("
                "product_id VARCHAR(40) PRIMARY KEY,"
                "product_category_name VARCHAR(50) NOT NULL,"
                "product_name_lenght SMALLINT NOT NULL,"
                "product_description_lenght SMALLINT NULL,"
                "product_photos_qty SMALLINT NULL,"
                "product_weight_g DECIMAL NULL,"
                "product_length_cm DECIMAL NULL,"
                "product_width_cm DECIMAL NULL"
                ");")

    cur.execute("CREATE TABLE clientes("
                "customer_unique_id VARCHAR(40) PRIMARY KEY,"
                "customer_id VARCHAR(40),"
                "customer_zip_code_prefix INT NOT NULL,"
                "customer_city VARCHAR(50) NOT NULL,"
                "customer_state VARCHAR(2) NOT NULL"
                ");")

    conn.commit()

    conn.close()

#-----------------------------------------------------------------------------------------------------------------------


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

    #CRIANDO TABELA DE HORAS
    cur.execute("CREATE TABLE horas("
                "hora_id SMALLINT PRIMARY KEY,"
                "hora SMALLINT UNIQUE"
                ");")
    
    cur.execute("ALTER TABLE horas "
                "ALTER COLUMN hora TYPE VARCHAR(8);")
   

    cur.execute("ALTER TABLE horas "
                "ALTER COLUMN hora_id TYPE INT;")
                

    cur.execute("ALTER TABLE horas "
                "ALTER COLUMN hora_id TYPE VARCHAR(6);")
    
    
    cur.execute("ALTER TABLE tempo "
                "ALTER COLUMN tempo_id TYPE VARCHAR(6);")
    

    conn.commit()

    conn.close()
    '''

#ADICIONAR TODAS AS RELAÇÕES DE TEMPO!!!!!!
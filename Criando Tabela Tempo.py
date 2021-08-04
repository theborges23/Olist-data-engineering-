import psycopg2

# Inserir dados do servidor

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

#CRIANDO AS TABELAS

    #CRIANDO A TABELA DE DIAS DO MES
    cur.execute("CREATE TABLE dias_mes("
                "dia_id SMALLINT PRIMARY KEY,"
                "dia SMALLINT NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE MESES (NUMERO)
    cur.execute("CREATE TABLE meses("
                "mes_id SMALLINT PRIMARY KEY,"
                "mes SMALLINT NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE ANOS
    cur.execute("CREATE TABLE anos("
                "ano_id SMALLINT PRIMARY KEY,"
                "ano SMALLINT NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE NOME DOS MESES
    cur.execute("CREATE TABLE nome_meses("
                "mes_nome_id SMALLINT PRIMARY KEY, "
                "mes_nome VARCHAR(15) NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE DIAS DA SEMANA
    cur.execute("CREATE TABLE dias_semana("
                "dia_semana_id INT PRIMARY KEY,"
                "dia_semana VARCHAR(15) NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE BIMESTRES
    cur.execute("CREATE TABLE bimestres("
                "bimestre_id SMALLINT PRIMARY KEY,"
                "bimestre SMALLINT NOT NULL UNIQUE"
                ");")
    #CRIANDO A TABELA DE TRIMESTRES
    cur.execute("CREATE TABLE trimestres("
                "trimestre_id SMALLINT PRIMARY KEY,"
                "trimestre SMALLINT NOT NULL UNIQUE"
                ");")
    #CRIANDO TABELA HORAS
    cur.execute("CREATE TABLE horas("
                "hora_id VARCHAR(6) PRIMARY KEY,"
                "hora VARCHAR(8) UNIQUE"
                ");")

#CRIANDO A TABELA TEMPO
    cur.execute("CREATE TABLE tempo("
                "tempo_id VARCHAR(6) PRIMARY KEY, "
                "date VARCHAR(8) NOT NULL UNIQUE,"
                "dia_mes SMALLINT NOT NULL,"
                "mes SMALLINT NOT NULL,"
                "ano SMALLINT NOT NULL,"
                "mes_nome VARCHAR(15) NOT NULL,"
                "dia_semana VARCHAR(15) NOT NULL,"
                "bimestre SMALLINT NOT NULL,"
                "trimestre SMALLINT NOT NULL,"
                "FOREIGN KEY (dia_mes) REFERENCES dias_mes(dia),"
                "FOREIGN KEY (mes) REFERENCES meses(mes),"
                "FOREIGN KEY (ano) REFERENCES anos(ano),"
                "FOREIGN KEY (mes_nome) REFERENCES nome_meses(mes_nome),"
                "FOREIGN KEY (dia_semana) REFERENCES dias_semana(dia_semana),"
                "FOREIGN KEY (bimestre) REFERENCES bimestres(bimestre),"
                "FOREIGN KEY (trimestre) REFERENCES trimestres(trimestre)"
                ");")

    conn.commit()
    conn.close()

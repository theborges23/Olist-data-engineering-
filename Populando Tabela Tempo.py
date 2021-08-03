DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"

import psycopg2

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
with conn.cursor() as cur:

    '''
    #PREENCHENDO A TABELA anos:
    for i in range(2016, 2021):
        cur.execute("INSERT INTO anos(ano_id, ano)"
                    "VALUES (%s, %s);", (i, i))
    

    #PREENCHENDO A TABELA BIMESTRES
    for i in range(1, 5):
        cur.execute("INSERT INTO bimestres(bimestre_id, bimestre)"
                    "VALUES (%s, %s);", (i, i))

    #PREENCHENDO A TABELA TRIMESTRES
    for i in range(1, 4):
        cur.execute("INSERT INTO trimestres(trimestre_id, trimestre)"
                    "VALUES (%s, %s);", (i, i))
    
    
    #PREENCHENDO A TABELA num_meses
    for i in range(1, 13):
        cur.execute("INSERT INTO num_meses(num_mes_id, num_mes)"
                    "VALUES (%s, %s);", (i, i))
                    
    #for i, j in zip(range(x), range(y)):

    #PREENCHENDO A TABELA nome_meses

    cur.execute("INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (1, 'janeiro');"
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (2, 'fevereiro'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (3, 'março'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (4, 'abril'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (5, 'maio'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (6, 'junho'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (7, 'julho'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (8, 'agosto'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (9, 'setembro'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (10, 'outubro'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (11, 'novembro'); "
                "INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES (12, 'dezembro');")
    
    # PREENCHENDO A TABELA dias_mes
    for i in range(1, 32):
        cur.execute("INSERT INTO dias_mes(dia_mes_id, dia_mes)"
                    "VALUES (%s, %s);", (i, i))
    

    # PREENCHENDO A TABELA dias_semana
    cur.execute("INSERT INTO dias_semana(dia_semana_id, dia_semana)"
                "VALUES "
                "(1, 'domingo'),"
                "(2, 'segunda-feira'),"
                "(3, 'terça-feira'),"
                "(4, 'quarta-feira'),"
                "(5, 'quinta-feira'),"
                "(6, 'sexta-feira'),"
                "(7, 'sábado');")
                
    
    #ALTERANDO FOREIGN KEYS PARA OS VALORES DESEJADOS
    cur.execute("ALTER TABLE tempo "
                "DROP CONSTRAINT tempo_ano_id_fkey, "
                "DROP CONSTRAINT tempo_bimestre_id_fkey, "
                "DROP CONSTRAINT tempo_dia_mes_id_fkey, "
                "DROP CONSTRAINT tempo_dia_semana_id_fkey, "
                "DROP CONSTRAINT tempo_mes_nome_id_fkey, "
                "DROP CONSTRAINT tempo_num_mes_id_fkey, "
                "DROP CONSTRAINT tempo_trimestre_id_fkey;")

    cur.execute("ALTER TABLE tempo "
                "RENAME COLUMN dia_mes_id TO dia_mes; "
                "ALTER TABLE tempo "
                "RENAME COLUMN ano_id TO ano;"
                "ALTER TABLE tempo "
                "RENAME COLUMN mes_nome_id TO mes_nome; "
                "ALTER TABLE tempo "
                "RENAME COLUMN num_mes_id TO mes; "
                "ALTER TABLE tempo "
                "RENAME COLUMN dia_semana_id TO dia_semana; "
                "ALTER TABLE tempo "
                "RENAME COLUMN bimestre_id TO bimestre; "
                "ALTER TABLE tempo "
                "RENAME COLUMN trimestre_id TO trimestre;")

    #ADICIONANDO CONSTRAINTS NECESSÁRIOS PARA AS FOREIGN KEYS, ALGUMAS DAS COLUNAS NAO ERAM UNIQUE
    cur.execute("ALTER TABLE dias_mes ADD CONSTRAINT mesunique UNIQUE (dia_mes); "
                "ALTER TABLE anos ADD CONSTRAINT anounique UNIQUE (ano); "
                ""
    #ALGUNS TIPOS DE VARIAVEIS ESTAVAM ERRADOS, TROCANDO NOME DO MES E DIA DA SEMANA PARA VARCHAR
                "ALTER TABLE tempo "
                "ALTER COLUMN mes_nome TYPE VARCHAR(15),"
                "ALTER COLUMN dia_semana TYPE VARCHAR(15);"
                ""
                ""
                "ALTER TABLE nome_meses ADD CONSTRAINT nomeunique UNIQUE (mes_nome); "
                "ALTER TABLE num_meses ADD CONSTRAINT nummesunique UNIQUE (num_mes); "
                "ALTER TABLE dias_semana ADD CONSTRAINT semanaunique UNIQUE (dia_semana); "
                "ALTER TABLE bimestres ADD CONSTRAINT bimestreunique UNIQUE (bimestre); "
                "ALTER TABLE trimestres ADD CONSTRAINT trimestre UNIQUE (trimestre); "
                "ALTER TABLE tempo "
    #ADICIONANDO FOREIGN KEYS NA TABELA TEMPO
                "ADD FOREIGN KEY (dia_mes) REFERENCES dias_mes(dia_mes), "
                "ADD FOREIGN KEY (ano) REFERENCES anos(ano), "
                "ADD FOREIGN KEY (mes_nome) REFERENCES nome_meses(mes_nome), "
                "ADD FOREIGN KEY (mes) REFERENCES num_meses(num_mes), "
                "ADD FOREIGN KEY (dia_semana) REFERENCES dias_semana(dia_semana), "
                "ADD FOREIGN KEY (bimestre) REFERENCES bimestres(bimestre), "
                "ADD FOREIGN KEY (trimestre) REFERENCES trimestres(trimestre); "
                )
    '''

    #PREENCHENDO A TABELA HORAS
    tempo_nome = []
    tempo = []
    for h in range(24):
        for m in range(60):
            for s in range(60):
                tempo_nome.append(f'{h:02d}:{m:02d}:{s:02d}')
                tempo.append(f'{h:02d}{m:02d}{s:02d}')
    print(tempo)

    for i in range(86400):
        cur.execute("INSERT INTO horas(hora_id, hora)"
                    "VALUES (%s, %s)", (tempo[i], tempo_nome[i]))


conn.commit()

cur.close()

conn.close()
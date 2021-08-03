DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"

import psycopg2
from datetime import datetime

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



    #ADICIONANDO BIMESTRE 5 E 6 E TRIMESTRE 4 ÀS TABELAS BIMESTRE E TRIMESTRE RESPECTIVAMENTE (FICOU FALTANDO)
    for i in range(5, 7):
        cur.execute("INSERT INTO bimestres(bimestre_id, bimestre)"
                    "VALUES (%s, %s)", (i, i))

    cur.execute("INSERT INTO trimestres(trimestre_id, trimestre)"
                "VALUES (%s, %s)", (4, 4))
    
       '''

    tempo_id = []
    tempo_date = []
    def print_data(a,m,d):
        """
        Edita os valores de dia mes e ano para serem adicionados a listas para futura interpolação
        """
        tempo_id.append(f'{d:02d}{m:02d}{a:02d}')
        tempo_date.append(f'{d:02d}/{m:02d}/{a:02d}')


    def dia_mes(date: str) -> str:
        """
        Extrai e retorna o valor do dia de date
        """
        return date[0:2]

    def mes(date: str) -> str:
        """
        Extrai e retorna o valor do mes de date
        """
        return date[2:4]

    def ano(date: str) -> str:
        """
        Extrai e retorna o valor do ano de date
        """
        return '20' + date[4:7]

    def mes_nome(date: str) -> str:
        """
        Recebe um código de uma data em str e retorna o nome do mês em uma string
        :param date: data em str
        :return: nome do mês em str
        """
        m = mes(date)
        if m == '01':
            return 'janeiro'
        elif m == '02':
            return 'fevereiro'
        elif m == '03':
            return 'março'
        elif m == '04':
            return 'abril'
        elif m == '05':
            return 'maio'
        elif m == '06':
            return 'junho'
        elif m == '07':
            return 'julho'
        elif m == '08':
            return 'agosto'
        elif m == '09':
            return 'setembro'
        elif m == '10':
            return 'outubro'
        elif m == '11':
            return 'novembro'
        elif m == '12':
            return 'dezembro'

    def dia_semana(date: str) -> str:
        """
        Recebe um valor de data em str e retorna qual era o dia da semana naquela data
        :param date: str
        :return: str contendo o dia da semana
        """
        dia = datetime(int(ano(date)), int(mes(date)), int(dia_mes(date)))
        weekday = dia.weekday()
        if weekday == 0:
            return 'segunda-feira'
        elif weekday == 1:
            return 'terça-feira'
        elif weekday == 2:
            return 'quarta-feira'
        elif weekday == 3:
            return 'quinta-feira'
        elif weekday == 4:
            return 'sexta-feira'
        elif weekday == 5:
            return 'sábado'
        elif weekday == 6:
            return 'domingo'


    def bimestre(date: str) -> int:
        """
        Recebe um código de data e devolve em que bimestre aquela data se encontra.
        :param date: str
        :return: int
        """
        m = int(mes(date))
        if 1 <= m <= 2:
            return 1
        elif 3 <= m <= 4:
            return 2
        elif 5 <= m <= 6:
            return 3
        elif 7 <= m <= 8:
            return 4
        elif 9 <= m <= 10:
            return 5
        elif 11 <= m <= 12:
            return 6


    def trimestre(date: str) -> int:
        """
        Recebe um código de data e devolve em que trimestre aquela data se encontra.
        :param date: str
        :return: int
        """
        m = int(mes(date))
        if 1 <= m <= 3:
            return 1
        elif 4 <= m <= 6:
            return 2
        elif 7 <= m <= 9:
            return 3
        elif 10 <= m <= 12:
            return 4






    # Gerando os códigos dos dias no perído selecionado e adicionando às listas respectivas
    for a in range(16, 21):
        for m in range(1, 13):
            if m in [1, 3, 5, 7, 8, 10, 12]:
                for d in range(1,32):
                    print_data(a,m,d)
            elif m in [4, 6, 9, 11]:
                for d in range(1, 31):
                    print_data(a,m,d)
            elif m == 2 and a % 4 == 0:
                for d in range(1, 30):
                    print_data(a,m,d)
            else:
                for d in range(1, 29):
                    print_data(a,m,d)

    for i in range(1827):

        cur.execute("INSERT INTO tempo(tempo_id, date, dia_mes, mes, ano, mes_nome, dia_semana, bimestre, trimestre)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (tempo_id[i], tempo_date[i], dia_mes(tempo_id[i]),
                    mes(tempo_id[i]), ano(tempo_id[i]), mes_nome(tempo_id[i]), dia_semana(tempo_id[i]),
                    bimestre(tempo_id[i]), trimestre(tempo_id[i])))


conn.commit()

cur.close()

conn.close()
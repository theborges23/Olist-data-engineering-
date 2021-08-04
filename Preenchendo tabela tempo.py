import psycopg2
from datetime import datetime

# Inserir dados do servidor

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

with conn.cursor() as cur:

    # PREENCHENDO A TABELA dias_mes
    for i in range(1, 32):
        cur.execute("INSERT INTO dias_mes(dia_id, dia)"
                    "VALUES (%s, %s);", (i, i))

    # PREENCHENDO A TABELA mes
    for i in range(1, 13):
        cur.execute("INSERT INTO meses(mes_id, mes)"
                    "VALUES (%s, %s);", (i, i))

    #PREENCHENDO A TABELA anos:
    ano_inicio = 2016
    ano_fim = 2020
    for i in range(ano_inicio, ano_fim + 1):
        cur.execute("INSERT INTO anos(ano_id, ano)"
                    "VALUES (%s, %s);", (i, i))

    # PREENCHENDO A TABELA nome_meses
    cur.execute("INSERT INTO nome_meses(mes_nome_id, mes_nome)"
                "VALUES "
                "(1, 'janeiro'), "
                "(2, 'fevereiro'), "
                "(3, 'março'), "
                "(4, 'abril'), "
                "(5, 'maio'), "
                "(6, 'junho'), "
                "(7, 'julho'), "
                "(8, 'agosto'), "
                "(9, 'setembro'), "
                "(10, 'outubro'), "
                "(11, 'novembro'), "
                "(12, 'dezembro');")

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

    # PREENCHENDO A TABELA BIMESTRES
    for i in range(1, 7):
        cur.execute("INSERT INTO bimestres(bimestre_id, bimestre)"
                    "VALUES (%s, %s);", (i, i))

    # PREENCHENDO A TABELA TRIMESTRES
    for i in range(1, 5):
        cur.execute("INSERT INTO trimestres(trimestre_id, trimestre)"
                    "VALUES (%s, %s);", (i, i))

    # PREENCHENDO A TABELA HORAS
    hora_id = []
    hora = []
    for h in range(24):
        for m in range(60):
            for s in range(60):
                hora.append(f'{h:02d}:{m:02d}:{s:02d}')
                hora_id.append(f'{h:02d}{m:02d}{s:02d}')
    for i in range(86400):
        cur.execute("INSERT INTO horas(hora_id, hora)"
                    "VALUES (%s, %s)", (hora_id[i], hora[i]))

    #PREENCHENDO A TABELA tempo
    """
    tempo_id GUARDA O CÓDIGO DA DATA, COMPOSTO DE UMA STRING DE 6 DÍGITOS (******), ZEROS À ESQUERDA TAMBÉM SERÃO 
    CONSIDERADOS (EX: 01/12/19 -> 011219)
    tempo_date É O MESMO QUE tempo_id PORÉM COM AS BARRAS DIVIDINDO OS VALORES
    """

    tempo_id = []
    tempo_date = []

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

    def print_data(a,m,d):
        """
        Edita os valores para gerar os códigos das datas
        """
        tempo_id.append(f'{d:02d}{m:02d}{a:02d}')
        tempo_date.append(f'{d:02d}/{m:02d}/{a:02d}')


    """
    Gerando o id das datas dos anos selecionados
    """
    ys = str(ano_inicio)
    ys = int(ys[2:4])

    ye = str(ano_fim)
    ye = int(ye[2:4])
    for a in range(ys, ye + 1):
        for m in range(1, 13):
            if m in [1, 3, 5, 7, 8, 10, 12]:
                for d in range(1, 32):
                    print_data(a, m, d)
            elif m in [4, 6, 9, 11]:
                for d in range(1, 31):
                    print_data(a, m, d)
            elif m == 2 and a % 4 == 0:
                for d in range(1, 30):
                    print_data(a, m, d)
            else:
                for d in range(1, 29):
                    print_data(a, m, d)

    for i in range(len(tempo_id)):
        cur.execute("INSERT INTO tempo(tempo_id, date, dia_mes, mes, ano, mes_nome, dia_semana, bimestre, trimestre)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (tempo_id[i], tempo_date[i], dia_mes(tempo_id[i]),
                                                                     mes(tempo_id[i]), ano(tempo_id[i]),
                                                                     mes_nome(tempo_id[i]), dia_semana(tempo_id[i]),
                                                                     bimestre(tempo_id[i]), trimestre(tempo_id[i])))

    conn.commit()
    conn.close()
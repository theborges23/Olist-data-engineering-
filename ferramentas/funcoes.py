import csv
from typing import Union
import psycopg2

DB_HOST = "localhost"
DB_NAME = "olistic"
DB_USER = "postgres"
DB_PASS = "teste"


def formata_data(data: str) -> Union[None, str]:
    """
    Função que recebe uma string contendo a data, formata e devolve uma string pronta para inserção no banco de dados
    """
    if data == '':
        return None
    return data[8:10] + '/' + data[5:7] + '/' + data[2:4]


def formata_hora(hora: str) -> Union[None, str]:
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


def devolve_null(valor: str) -> Union[None, str]:
    """
    Verifica um valor, e se este for uma string vazia, retorna none.
    :param valor: string
    :return: None ou o próprio valor
    """
    if valor == '':
        return None
    else:
        return valor


def ler_csv(arquivo: str) -> list:
    """
    Recebe o nome de um arquivo csv e retorna o objeto resultante da leitura
    """
    with open(arquivo, 'r') as arquivo:
        leitura = csv.reader(arquivo)
        leitura = list(leitura)
        return leitura

def cadastrados(tabela: str) -> list:
    """
    Recebe o nome da tabela em uma string e retorna um objeto resultante da leitura do banco de datos daquela tabela
    """
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM %s;", (tabela, ))
        leitura = cur.fetchall()
        leitura = list(leitura)
        conn.commit()
        conn.close()

        return leitura


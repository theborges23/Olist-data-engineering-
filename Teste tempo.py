

import csv

def formata_data(data: str) -> str:
    """
    Função que recebe uma string contendo a data, formata e devolve uma string pronta para inserção no banco de dados
    """
    return data[8:10] + '/' + data[5:7] + '/' + data[2:4]

with open('olist_orders_dataset.csv', 'r') as pedidos:
    scv_reader = csv.reader(pedidos)
    next(scv_reader)
    for linha in scv_reader:
        print(formata_data(linha[3]))


import csv
import os

PATH_ARQUIVO = r'C:\Users\diaxt\Desktop\INDICE_IBOVESPA\PRINCIPAIS_ACOES_IBOVESPA.csv'
ATIVOS = set()

with open(PATH_ARQUIVO, newline='') as csvfile:
    arquivo = csv.reader(csvfile, delimiter=',')
    for linha in arquivo:
        if linha[0] == 'ANO':
            continue

        if int(linha[0]) < 2021:
            continue
        linha.pop(0)
        linha.pop(0)

        tickers = [ativo for ativo in linha if ativo != '-']
        
        for ticker in tickers:
            ATIVOS.add(ticker)      


nome_arquivo = 'ATIVOS_IBOV.csv'
with open(nome_arquivo, 'w', newline='') as csvfile:
    linha = csv.writer(csvfile, delimiter=';')
    for ativo in ATIVOS:
        print(ativo)
        linha.writerow([ativo])
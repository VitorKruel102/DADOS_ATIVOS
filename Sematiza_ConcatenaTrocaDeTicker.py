"""
Utilizado para concatener os dados que tiverão a troca de ticker.

Existe um arquivo chamado 'ativos-mudou-ticker.json' que contém
todos os ativos que mudaram de ticker em 2021 para frente.

Criado: 20/07/2023 09:00

Author: Vitor Kruel.
"""
import csv
import json
import os
import pandas as pd


from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

DIRETORIO_DADOS_INTRADAY = r'E:\DADOS_FINANCEIROS\DADOS\Database_PrincipaisAcoes'
DIRETORIO_JSON = os.path.join(BASE_DIR, 'ativos-mudaram-ticker.json')

class ConcatenaTrocaAtivos:
    def __init__(self) -> None:
        self.ativos_de_interesse = self.retorna_ativos_que_mudaram_de_ticker()

        for ativo in self.ativos_de_interesse.keys():

            if self.existe_ativo_no_diretorio_dados_intraday(ativo):
                print(ativo)
            else:
                print('Not File')

    
    def existe_ativo_no_diretorio_dados_intraday(self, ativo):
        """Verifica se existe ativos no diretorio informado do intraday."""
        path_ativo = os.path.join(DIRETORIO_DADOS_INTRADAY, f'{ativo}_BMF_I.csv')
        if os.path.exists(path_ativo):
            return True
        return False


    def retorna_ativos_que_mudaram_de_ticker(self) -> dict:
        """Retorna um dicionário com os ativos que mudaram
        de tickers dentro do arquivo ativos-mudaram-ticker.json"""
        with open(DIRETORIO_JSON, 'r') as arquivo:
            return json.load(arquivo)


ConcatenaTrocaAtivos()
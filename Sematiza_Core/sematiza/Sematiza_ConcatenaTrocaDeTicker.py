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

DIRETORIO_DADOS_PARA_AJUSTE = r'D:\DADOS_FINANCEIROS\Database_DadosParaAjuste'
DIRETORIO_JSON = os.path.join(BASE_DIR, 'ativos-mudaram-ticker.json')

class ConcatenaTrocaAtivos:
    def __init__(self) -> None:
        self.ativos_de_interesse = self.retorna_ativos_que_mudaram_de_ticker()

        for ticker_antigo, ticker_novo in self.ativos_de_interesse.items():
            print(ticker_antigo)
            if not self.existe_ativo_no_diretorio_dados_para_ajuste(ticker_antigo):
                continue

            df_ticker_antigo = pd.read_csv(self.retorna_path_do_ticker(ticker_antigo), sep=';') 
            df_ticker_novo = pd.read_csv(self.retorna_path_do_ticker(ticker_novo), sep=';') 

            df_ticker_novo_filtrado = df_ticker_novo[df_ticker_novo['<date>'] > df_ticker_antigo['<date>'].iloc[-1]]
        
            df_concatenado = pd.concat([df_ticker_antigo, df_ticker_novo_filtrado])
            df_concatenado['<ticker>'] = ticker_novo

            os.remove(self.retorna_path_do_ticker(ticker_antigo))
            df_concatenado.to_csv(os.path.join(DIRETORIO_DADOS_PARA_AJUSTE, f'{ticker_novo}_BMF_I.csv'), sep=';', index=False)
            
    def retorna_path_do_ticker(self, ativo):
        """."""
        return os.path.join(DIRETORIO_DADOS_PARA_AJUSTE, f'{ativo}_BMF_I.csv')
    
    def existe_ativo_no_diretorio_dados_para_ajuste(self, ativo):
        """Verifica se existe ativos no diretorio informado do intraday."""
        path_ativo = os.path.join(DIRETORIO_DADOS_PARA_AJUSTE, f'{ativo}_BMF_I.csv')
        if os.path.exists(path_ativo):
            return True
        return False


    def retorna_ativos_que_mudaram_de_ticker(self) -> dict:
        """Retorna um dicionário com os ativos que mudaram
        de tickers dentro do arquivo ativos-mudaram-ticker.json"""
        with open(DIRETORIO_JSON, 'r') as arquivo:
            return json.load(arquivo)


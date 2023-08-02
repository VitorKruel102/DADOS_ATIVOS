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
import sys
import pandas as pd

from datetime import datetime


BASE_DIR = os.getcwd()

sys.path.insert(0, os.path.join(BASE_DIR, 'config'))
import settings as _settings

sys.path.insert(1, os.path.join(BASE_DIR, 'utils'))
from log_sematiza import LogFileMixin, LogPrintMixin

class ConcatenaTrocaAtivos:


    def __init__(self) -> None:
        self.ativos_de_interesse = self.retorna_ativos_que_mudaram_de_ticker()

        self._log_inicializao = LogFileMixin('log-concatena-ativos.txt', reiniciar_arquivo=True)
        self._log_inicializao.success(f'INICIALIZAÇÃO')
        

        self._log_file = LogFileMixin('log-concatena-ativos.txt')
        self._log_file.success(f'{"-" * 80}')

        self._log_print = LogPrintMixin()

        for ticker_antigo, ticker_novo in self.ativos_de_interesse.items():

            if not self.existe_ativo_no_diretorio_dados_para_ajuste(ticker_antigo):
                self._log_file.error(f'NÃO FOI ENCONTRADO O ATIVO {ticker_antigo}')
                self._log_file.success(f'{"-" * 80}')
                continue

            self._log_print.success(f'PROCESSAMENTO: {ticker_antigo} ---> {ticker_novo}')
            
            self._log_file.success(f'DADOS PROCESSADO: {ticker_antigo} ---> {ticker_novo}')
            self._log_file.success(f'{"-" * 80}')
            
            df_ticker_antigo = pd.read_csv(self.retorna_path_do_ticker(ticker_antigo), sep=';') 
            df_ticker_novo = pd.read_csv(self.retorna_path_do_ticker(ticker_novo), sep=';') 

            df_ticker_novo_filtrado = df_ticker_novo[df_ticker_novo['<date>'] > df_ticker_antigo['<date>'].iloc[-1]]
        
            df_concatenado = pd.concat([df_ticker_antigo, df_ticker_novo_filtrado])
            df_concatenado['<ticker>'] = ticker_novo

            os.remove(self.retorna_path_do_ticker(ticker_antigo))
            df_concatenado.to_csv(os.path.join(_settings.DIRETORIO_DADOS_PARA_AJUSTE, f'{ticker_novo}_BMF_I.csv'), sep=';', index=False)
            
    def retorna_path_do_ticker(self, ativo):
        """."""
        return os.path.join(_settings.DIRETORIO_DADOS_PARA_AJUSTE, f'{ativo}_BMF_I.csv')
    
    def existe_ativo_no_diretorio_dados_para_ajuste(self, ativo):
        """Verifica se existe ativos no diretorio informado do intraday."""
        path_ativo = os.path.join(_settings.DIRETORIO_DADOS_PARA_AJUSTE, f'{ativo}_BMF_I.csv')
        if os.path.exists(path_ativo):
            return True
        return False

    def retorna_ativos_que_mudaram_de_ticker(self) -> dict:
        """Retorna um dicionário com os ativos que mudaram
        de tickers dentro do arquivo ativos-mudaram-ticker.json"""
        with open(_settings.PATH_ATIVOS_QUE_MUDARAM_NOME, 'r') as arquivo:
            return json.load(arquivo)


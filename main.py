import json
import shutil
import os
import pandas as pd

from config import settings as _settings

from sematiza.Sematiza_ConcatenaTrocaDeTicker import ConcatenaTrocaAtivos
from sematiza.Sematiza_Alertador import Alertador

class ManageSematiza:


    def __init__(self) -> None:
        self.remover_arquivos_do_diretorio(_settings.DIRETORIO_DADOS_PARA_AJUSTE)
        
        ativos_de_interesse = self.retorna_listas_de_tickers_de_interesse()
        self.copia_arquivos_intraday_para_outro_diretorio(
            _settings.DIRETORIO_INTRADAY_MINUTOS,
            _settings.DIRETORIO_DADOS_PARA_AJUSTE,
            ativos_de_interesse,
        )

        ConcatenaTrocaAtivos()
        Alertador()

    def remover_arquivos_do_diretorio(self, path_diretorio) -> None:
        """."""
        for path, _, arquivos in os.walk(path_diretorio): 
            for arquivo in arquivos:
                path_do_arquivo = os.path.join(path, arquivo)
                
                os.remove(path_do_arquivo)
    
    def retorna_listas_de_tickers_de_interesse(self) -> list:
        """Existe o arquivo na base do projeto chamado dados-interesse,
        são esses tickers que a função vai retornar."""
        with open(_settings.PATH_ARQUIVO_DADOS_INTERESSE, 'r') as arquivo_json:
            return json.load(arquivo_json)

    def copia_arquivos_intraday_para_outro_diretorio(self, 
        path_diretorio: str, 
        path_novo_diretoiro: str, 
        list_de_nomes_arquivos: list
        ) -> None:
        """Copia dados do TradeIntraday para outro diretório."""
        lista_de_ativos =  [f'{x}_BMF_I.csv' for x in list_de_nomes_arquivos]

        for ativo in lista_de_ativos:
            path_do_ativo = os.path.join(path_diretorio, ativo)
            
            if not os.path.exists(path_do_ativo):
                raise 'LOG ERROR'
                
            path_arquivo_diretorio_novo =  os.path.join(path_novo_diretoiro, ativo)
            
            shutil.copyfile(path_do_ativo, path_arquivo_diretorio_novo)



        
if __name__ == '__main__':
    ManageSematiza()

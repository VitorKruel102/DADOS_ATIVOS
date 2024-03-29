"""
Ajustador: O objetivo deste código é aplicar o ajuste por dividendos 
nos dados do Intraday da B3 ou CO41.

Dados Intraday B3:
    - Dados disponibilizados no site da B3.

Dados CO41:
    - Dados disponibilizados pelo Luciano.

`O método utilizado é o seguinte:` obter o fator de diferença entre o 
fechamento do último candle do dia no Intraday ou do CO41 e o fechamento 
do candle diário ajustado por dividendos no Profit. Esse fator de diferença 
será multiplicado em todas as aberturas, máximas, mínimas e fechamentos dos 
dados do Intraday naquele dia, de forma a tornar os dados proporcionais aos 
ajustados por dividendos.

`Requisitos:` Para que o código funcione corretamente, é necessário ter o arquivo 
do Profit ajustado por dividendos com o split correspondente à data dos dados do 
Intraday.

`Variável DATA_INICIO:` Essa variável é utilizada para controlar a atualização dos dados. 
Por padrão, seu valor é None, o que significa que todos os dados ajustados serão apagados 
e o processo de atualização começará do início. No entanto, caso seja fornecida uma data 
no formato AAAAMMDD, a atualização será realizada a partir dessa data, filtrando os dados 
do Intraday ou CO41 e ajustando apenas a partir desse ponto. Essa variável é principalmente 
utilizada para atualizações diárias, permitindo ajustar apenas os dados mais recentes.

Criado: 13/07/2023 09:25

Autor: Vitor Kruel
"""
import csv
import json
import os
import pandas as pd


from pprint import pprint

BASE_DIR = os.getcwd()

DIRETORIO_PROFIT_AJUSTADO_COM_SPLIT = r''
DIRETORIO_INTRADAY_B3 = r''
DIRETORIO_CO41 = r''
DIRETORIO_PARA_SALVAR_DADOS_AJUSTADOS = r'E:\DADOS_FINANCEIROS\DADOS\Database_MinutoAjustado'
PATH_DO_ARQUIVO_DADOS_INTERESSE = os.path.join(BASE_DIR, 'dados-interesse.json')

DIA_INICIO = None


class Ajustador:
    def __init__(self) -> None:
        self.salva_log_desempenho(reiniciar_arquivo=True)        

        if not DIA_INICIO:
            self.deletar_dados_antigos()

        self._estrutura_para_dados_ajustados = self.inicia_estrutura_para_dados_ajustados()
        self.lista_dados_de_interesse = self.retorna_dados_de_interesse()

    def deletar_dados_antigos(self) -> None:
        """."""
        for path, _, arquivos in os.walk(DIRETORIO_PARA_SALVAR_DADOS_AJUSTADOS):
            for arquivo in arquivos:
                path_arquivo = os.path.join(path, arquivo)

                os.remove(path_arquivo)

    def inicia_estrutura_para_dados_ajustados(self) -> dict:
        """."""
        return {
            '<ticker>': [],
            '<date>': [],
            '<time>': [],
            '<trades>': [],
            '<close>': [],
            '<low>': [],
            '<high>': [],
            '<open>': [],
            '<vol>': [],
            '<qty>': [],
            '<aft>': [],
        }

    def retorna_dados_de_interesse(self) -> list:
        """Sua funcionalida é retornar os dados que estão 
        no arquivo dados-interesse.json no formato de lista."""
        with open(PATH_DO_ARQUIVO_DADOS_INTERESSE, 'r') as arquivo:
            return json.load(arquivo)

    def log_erro(self, mensagem) -> None:
        """Utilizado para exibir erros na tela."""
        print('ERRO NO PROCESSAMENTO:')
        print(mensagem)
        print('-' * 80)

    def salva_log_desempenho(self, registro, reiniciar_arquivo=False) -> None:
        """Salva na raiz do projeto o arquivo log-desempenho.txt responsável 
        por registrar o desempenho em locais desejados."""
        if reiniciar_arquivo:
            if os.path.exists('log-desempenho-ajustador.txt'):
                os.remove('log-desempenho-ajustador.txt')

        if os.path.exists('log-desempenho-ajustador.txt'):
            with open('log-desempenho-ajustador.txt', 'a', newline='', encoding='utf-8') as arquivo:
                writer = csv.writer(arquivo)
                writer.writerow([registro])
            return


objeto = Ajustador()

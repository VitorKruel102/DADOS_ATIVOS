"""
Criado: 10/07/2023 17:43

Autor: Vitor Kruel.
"""
from Sematiza_Core.utils import log

import csv
import os
import json
import pandas as pd
pd.options.mode.chained_assignment = None

from pandas_market_calendars import get_calendar
from datetime import datetime
from config import settings as _settings


PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO = 20211013


class Alertador:


    def __init__(self) -> None:
        self.deletar_dados_antigos()

        self.inicio_processamento_global = datetime.now()
        self.salva_log_desempenho(f'INICIALIZAÇÃO: {self.inicio_processamento_global}', reiniciar_arquivo=True)
        self.salva_log_desempenho(f'{"-" * 80}', )

        self.feriados_b3 = self.retornar_feriados_integrais_da_b3()
        self._estrutura_de_dados_com_erros = self.inicializa_campos_de_analise()
        self._estrutura_estatistica = self.inicializa_campos_dados_estatisticos()

        self.enviar_dados_intraday()
        self.finaliza_os_dados_estatisticos()    

        self.salva_log_desempenho(f'FINALIZAÇÃO: {datetime.now()}.')
        self.salva_log_desempenho(f'TEMPO TOTAL DE EXECUÇÃO: {datetime.now() - self.inicio_processamento_global}.')

    def deletar_dados_antigos(self):
        """."""
        for path, _, arquivos in os.walk(_settings.DIRERORIO_ALERTADOR):
            for arquivo in arquivos:
                os.remove(os.path.join(path, arquivo))

    def enviar_dados_intraday(self) -> str:
        """Enviar dados do Intraday para a função gerenciar 
        comparações para validação dos dados."""
        for path, _, arquivos in os.walk(_settings.DIRETORIO_PRINCIPAIS_ACOES):
            for arquivo in arquivos:
                path_arquivo = os.path.join(path, arquivo)

                self.gerenciar_comparacoes(path_arquivo)

    def gerenciar_comparacoes(self, path_arquivo):
        """Sua funcionalidade é gerenciar os processos para a 
        comparação dos dados do Intraday com o do Profit Pro."""
        nome_do_ticker = path_arquivo.split('\\')[-1].split('_')[0]

        df_intraday = pd.read_csv(path_arquivo, sep=';')
        dias_uteis_da_b3 = self.retorna_dias_uteis_b3(df_intraday)
        
        hora_inicial_do_processamento = datetime.now()
        self.salva_log_desempenho(f'{nome_do_ticker} --> Inicializou Processamento: {hora_inicial_do_processamento}')

        for dia in dias_uteis_da_b3:
            dia_int = int(dia.replace('-', '')) #Formato: AAAAMMDD

            if not self.eh_dia_util(dia, self.feriados_b3):
                continue

            if self.eh_dados_ajustados(dia_int):
                df_diario = self.retorna_dataframe_ajustado(nome_do_ticker)
                eh_ajustado = True
            else:
                df_diario = self.retorna_dataframe_nao_ajustado(nome_do_ticker)
                eh_ajustado = False

            if self.finalizou_dados_ajustados(dia_int):
                self.analisar_dados_com_problemas_no_ajustado()
            
            df_diario_dia_atual = df_diario[df_diario['Data'] == dia_int] 
            df_intraday_dia_atual = df_intraday[df_intraday['<date>'] == dia_int]

            if not self.tem_dados_no_dataframe(df_diario_dia_atual):
                self.log_erro(f'{nome_do_ticker}: Não encontrato nos dados do Profit. (Data: {dia_int})')
                continue

            _abertura_diario = round(df_diario_dia_atual['Abertura'].iloc()[0], 2)
            _fechamento_diario = round(df_diario_dia_atual['Fechamento'].iloc()[0], 2)
            
            if not self.tem_dados_no_dataframe(df_intraday_dia_atual):
                self.adiciona_informacoes_na_estrutura_de_dados_com_erros(
                    eh_ajustado=eh_ajustado,
                    INFORMACAO_DIA_INT=dia_int,
                    INFORMACAO_TICKER=nome_do_ticker,
                    INFORMACAO_ABERTURA_DIARIO=_abertura_diario,
                    INFORMACAO_ABERTURA_MINUTO=0,
                    INFORMACAO_DIFERENCA_ABERTURA=0,
                    INFORMACAO_FECHAMENTO_DIARIO=_fechamento_diario,
                    INFORMACAO_FECHAMENTO_MINUTO=0,
                    INFORMACAO_DIFERENCA_FECHAMENTO=0,
                )
                continue

            _abertura_intraday = round(df_intraday_dia_atual['<open>'].iloc()[0], 2)
            _fechamento_intraday = round(df_intraday_dia_atual['<close>'].iloc()[-1], 2)

            if self.primeira_e_segunda_validacao_for_ok(
                _abertura_intraday,
                _abertura_diario,
                _fechamento_intraday,
                _fechamento_diario
                ):
                continue

            self.adiciona_informacoes_na_estrutura_de_dados_com_erros(
                eh_ajustado=eh_ajustado,
                INFORMACAO_DIA_INT=dia_int,
                INFORMACAO_TICKER=nome_do_ticker,
                INFORMACAO_ABERTURA_DIARIO=_abertura_diario,
                INFORMACAO_ABERTURA_MINUTO=_abertura_intraday,
                INFORMACAO_DIFERENCA_ABERTURA=round(_abertura_diario / _abertura_intraday, 3),
                INFORMACAO_FECHAMENTO_DIARIO=_fechamento_diario,
                INFORMACAO_FECHAMENTO_MINUTO=_fechamento_intraday,
                INFORMACAO_DIFERENCA_FECHAMENTO=round(_fechamento_diario / _fechamento_intraday, 3),
            )           

        df_dados_com_problemas = pd.DataFrame(self._estrutura_de_dados_com_erros)
        
        if not self.tem_dados_no_dataframe(df_dados_com_problemas):
            return

        self.adicionar_dados_na_estrutura_estatistica(
            df_intraday, 
            df_dados_com_problemas, 
            nome_do_ticker
        )

        self.salva_dados_no_diretorio_alertador(df_dados_com_problemas, f'{nome_do_ticker}_estudo.csv')  
        self.resetar_estrututa(self._estrutura_de_dados_com_erros) 

        self.salva_log_desempenho(f'{nome_do_ticker} --> Finalizou Processamento: {datetime.now()}')
        self.salva_log_desempenho(f'{nome_do_ticker} --> Tempo total de processamento: {datetime.now() - hora_inicial_do_processamento}')
        self.salva_log_desempenho(f'{"-" * 80}', )

    def log_erro(self, mensagem) -> None:
        """Exibe na tela o erro."""
        print(f'ERROR NO PROCESSAMENTO:')
        print(mensagem)
        print('-' * 50)
 
    def salva_log_desempenho(self, registro, reiniciar_arquivo=False) -> None:
        """Salva na raiz do projeto o arquivo log-desempenho.txt responsável 
        por registrar o desempenho em locais desejados."""
        path_log = os.path.join(_settings.DIRETORIO_LOG)
        if reiniciar_arquivo:
            if os.path.exists(path_log):
                os.remove(path_log)

        with open(path_log, 'a', newline='', encoding='utf-8') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([registro])

    def retornar_feriados_integrais_da_b3(self) -> list:
        """Retorna dados do arquivo feriados-b3.json com todos os feriados integrais da b3."""
        with open(_settings.PATH_ARQUIVO_FERIADOS, 'r') as arquivo:
            return json.load(arquivo)["feriado-dia-inteiro"]

    def eh_dia_util(self, dia, feriados_b3) -> bool:
        """Vai verificar se realmente é um dia útil ou não."""
        if dia in feriados_b3:
            return False
        return True

    def eh_dados_ajustados(self, dia_int) -> bool:
        """Verifica se o dia está no período de dados ajustados ou não."""
        if dia_int < PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO:
            return True
        return False
        
    def retorna_dataframe_ajustado(self, ticker, separador=';'):
        """."""
        path_diario = os.path.join(_settings.DIRETORIO_PROFIT_AJUSTADO_COM_SPLIT, f'{ticker}_DIARIO_NAS.csv')
        
        df_diario = pd.read_csv(path_diario, sep=separador)
        df_diario['Data'] = df_diario['Data'].astype(int)
        return df_diario

    def retorna_dataframe_nao_ajustado(self, ticker, separador=';'):
        """."""
        path_diario = os.path.join(_settings.DIRETORIO_PROFIT_SEM_AJUSTE, f'{ticker}_DIARIO.csv')

        df_diario = pd.read_csv(path_diario, sep=separador) 
        df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
        df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
        df_diario['Data'] = df_diario['Data'].astype(int)

        return df_diario

    def finalizou_dados_ajustados(self, dia_int) -> bool:
        """Verifica se o dia atual é o primeiro dia com dados não ajustados."""
        if dia_int == PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO:
            return True
        return False

    def analisar_dados_com_problemas_no_ajustado(self):
        """Utilizamos essa função para realizar à moda dos dados que tiveram 
        problemas e se necessário ver se as diferenças são iguais à moda."""
        df_dados_com_erros = pd.DataFrame(self._estrutura_de_dados_com_erros)

        if not self.tem_dias_suficientes_de_erros(df_dados_com_erros['<data>']):
            return

        moda_diferenca_abertura = df_dados_com_erros['<diferenças_das_aberturas>'].mode()

        df_comparacao = df_dados_com_erros[df_dados_com_erros['<diferenças_das_aberturas>'] != moda_diferenca_abertura[0]]
        df_comparacao['<moda>'] = moda_diferenca_abertura[0]
        
        self._estrutura_de_dados_com_erros = df_comparacao.to_dict('list')
        
    def tem_dias_suficientes_de_erros(self, array_data) -> bool:
        """Retorna se o array tem mais ou pelo menos igual a 20 elementos."""
        if len(array_data) >= 20:
            return True
        return False
    
    def tem_dados_no_dataframe(self, daaframe) -> bool:
        """Retorna se o DataFrame tem pelo menos uma linha."""
        if daaframe.shape[0] == 0:
            return False
        return True

    def adiciona_informacoes_na_estrutura_de_dados_com_erros(self, eh_ajustado=True,**kwargs):
        """."""
        dia_int = kwargs.get('INFORMACAO_DIA_INT')
        nome_do_ticker = kwargs.get('INFORMACAO_TICKER')
        abertura_diario = kwargs.get('INFORMACAO_ABERTURA_DIARIO')
        abertura_do_minuto = kwargs.get('INFORMACAO_ABERTURA_MINUTO')
        diferenças_das_aberturas = kwargs.get('INFORMACAO_DIFERENCA_ABERTURA')
        fechamento_do_diario = kwargs.get('INFORMACAO_FECHAMENTO_DIARIO')
        fechamento_do_minuto = kwargs.get('INFORMACAO_FECHAMENTO_MINUTO')
        diferenças_dos_fechamento = kwargs.get('INFORMACAO_DIFERENCA_FECHAMENTO')

        self._estrutura_de_dados_com_erros['<data>'].append(dia_int)
        self._estrutura_de_dados_com_erros['<ticker>'].append(nome_do_ticker)
        self._estrutura_de_dados_com_erros['<abertura_do_diario>'].append(abertura_diario)
        self._estrutura_de_dados_com_erros['<abertura_do_minuto>'].append(abertura_do_minuto)
        self._estrutura_de_dados_com_erros['<diferenças_das_aberturas>'].append(diferenças_das_aberturas)
        self._estrutura_de_dados_com_erros['<fechamento_do_diario>'].append(fechamento_do_diario)
        self._estrutura_de_dados_com_erros['<fechamento_do_minuto>'].append(fechamento_do_minuto)
        self._estrutura_de_dados_com_erros['<diferenças_dos_fechamento>'].append(diferenças_dos_fechamento)

        if eh_ajustado:
            self._estrutura_de_dados_com_erros['<problemas>'].append('Ajustado')
            self._estrutura_de_dados_com_erros['<moda>'].append(0)
            return

        self._estrutura_de_dados_com_erros['<problemas>'].append('Nao_Ajustado')
        self._estrutura_de_dados_com_erros['<moda>'].append(1)    

    def primeira_e_segunda_validacao_for_ok( 
        self, abertura_intraday, abertura_diario, fechamento_intraday, fechamento_diario
        ) -> bool:
        """Realiza a comparação entre as aberturas (Intrada vs Diario) 
        e fechamentos(Intrada vs Diario)"""
        if (abertura_intraday == abertura_diario) and (fechamento_intraday == fechamento_diario):
            return True
        return False

    def retorna_dias_uteis_b3(self, dataframe_intraday, tipo_calendario='B3'):
        """Retorna os dias úteis no periodo do dataframe intraday."""
        primeira_data = datetime.strptime(str(dataframe_intraday['<date>'].iloc[0]), "%Y%m%d").strftime("%Y-%m-%d")
        ultima_data = datetime.strptime(str(dataframe_intraday['<date>'].iloc[-1]), "%Y%m%d").strftime("%Y-%m-%d")

        b3_calendario = get_calendar(tipo_calendario)
        calendario = b3_calendario.schedule(start_date=primeira_data, end_date=ultima_data)
        return calendario.index.astype(str)

    def inicializa_campos_de_analise(self) -> dict:
        """É utilizado para retornar os campos que irão para as 
        planilhas dos ativos para as comparações com erros."""
        return {
            '<data>': [],
            '<ticker>' : [],
            '<abertura_do_diario>': [],
            '<abertura_do_minuto>': [],
            '<diferenças_das_aberturas>': [],
            '<fechamento_do_diario>': [],
            '<fechamento_do_minuto>': [],
            '<diferenças_dos_fechamento>': [],
            '<moda>': [],
            '<problemas>': [],
        }

    def inicializa_campos_dados_estatisticos(self):
        """É utilizado para criar uma planilha com todos os ativos e 
        seu percentual de integridade."""
        return {
            '<ticker>': [],
            '<fonte_nelogica>': [],
            '<periodo_inicial_nelogica>': [],
            '<periodo_final_nelogica>': [],
            '<total_dias_nelogica>': [],
            '<integridade_nelogica>': [],
            '<fonte_b3>': [],
            '<periodo_inicial_b3>': [],
            '<periodo_final_b3>': [],
            '<total_dias_b3>': [],
            '<integridade_b3>': [],
            '<total_dias>': [],
            '<dias_com_erros>': [],
            '<integridade_total>': [],
        }

    def adicionar_dados_na_estrutura_estatistica(self, dataframe_intraday, dataframe_dados_com_erros, ticker):
        """."""
        dias_com_erros = dataframe_dados_com_erros.shape[0]
        periodo_dados = len(dataframe_intraday['<date>'].iloc[1:].unique())  

        # Período de Dados Nelogica (Ajustado):
        df_nelogica = dataframe_intraday[dataframe_intraday['<date>'] < PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO]
        df_com_erros_nelogica = dataframe_dados_com_erros[dataframe_dados_com_erros['<data>'] < PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO]
    
        nelogica_periodo_com_erros = len(df_com_erros_nelogica['<data>'].unique()) 
        nelogica_periodo_total = len(df_nelogica['<date>'].iloc[1:].unique())  
        self.formata_data_inteiro_para_datetime(df_nelogica)
        
        # Período de Dados B3 (Não Ajustado):
        df_b3 = dataframe_intraday[dataframe_intraday['<date>'] >= PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO]
        df_com_erros_b3 = dataframe_dados_com_erros[dataframe_dados_com_erros['<data>'] >= PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO]

        b3_periodo_com_erros = len(df_com_erros_b3['<data>'].unique()) 
        b3_periodo_total = len(df_b3['<date>'].unique())  
        self.formata_data_inteiro_para_datetime(df_b3)

        self.formata_data_inteiro_para_datetime(dataframe_intraday) 

        self._estrutura_estatistica['<ticker>'].append(ticker)
        self._estrutura_estatistica['<fonte_nelogica>'].append('Nelogica')
        self._estrutura_estatistica['<periodo_inicial_nelogica>'].append(df_nelogica['<date>'].min())
        self._estrutura_estatistica['<periodo_final_nelogica>'].append(df_nelogica['<date>'].max())
        self._estrutura_estatistica['<total_dias_nelogica>'].append(df_nelogica['<date>'].max() - df_nelogica['<date>'].min())
        self._estrutura_estatistica['<integridade_nelogica>'].append(self.percentual_integridade(nelogica_periodo_total, nelogica_periodo_com_erros))
        ...
        self._estrutura_estatistica['<fonte_b3>'].append('B3')
        self._estrutura_estatistica['<periodo_inicial_b3>'].append(df_b3['<date>'].min())
        self._estrutura_estatistica['<periodo_final_b3>'].append(df_b3['<date>'].max())
        self._estrutura_estatistica['<total_dias_b3>'].append(df_b3['<date>'].max() - df_b3['<date>'].min())
        self._estrutura_estatistica['<integridade_b3>'].append(self.percentual_integridade(b3_periodo_total, b3_periodo_com_erros))
        ...
        self._estrutura_estatistica['<total_dias>'].append(dataframe_intraday['<date>'].max() - dataframe_intraday['<date>'].min())
        self._estrutura_estatistica['<dias_com_erros>'].append(dias_com_erros)
        self._estrutura_estatistica['<integridade_total>'].append(self.percentual_integridade(periodo_dados, dias_com_erros))

    def percentual_integridade(self, periodo_total, periodo_com_erros):
        """Retorna a percentual de integridade dos dados."""
        try:
            return round(((periodo_total - periodo_com_erros) / periodo_total) * 100, 2)
        except ZeroDivisionError:
            return 100.00

    def formata_data_inteiro_para_datetime(self, dataframe, nome_da_coluna_data='<date>'):
        dataframe[nome_da_coluna_data] = dataframe[nome_da_coluna_data].astype(str)
        dataframe[nome_da_coluna_data] = pd.to_datetime(dataframe[nome_da_coluna_data]) 

    def salva_dados_no_diretorio_alertador(self, dataframe, nome_arquivo):
        """Salva os dados no diretorio Database_Alertador."""
        path_ativo = os.path.join(_settings.DIRERORIO_ALERTADOR, nome_arquivo)
        dataframe.to_csv(path_ativo, encoding='utf-8', sep=';', index=False)

    def resetar_estrututa(self, estrutua):
        """Reseta a estrutura para poder 
        salvar novos dados de outro ativo."""
        for coluna in estrutua.keys():
            estrutua[coluna].clear()

    def finaliza_os_dados_estatisticos(self):
        """Cria um dataframe do dados estatísticos e 
        encaminha para salvar dados."""
        df_estatistica = pd.DataFrame(self._estrutura_estatistica)
        self.salva_dados_no_diretorio_alertador(df_estatistica, f'_Estatistica_Tickers.csv')    


objeto = Alertador()
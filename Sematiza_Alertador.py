"""
Alertador - Realiza uma analise estatística para alertar os problemas nos dados,
sua avaliação é feita pela comparação do dados intraday de fechamento e abertura com os dados
do Diário do Profit Pro.

LEMBRETE: Existe dados ajustados e não ajustados no historico
    DIA < 20211013 (AJUSTADOS)
    DIA >= 20211013 (NÃO AJUSTADOS)

Então vamos precisar identificar qual a data para determinar se vamos usar o profit ajustado 
ou não ajustao para a analise

Será realizado dois testes antes de confirmar os problemas nos dados:

    - Comparar a Abertura do do dia no Intraday com o Diario;
    - Comparar Fechamentos do dia no Intraday com o Diario;
    - Os dias que não passaram no teste, será realizado uma moda para ver se mantem constantes
    (Precisa ter no mínimo 10 dias com problemas para tirar a moda).

Foi realizado uma conversa com o luciano, onde foi acordado que se a variação for diferente, porém,
se manter estável no periodo, iremos considerar como dados integros. Para fazer isso, todos os dados
que não foram validados inicialmente pelo fechamento e abertura, foi preciso tirar a moda desses dados
e todos os dias que forem igual a moda foi considerado Ok e passaram no teste.

Lembrando que esses problemas de variação da diferença ocorre apenas no periodo com ajustes por dividentos.

Para o código, precisamos dos dados do Intraday e Profit Diario no mesmo periodo em ambos os arquivos,
para conseguir fazer a validação correta.

Será criado um arquivo para cada ativo que vão conter os dias e as informações dos problemas, mostrando
como foi a diferente entre as comparações de fechamento e abertura, assim como a diferenta com a moda no
segundo teste.

No final, vai criar um relatorio geral com todos os ativos com sua repectiva integrida em porcentagem.

Criado: 10/07/2023 17:43

Autor: Vitor Kruel.
"""
import os
import json
import pandas as pd


from pandas_market_calendars import get_calendar
from datetime import datetime


DIRETORIO_DADOS_INTRADAY = r'E:\DADOS_FINANCEIROS\DADOS\Database_PrincipaisAcoes'
DIRETORIO_DADOS_DIARIOS_AJUSTADOS = r'E:\DADOS_FINANCEIROS\Profit\Database_NAS'
DIRETORIO_DADOS_DIARIOS_SEM_AJUSTE = r'E:\DADOS_FINANCEIROS\Profit\Database_ProfitDiario'
DIRERORIO_PARA_SALVAR_DADOS_ALERTADOR = r'E:\DADOS_FINANCEIROS\DADOS\Database_Alertador' 
DIRETORIO_DADOS_JSON_DOS_FERIADOS_B3 = r'C:\Users\vkrue\OneDrive\Área de Trabalho\GitHub\DADOS_ATIVOS\feriados-b3.json'

PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO = 20211013


class Alertador:
    def __init__(self) -> None:
        self.feriados_b3 = self.retornar_feriados_integrais_da_b3()
        self._estrutura_para_comparacao = self.inicializa_campos_de_analise(None)
        self._estrutura_estatistica = self.inicializa_campos_dados_estatisticos(None)

        self.enviar_dados()

        df_estatistica = pd.DataFrame(self._estrutura_estatistica)
        self.salva_dados_no_diretorio_alertador(df_estatistica, f'Estatistica_Tickes.csv')

    def enviar_dados(self) -> str:
        """."""
        for path, _, arquivos in os.walk(DIRETORIO_DADOS_INTRADAY):
            for arquivo in arquivos:
                path_arquivo = os.path.join(path, arquivo)
                
                self.manipular_dados(path_arquivo)

    def manipular_dados(self, path_arquivo):
        """."""
        nome_do_ticker = path_arquivo.split('\\')[-1].split('_')[0]

        df_intraday = pd.read_csv(path_arquivo, sep=';')
        dias_uteis_da_b3 = self.retorna_dias_uteis_b3(df_intraday)
        
        for dia in dias_uteis_da_b3:
            dia_int = int(dia.replace('-', ''))

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
            
            df_diario_filtrado = df_diario[df_diario['Data'] == dia_int] 
            df_intraday_unique = df_intraday[df_intraday['<date>'] == dia_int]

            if not self.tem_dados_no_dataframe(df_diario_filtrado):
                self.log_erro(f'{nome_do_ticker}: Não encontrato nos dados do Profit. (Data: {dia_int})')
                continue

            _abertura_diario = round(df_diario_filtrado['Abertura'].iloc()[0], 2)
            _fechamento_diario = round(df_diario_filtrado['Fechamento'].iloc()[0], 2)
            
            df_intraday_unique = df_intraday[df_intraday['<date>'] == dia_int]
            if not self.tem_dados_no_dataframe(df_intraday_unique):
                self.adiciona_informacoes_na_estrutura_de_comparacoes(
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

            _abertura_intraday = round(df_intraday_unique['<open>'].iloc()[0], 2)
            _fechamento_intraday = round(df_intraday_unique['<close>'].iloc()[-1], 2)

            if self.primeira_e_segunda_validacao_for_ok(
                _abertura_intraday,
                _abertura_diario,
                _fechamento_intraday,
                _fechamento_diario
                ):
                continue

            self.adiciona_informacoes_na_estrutura_de_comparacoes(
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

        df_dados_com_problemas = pd.DataFrame(self._estrutura_para_comparacao)
        
        if not self.tem_dados_no_dataframe(df_dados_com_problemas):
            return

        self.adicionar_dados_na_estrutura_estatistica(
            df_intraday, 
            df_dados_com_problemas, 
            nome_do_ticker
        )

        self.salva_dados_no_diretorio_alertador(df_dados_com_problemas, f'{nome_do_ticker}_estudo.csv')  
        self.resetar_estrututa(self._estrutura_para_comparacao) 
         
    def log_erro(self, mensagem):
        """."""
        print(f'ERROR NO PROCESSAMENTO:')
        print(mensagem)
        print('-' * 50)
 
    def retornar_feriados_integrais_da_b3(self):
        with open(DIRETORIO_DADOS_JSON_DOS_FERIADOS_B3, 'r') as arquivo:
            return json.load(arquivo)["feriado-dia-inteiro"]

    def eh_dia_util(self, dia, feriados_b3) -> bool:
        """Vai verificar se realmente é um dia útil ou não."""
        if dia in feriados_b3:
            return False
        return True

    def eh_dados_ajustados(self, dia_int):
        """."""
        if dia_int < PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO:
            return True
        return False
        
    def retorna_dataframe_ajustado(self, ticker, separador=';'):
        """."""
        path_diario = os.path.join(DIRETORIO_DADOS_DIARIOS_AJUSTADOS, f'{ticker}_DIARIO_NAS.csv')
        
        df_diario = pd.read_csv(path_diario, sep=separador)
        df_diario['Data'] = df_diario['Data'].astype(int)

        return df_diario

    def retorna_dataframe_nao_ajustado(self, ticker, separador=';'):
        """."""
        path_diario = os.path.join(DIRETORIO_DADOS_DIARIOS_SEM_AJUSTE, f'{ticker}_DIARIO.csv')
        df_diario = pd.read_csv(path_diario, sep=separador) 

        df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
        df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
        df_diario['Data'] = df_diario['Data'].astype(int)
        
        return df_diario

    def finalizou_dados_ajustados(self, dia_int):
        """Verifica se o dia atual é o primeiro dia com dados não ajustados."""
        if dia_int == PRIMEIRA_DATA_COM_DADOS_NAO_AJUSTADO:
            return True
        return False

    def analisar_dados_com_problemas_no_ajustado(self):
        """."""
        df_dados = pd.DataFrame(self._estrutura_para_comparacao)

        if not self.tem_dias_suficientes_de_erros(df_dados['<data>']):
            return

        moda_diferenca_abertura = df_dados['<diferenças_das_aberturas>'].mode()

        df_comparacao = df_dados[df_dados['<diferenças_das_aberturas>'] != moda_diferenca_abertura[0]]
        df_comparacao['<moda>'] = moda_diferenca_abertura[0]
        
        self._estrutura_para_comparacao = df_comparacao.to_dict('list')
        """
        if len(self._estrutura_para_comparacao['Data']) == 0:
            self._estrutura_para_comparacao = {
                item: []
                for item in df_comparacao.to_dict()
            }
        """
        
    def tem_dias_suficientes_de_erros(self, array_data):
        """."""
        if len(array_data) >= 20:
            return True
        return False
    
    def tem_dados_no_dataframe(self, daaframe):
        """."""
        if daaframe.shape[0] == 0:
            return False
        return True

    def adiciona_informacoes_na_estrutura_de_comparacoes(self, eh_ajustado=True,**kwargs):
        """."""
        dia_int = kwargs.get('INFORMACAO_DIA_INT')
        nome_do_ticker = kwargs.get('INFORMACAO_TICKER')
        abertura_diario = kwargs.get('INFORMACAO_ABERTURA_DIARIO')
        abertura_do_minuto = kwargs.get('INFORMACAO_ABERTURA_MINUTO')
        diferenças_das_aberturas = kwargs.get('INFORMACAO_DIFERENCA_ABERTURA')
        fechamento_do_diario = kwargs.get('INFORMACAO_FECHAMENTO_DIARIO')
        fechamento_do_minuto = kwargs.get('INFORMACAO_FECHAMENTO_MINUTO')
        diferenças_dos_fechamento = kwargs.get('INFORMACAO_DIFERENCA_FECHAMENTO')

        self._estrutura_para_comparacao['<data>'].append(dia_int)
        self._estrutura_para_comparacao['<ticker>'].append(nome_do_ticker)
        self._estrutura_para_comparacao['<abertura_do_diario>'].append(abertura_diario)
        self._estrutura_para_comparacao['<abertura_do_minuto>'].append(abertura_do_minuto)
        self._estrutura_para_comparacao['<diferenças_das_aberturas>'].append(diferenças_das_aberturas)
        self._estrutura_para_comparacao['<fechamento_do_diario>'].append(fechamento_do_diario)
        self._estrutura_para_comparacao['<fechamento_do_minuto>'].append(fechamento_do_minuto)
        self._estrutura_para_comparacao['<diferenças_dos_fechamento>'].append(diferenças_dos_fechamento)

        if eh_ajustado:
            self._estrutura_para_comparacao['<problemas>'].append('Ajustado')
            self._estrutura_para_comparacao['<moda>'].append(0)
            return

        self._estrutura_para_comparacao['<problemas>'].append('Nao_Ajustado')
        self._estrutura_para_comparacao['<moda>'].append(1)    

    def primeira_e_segunda_validacao_for_ok( 
        self, abertura_intraday, abertura_diario, fechamento_intraday, fechamento_diario
        ):
        """Realiza a comparação entre as aberturas (Intrada vs Diario) 
        e fechamento(Intrada vs Diario)"""
        if (abertura_intraday == abertura_diario) and (fechamento_intraday == fechamento_diario):
            return True
        return False

    def retorna_dias_uteis_b3(self, dataframe_intraday, tipo_calendario='B3'):
        """."""
        primeira_data = datetime.strptime(str(dataframe_intraday['<date>'].iloc[0]), "%Y%m%d").strftime("%Y-%m-%d")
        ultima_data = datetime.strptime(str(dataframe_intraday['<date>'].iloc[-1]), "%Y%m%d").strftime("%Y-%m-%d")

        b3_calendario = get_calendar(tipo_calendario)
        calendario = b3_calendario.schedule(start_date=primeira_data, end_date=ultima_data)
        return calendario.index.astype(str)

    def inicializa_campos_de_analise(self, estrutura_de_dados) -> dict:
        """É utilizado para retornar os campos que irão 
        para as planilhas dos ativos para as comparações."""
        if not estrutura_de_dados:
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

    def inicializa_campos_dados_estatisticos(self, estrutura_de_dados):
        """É utilizado para criar uma planilha com todos os ativos e 
        seu percentual de integridade."""
        if not estrutura_de_dados:
            return {
                '<ticker>': [],
                '<periodo_inicial>': [],
                '<periodo_final>': [],
                '<total_dias>': [],
                '<dias_com_erros>': [],
                '<percentual_integridade>': [],
            }

    def adicionar_dados_na_estrutura_estatistica(self, daataframe_intraday, dataframe_comparacao, ticker):
        """."""
        dias_com_erros = dataframe_comparacao.shape[0]
        periodo_dados = len(daataframe_intraday['<date>'].iloc[1:].unique())  

        daataframe_intraday['<date>'] = daataframe_intraday['<date>'].astype(str)
        daataframe_intraday['<date>'] = pd.to_datetime(daataframe_intraday['<date>'])   

        self._estrutura_estatistica['<ticker>'].append(ticker)
        self._estrutura_estatistica['<periodo_inicial>'].append(daataframe_intraday['<date>'].min())
        self._estrutura_estatistica['<periodo_final>'].append(daataframe_intraday['<date>'].min())
        self._estrutura_estatistica['<total_dias>'].append(daataframe_intraday['<date>'].max() - daataframe_intraday['<date>'].min())
        self._estrutura_estatistica['<dias_com_erros>'].append(dias_com_erros)
        self._estrutura_estatistica['<percentual_integridade>'].append(round(((periodo_dados - dias_com_erros) / periodo_dados) * 100, 2))

    def salva_dados_no_diretorio_alertador(self, dataframe, nome_arquivo):
        """Salva os dados no diretorio Database_Alertador."""
        path_ativo = os.path.join(DIRERORIO_PARA_SALVAR_DADOS_ALERTADOR, nome_arquivo)
        dataframe.to_csv(path_ativo, sep=';', index=False)

    def resetar_estrututa(self, estrutua):
        """Reseta a estrutura de comparação, para 
        poder salvar novos dados de outro ativo."""
        for coluna in estrutua.keys():
            estrutua[coluna].clear()
    

objeto = Alertador()
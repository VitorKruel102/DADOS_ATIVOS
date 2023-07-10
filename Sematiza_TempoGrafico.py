"""

Criado: 07/07/2023 08:50

Autor: Vitor Kruel
"""
import csv
import os
import pandas as pd
import warnings


from datetime import datetime
from pprint import pprint
from time import sleep


DIRETORIO_PARA_SALVAR_OS_TEMPOS_GRAFICOS = r'D:\DADOS_FINANCEIROS\Database_Sematiza_TemposGraficos'
DIRETORIO_DOS_DADOS_AJUSTADOS = r'D:\DADOS_FINANCEIROS\Database_MinutoAjustado'

DATA_INICIAL_DE_INTERESSE = 20211019 # Formato: AAAAMMDD
HORARIO_DE_INTERESSE_DE_ABERTURA = 1030 # Formato: HHMM

HORA_DE_FECHAMENTO_DO_MERCADO = 18 # Formato: HH
HORA_DE_ABERTURA_DO_MERCADO = 10 # Formato: HH

TEMPOS_GRAFICOS_INTERESSE = [
    '01_MINUTO',  
    '05_MINUTO',      
    '10_MINUTO',      
    '15_MINUTO', 
    '20_MINUTO', 
    '30_MINUTO',  
    '60_MINUTO',
    'DIARIO',
]


class CriaTemposGraficos:
    candles_prontos = {}

    def __init__(self, precisa_deletar_dados=True) -> None:
        """."""
        if precisa_deletar_dados:
            self.deletar_dados_antigos()

        self.enviar_dados()
        self.salvar_dados_manipulados()

    def deletar_dados_antigos(self):
        """."""
        for path, diretorios, arquivos in os.walk(DIRETORIO_PARA_SALVAR_OS_TEMPOS_GRAFICOS):
            for arquivo in arquivos:
                os.remove(os.path.join(path, arquivo))

    def enviar_dados(self):
        """."""
        for _, _, arquivos in os.walk(DIRETORIO_DOS_DADOS_AJUSTADOS):
            for arquivo in arquivos:
                path_arquivo = os.path.join(DIRETORIO_DOS_DADOS_AJUSTADOS, arquivo)
    
                self.manipular_dados(path_arquivo, separador=';')

    def manipular_dados(self, path_arquivo, separador):
        """."""
        DIA_ANALISADO = None
        with open(path_arquivo, 'r', encoding='UTF-8') as arquivo:
            for linha in csv.reader(arquivo, delimiter=separador):
                
                if self.eh_cabecalho(linha):
                    continue

                __data = int(linha[1])
                if not self.eh_periodo_de_interesse(__data):
                    continue

                __time_hhmm = int(str(linha[2])[:4])
                if not self.iniciou_o_mercado(__time_hhmm):
                    continue

                __ticker = linha[0]
                __trades = int(linha[3])
                __close = float(linha[4])
                __low = float(linha[5])
                __high = float(linha[6])
                __open = float(linha[7])
                __vol = float(linha[8])
                __qty = int(linha[9])
                __aft = linha[10]

                __precos = [
                    __open,
                    __high,
                    __low,
                    __close
                ]

                if not DIA_ANALISADO:
                    DIA_ANALISADO = __data
                    dados_do_ativo = self.inicializar_estrutura_dos_dados(__ticker)
                    hora_do_fechamento_do_mercado = self.hora_que_fecha_mercado(__data)
                    self.adiciona_na_estrutura_hora_de_abertura_e_fechamento_primeiro_candle(
                        INIT_TIME=__time_hhmm, 
                        INIT_TICKER=__ticker, 
                        INIT_DADOS=dados_do_ativo
                    )
                    
                if self.mudou_de_dia(__data, DIA_ANALISADO):
                    self.montagem_do_ultimo_candles(
                        MONTAGEM_DADOS = dados_do_ativo,
                        MONTAGEM_DATA = DIA_ANALISADO,
                    )

                    hora_do_fechamento_do_mercado = self.hora_que_fecha_mercado(__data)
                    self.adiciona_na_estrutura_hora_de_abertura_e_fechamento_primeiro_candle(
                        INIT_TIME=__time_hhmm, 
                        INIT_TICKER=__ticker, 
                        INIT_DADOS=dados_do_ativo
                    )
                    DIA_ANALISADO = __data

                if __time_hhmm > hora_do_fechamento_do_mercado:
                    if self.ja_eh_aftermarket(__time_hhmm, hora_do_fechamento_do_mercado):
                        continue
                
                # Armazena dados Diario:
                dados_do_ativo[__ticker]['DIARIO']['<prices>'] += __precos
                dados_do_ativo[__ticker]['DIARIO']['<vol>'].append(__vol)
                dados_do_ativo[__ticker]['DIARIO']['<trades>'].append(__trades)
                dados_do_ativo[__ticker]['DIARIO']['<qty>'].append(__qty)

                for tempo_grafico in TEMPOS_GRAFICOS_INTERESSE:
                    if tempo_grafico == 'DIARIO':
                        continue

                    horario_que_fecha_o_candle = dados_do_ativo[__ticker][tempo_grafico]['horario_de_fechamento'] 
                    if __time_hhmm < horario_que_fecha_o_candle:
                        ...
                        dados_do_ativo[__ticker][tempo_grafico]['<prices>'] += __precos
                        dados_do_ativo[__ticker][tempo_grafico]['<vol>'].append(__vol)
                        dados_do_ativo[__ticker][tempo_grafico]['<trades>'].append(__trades)
                        dados_do_ativo[__ticker][tempo_grafico]['<qty>'].append(__qty)
                    else:
                        self.gerencia_a_montagem_do_candle(
                                GERENCIADOR_HORARIO = __time_hhmm,
                                GERENCIADO_HORA_DO_FECHAMENTO_DO_MERCADO = hora_do_fechamento_do_mercado,
                                GERENCIADOR_DADOS_ATIVOS = dados_do_ativo,
                                GERENCIADOR_TICKER = __ticker,
                                GERENCIADOR_TEMPO_GRAFICO = tempo_grafico,
                                GERENCIADOR_DATA_ATUAL = __data,
                                GERENCIADOR_PRECOS = __precos,
                                GERENCIADOR_VOLUME = __vol,
                                GERENCIADOR_TRADES = __trades,
                                GERENCIADOR_QTY = __qty,    
                        )

            self.montagem_do_ultimo_candles(
                MONTAGEM_DADOS = dados_do_ativo,
                MONTAGEM_DATA = __data,
            )

    def eh_cabecalho(self, linha):
        """Verifica se a Linha é o Cabeçalho."""
        if linha[0] == '<ticker>':
            return True
        return False

    def eh_periodo_de_interesse(self, data):
        """Verifica se a data atual está 
        dentro do período de interesse."""
        if data < DATA_INICIAL_DE_INTERESSE:
            return False
        return True

    def iniciou_o_mercado(self, time):
        """Verifica se o horário atual está dentro
         do horário de interesse da abertura."""
        if time < HORARIO_DE_INTERESSE_DE_ABERTURA:
            return False
        return True

    def mudou_de_dia(self, data_atual, data_analisada):
        """Verifica se recomeçou um novo dia."""
        if data_atual != data_analisada:
            return True
        return False

    def inicializar_estrutura_dos_dados(self, ticker):
        """É responsável em construir a estrutura dos dados 
        que será utilizado para a monstagem dos candles."""
        estrutura = {ticker: {}}

        for tempo_de_interesse in TEMPOS_GRAFICOS_INTERESSE:
            if tempo_de_interesse == 'DIARIO':
                estrutura[ticker].update({
                    tempo_de_interesse: {
                        '<qty>'  : [],
                        '<prices>': [],
                        '<vol>'  : [],
                        '<trades>': [],
                    }
                })
            else:
                estrutura[ticker].update({
                    tempo_de_interesse: {
                        'horario_de_fechamento': None,
                        'horario_de_abertura': None,
                        '<qty>'  : [],
                        '<prices>': [],
                        '<vol>'  : [],
                        '<trades>': []  
                    }
                })

        assert any(estrutura)
        return estrutura

    def hora_que_fecha_mercado(self, data_atual):
        """Retornar o horário de fechamento do 
        mercado em relação ao dia informado."""
        if data_atual >= 20041103 and data_atual < 20050402:
            return 1755
        elif data_atual >= 20050402 and data_atual < 20051017:
            return 1655
        elif data_atual >= 20051017 and data_atual < 20060403:
            return 1755
        elif data_atual >= 20060403 and data_atual < 20061106:
            return 1655
        elif data_atual >= 20061106 and data_atual < 20070312:
            return 1755
        elif data_atual >= 20070312 and data_atual < 20071015:
            return 1655
        elif data_atual >= 20071015 and data_atual < 20080310:
            return 1755
        elif data_atual >= 20080310 and data_atual < 20081020:
            return 1655
        elif data_atual >= 20081020 and data_atual < 20090309:
            return 1755
        elif data_atual >= 20090309 and data_atual < 20091019:
            return 1655
        elif data_atual >= 20091019 and data_atual < 20100315:
            return 1755
        elif data_atual >= 20100315 and data_atual < 20101018:
            return 1655
        elif data_atual >= 20101018 and data_atual < 20110314:
            return 1755
        elif data_atual >= 20110314 and data_atual < 20111017:
            return 1655
        elif data_atual >= 20111017 and data_atual < 20120312:
            return 1755
        elif data_atual >= 20120312 and data_atual < 20121203:
            return 1655
        elif data_atual >= 20121203 and data_atual < 20130708:
            return 1755
        elif data_atual >= 20130708 and data_atual < 20151221:
            return 1655
        elif data_atual >= 20151221 and data_atual < 20160229:
            return 1755
            """
            Existe um GAP
            """
        elif data_atual >= 20180310 and data_atual < 20181105:
            return 1655
        elif data_atual >= 20181105 and data_atual < 20190308:
            return 1755 
        elif data_atual >= 20190308 and data_atual < 20191104:
            return 1655
        elif data_atual >= 20190308 and data_atual < 20200303:
            return 1755
        elif data_atual >= 20200303 and data_atual < 20201103:
            return 1655
        elif data_atual >= 20201103 and data_atual < 20210312:
            return 1755
        elif data_atual >= 20210312 and data_atual < 20211108:
            return 1655
        elif data_atual >= 20211108 and data_atual < 20220314:
            return 1755
        elif data_atual >= 20220314 and data_atual < 20221107:
            return 1655
        elif data_atual >= 20221107 and data_atual < 20230313:
            return 1755
        else:
            return 1655        

    def adiciona_na_estrutura_hora_de_abertura_e_fechamento_primeiro_candle(self, **kwargs) -> None:
        """Sua funcionalida é retornar o horário de fechamento do primeiro
        candle de todos os tempos gráficos, menos do gráfico diário."""
        horario_atual = kwargs.get('INIT_TIME')
        ticker = kwargs.get('INIT_TICKER')
        estrutura_de_dados = kwargs.get('INIT_DADOS')

        for tempo_grafico in TEMPOS_GRAFICOS_INTERESSE:
            if tempo_grafico == 'DIARIO':
                continue
            
            tempo_grafico_int = int(tempo_grafico.split('_')[0])  
            todos_horarios_do_grafico = self.retorna_os_horarios_de_fechamento_de_todos_os_candles_do_dia(
                tempo_grafico_int,
            )

            for index, horario in enumerate(todos_horarios_do_grafico):
                if horario == horario_atual:
                    try:
                        estrutura_de_dados[ticker][tempo_grafico]['horario_de_fechamento'] = \
                            int(todos_horarios_do_grafico[index + 1])
                        estrutura_de_dados[ticker][tempo_grafico]['horario_de_abertura'] = \
                            int(todos_horarios_do_grafico[index])
                    except IndexError:
                        estrutura_de_dados[ticker][tempo_grafico]['horario_de_fechamento'] = \
                            int(todos_horarios_do_grafico[index])
                        estrutura_de_dados[ticker][tempo_grafico]['horario_de_abertura'] = \
                            int(todos_horarios_do_grafico[index - 1])
                    break
                elif horario > horario_atual:
                    estrutura_de_dados[ticker][tempo_grafico]['horario_de_fechamento'] = \
                        int(todos_horarios_do_grafico[index])
                    estrutura_de_dados[ticker][tempo_grafico]['horario_de_abertura'] = \
                        int(todos_horarios_do_grafico[index - 1])
                    break

    def eh_tempo_grafico_de_hora(self, tempo_int):
        """Análisa se o tempo gráfico é de minutos(1, 5, 10, 20, 30minutos) 
        ou se é tempo gráfico de horarios 60(1Hora), 120(2Hora) ou 
        240minutos(4Hora)."""
        if tempo_int == 60:
            return True
        return False

    def retorna_os_horarios_de_fechamento_de_todos_os_candles_do_dia(self, tempo_grafico):
        """."""
        minuto_abertura = str(HORARIO_DE_INTERESSE_DE_ABERTURA)[2:]

        if self.eh_tempo_grafico_de_hora(tempo_grafico):
            return [
                int(f'{hora}{minuto_abertura}')
                for hora in range(HORA_DE_ABERTURA_DO_MERCADO, HORA_DE_FECHAMENTO_DO_MERCADO)
            ]
        return [
            int(f'{hora}{minuto}')
            if len(str(minuto)) == 2 else int(f'{hora}0{minuto}') 
            for hora in range(HORA_DE_ABERTURA_DO_MERCADO, HORA_DE_FECHAMENTO_DO_MERCADO)
            for minuto in range(0, 60, tempo_grafico)
        ]

    def ja_eh_aftermarket(self, hora_atual, horario_de_fechamento):
        """Observação1: Acrescentamos 20 minutos depois do fechamento 
        do mercado para pegar os negócios que ficam em aberto e a b3 
        precisa fecha-los."""
        horario_de_fechamento_de_verao = 1655
        
        if self.eh_horario_de_verao(horario_de_fechamento):
            if hora_atual > 1715: # Observação1
                return True
        else:
            if hora_atual > 1815: # Observação1
                return True
        return False

    def eh_horario_de_verao(self, horario_de_fechamento):
        """."""
        if horario_de_fechamento == 1655:
            return True
        return False

    def gerencia_a_montagem_do_candle(self, **kwargs) -> None:
        """."""
        _horario_atual = kwargs.get('GERENCIADOR_HORARIO')
        horario_fechamendo_do_dia = kwargs.get('GERENCIADO_HORA_DO_FECHAMENTO_DO_MERCADO')
        estrutura_de_dados = kwargs.get('GERENCIADOR_DADOS_ATIVOS')
        _ticker = kwargs.get('GERENCIADOR_TICKER')
        tempo_grafico = kwargs.get('GERENCIADOR_TEMPO_GRAFICO')
        _data = kwargs.get('GERENCIADOR_DATA_ATUAL')
        _precos = kwargs.get('GERENCIADOR_PRECOS')
        _vol = kwargs.get('GERENCIADOR_VOLUME')
        _trades = kwargs.get('GERENCIADOR_TRADES')
        _qty = kwargs.get('GERENCIADOR_QTY') 

        if _horario_atual >= horario_fechamendo_do_dia:
            ...
            hora = int(str(horario_atual)[:2])
            novo_horario_fechamento = int(f'{hora + 5}00')

            estrutura_de_dados[_ticker][tempo_grafico]['horario_de_fechamento'] = novo_horario_fechamento
            estrutura_de_dados[_ticker][tempo_grafico]['<prices>'] += _precos
            estrutura_de_dados[_ticker][tempo_grafico]['<vol>'].apppend(_vol)
            estrutura_de_dados[_ticker][tempo_grafico]['<trades>'].append(_trades)
            estrutura_de_dados[_ticker][tempo_grafico]['<qty>'].append(_qty)
            return
        
        if estrutura_de_dados[_ticker][tempo_grafico]['<prices>']:
            horario_de_abertura_do_candle = str(estrutura_de_dados[_ticker][tempo_grafico]['horario_de_abertura'])
            horario_de_fechamento_do_candle = str(estrutura_de_dados[_ticker][tempo_grafico]['horario_de_fechamento'])

            self.montagem_de_candles(
                MONTAGEM_TEMPO_GRAFICO = tempo_grafico,
                MONTAGEM_DADOS = estrutura_de_dados,
                MONTAGEM_TICKER = _ticker,
                MONTAGEM_DATA = _data,
                MONTAGEM_HORA_DE_ABERTURA_DO_CANDLE = horario_de_abertura_do_candle,
            )

            tempo_grafico_int = int(tempo_grafico.split('_')[0])
            novo_horario_abertura = self.ajusta_troca_horario(int(horario_de_abertura_do_candle) + tempo_grafico_int)
            novo_horario_fechamento = self.ajusta_troca_horario(int(horario_de_fechamento_do_candle) + tempo_grafico_int)

            estrutura_de_dados[_ticker][tempo_grafico]['horario_de_abertura'] = novo_horario_abertura
            estrutura_de_dados[_ticker][tempo_grafico]['horario_de_fechamento'] = novo_horario_fechamento
            estrutura_de_dados[_ticker][tempo_grafico]['<prices>'] += _precos
            estrutura_de_dados[_ticker][tempo_grafico]['<vol>'].append(_vol)
            estrutura_de_dados[_ticker][tempo_grafico]['<trades>'].append(_trades)
            estrutura_de_dados[_ticker][tempo_grafico]['<qty>'].append(_qty) 
            return


    def montagem_de_candles(self, **kwargs) -> None:
        """."""        
        tempo_grafico = kwargs.get('MONTAGEM_TEMPO_GRAFICO')
        estrutra_de_dadps = kwargs.get('MONTAGEM_DADOS')
        _ticker = kwargs.get('MONTAGEM_TICKER') 
        _data = kwargs.get('MONTAGEM_DATA')
        horario_de_abertua_do_candle = kwargs.get('MONTAGEM_HORA_DE_ABERTURA_DO_CANDLE')

        _precos = estrutra_de_dadps[_ticker][tempo_grafico]['<prices>']
        _trades = estrutra_de_dadps[_ticker][tempo_grafico]['<trades>']
        _vol = estrutra_de_dadps[_ticker][tempo_grafico]['<vol>']
        _qty = estrutra_de_dadps[_ticker][tempo_grafico]['<qty>']

        if _ticker not in self.candles_prontos:
            self.candles_prontos[_ticker] = {
                tempo_grafico: {
                    '<ticker>': [_ticker],
                    '<date>': [_data],
                    '<time>': [horario_de_abertua_do_candle],
                    '<trades>': [sum(_trades)],
                    '<close>': [_precos[-1]],
                    '<low>': [min(_precos)],
                    '<high>': [max(_precos)],
                    '<open>': [_precos[1]],
                    '<vol>': [sum(_vol)],
                    '<qty>': [sum(_qty)],
                    '<aft>': ['N'],     
                }
            }
        elif tempo_grafico not in self.candles_prontos[_ticker]:
            self.candles_prontos[_ticker][tempo_grafico] = {
                '<ticker>': [_ticker],
                '<date>': [_data],
                '<time>': [horario_de_abertua_do_candle],
                '<trades>': [sum(_trades)],
                '<close>': [_precos[-1]],
                '<low>': [min(_precos)],
                '<high>': [max(_precos)],
                '<open>': [_precos[1]],
                '<vol>': [sum(_vol)],
                '<qty>': [sum(_qty)],
                '<aft>': ['N'],
            }
        else:
            self.candles_prontos[_ticker][tempo_grafico]['<ticker>'].append(_ticker)
            self.candles_prontos[_ticker][tempo_grafico]['<date>'].append(_data)
            self.candles_prontos[_ticker][tempo_grafico]['<time>'].append(horario_de_abertua_do_candle)
            self.candles_prontos[_ticker][tempo_grafico]['<trades>'].append(sum(_trades))
            self.candles_prontos[_ticker][tempo_grafico]['<close>'].append(_precos[-1])
            self.candles_prontos[_ticker][tempo_grafico]['<low>'].append(min(_precos))
            self.candles_prontos[_ticker][tempo_grafico]['<high>'].append(max(_precos))
            self.candles_prontos[_ticker][tempo_grafico]['<open>'].append(_precos[1])
            self.candles_prontos[_ticker][tempo_grafico]['<vol>'].append(sum(_vol))
            self.candles_prontos[_ticker][tempo_grafico]['<qty>'].append(sum(_qty))
            self.candles_prontos[_ticker][tempo_grafico]['<aft>'].append('N') 

        estrutra_de_dadps[_ticker][tempo_grafico]['<prices>']  = []
        estrutra_de_dadps[_ticker][tempo_grafico]['<trades>'] = []
        estrutra_de_dadps[_ticker][tempo_grafico]['<vol>'] = []
        estrutra_de_dadps[_ticker][tempo_grafico]['<qty>'] = []


    def ajusta_troca_horario(self, novo_horario) -> int:
        """Ele vai retornar sempre que necessário o ajuste do horário. Por exemplo,
        se o horario atual for 1056 e o tempo gráfico atual é de 5minutos, se for enviado
        para a função o novo horario sendo 1056 + 5min = 1061 a função vai retornar 1101.
        """
        minuto_atual = int(str(novo_horario)[2:])
        if minuto_atual >= 60:
            novo_horario = int(str(novo_horario)[:2]) + 1
            novo_minito = minuto_atual - 60
            return int(f'{novo_horario}0{novo_minito}' if len(str(novo_minito)) == 1 else f'{novo_horario}{novo_minito}')
        
        return novo_horario 

    def montagem_do_ultimo_candles(self, **kwargs) -> None:
        """Como precisamos ampliar o horário do fechamento do mercado, 
        essa função será necessária para montar o ultimo candle do dia."""
        estrutura_de_dados = kwargs.get('MONTAGEM_DADOS')
        _data = kwargs.get('MONTAGEM_DATA')

        for ticker, _ in estrutura_de_dados.items():
            for tempo_grafico in TEMPOS_GRAFICOS_INTERESSE:
                if tempo_grafico == 'DIARIO':
                    self.montagem_de_candles(
                        MONTAGEM_TEMPO_GRAFICO = tempo_grafico,
                        MONTAGEM_DADOS = estrutura_de_dados,
                        MONTAGEM_TICKER = ticker,
                        MONTAGEM_DATA = _data,
                        MONTAGEM_HORA_DE_ABERTURA_DO_CANDLE = 'D',
                    )
                else:
                    if estrutura_de_dados[ticker][tempo_grafico]['<prices>']:
                        horario_de_abertura_do_candle = str(estrutura_de_dados[ticker][tempo_grafico]['horario_de_abertura'])

                        self.montagem_de_candles(
                            MONTAGEM_TEMPO_GRAFICO = tempo_grafico,
                            MONTAGEM_DADOS = estrutura_de_dados,
                            MONTAGEM_TICKER = ticker,
                            MONTAGEM_DATA = _data,
                            MONTAGEM_HORA_DE_ABERTURA_DO_CANDLE = horario_de_abertura_do_candle,
                        )                    

    def salvar_dados_manipulados(self, separador=';') -> None:
        """."""
        for ticker, tempos_graficos in self.candles_prontos.items():
            for tempo_grafico, _ in tempos_graficos.items():
                nome_do_diretorio = tempo_grafico
                print(nome_do_diretorio)
                nome_do_arquivo = f'{ticker}_{tempo_grafico}.csv'

                self.cria_diretorio_se_necessario(nome_do_diretorio)
                path_arquivo = os.path.join(DIRETORIO_PARA_SALVAR_OS_TEMPOS_GRAFICOS, nome_do_diretorio, nome_do_arquivo)

                if os.path.exists(path_arquivo):
                    df_tempo_grafico = pd.read_csv(path_arquivo, encoding='utf-8', sep=separador)
                    df_novos_dados = pd.DataFrame(self.candles_prontos[ticker][tempo_grafico])
                    df_concatenado = pd.concat([df_tempo_grafico, df_novos_dados])
                    df_concatenado.to_csv(path_arquivo, sep=separador, index=False)
                    continue
                
                df_novos_dados = pd.DataFrame(self.candles_prontos[ticker][tempo_grafico])
                df_novos_dados.to_csv(path_arquivo, sep=separador, index=False)


    def cria_diretorio_se_necessario(self, nome_do_diretorio) -> bool:
        """."""
        diretorio_do_tempo_grafico = os.path.join(DIRETORIO_PARA_SALVAR_OS_TEMPOS_GRAFICOS, nome_do_diretorio)
        
        if not os.path.exists(diretorio_do_tempo_grafico):
            os.mkdir(diretorio_do_tempo_grafico)


objeto = CriaTemposGraficos()

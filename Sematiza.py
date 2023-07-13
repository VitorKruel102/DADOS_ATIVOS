import csv
import os
import pandas as pd
from pandas_market_calendars import get_calendar
from time import sleep

"""ATIVOS = [
    'BBSE3',   'ENGI11',  'EGIE3',   'ITUB4',   'ITSA4',   'BRFS3',   'PRIO3',   'LOGG3',   'BEEF3',   'GGBR4',   'OIBR4',   'TELB3',   'CCRO3',
    'RAIL3',   'MRVE3',   'CGAS5',   'LIGT3',   'BPAC11',  'BBAS3',   'PMAM3',   'STBP3',   'UGPA3',   'WHRL3',   'SEER3',   'GOAU4',   'TGMA3',
    'VBBR3',   'YDUQ3',   'ASAI3',   'DXCO3',   'BRKM5',   'GEPA4',   'TOTS3',   'WEGE3',   'BBDC3',   'TUPY3',   'TIMS3',   'TRPL4',   'PCAR3',
    'RENT3',   'IRBR3',   'GGBR3',   'B3SA3',   'GOLL4',   'VIVT3',   'ELET3',   'ELET6',   'EMBR3',   'SUZB3',   'PETR4',   'CMIG4',   'OIBR3',
    'CSNA3',   'POMO4',   'CYRE3',   'VALE3',   'RSID3',   'EMAE4',   'ALPA4',   'AMER3',   'CVCB3',   'VIIA3',   'FLRY3',   'KLBN11',  'NEOE3',
    'QUAL3',   'CPLE6',   'ECOR3',   'AURE3',   'ENEV3',   'USIM3',   'HYPE3',   'CIEL3',   'ENBR3',   'TAEE11',  'SYNE3',   'RADL3',   'MRFG3',
    'UNIP6',   'ALSO3',   'MYPK3',   'SBSP3',   'SANB4',   'INEP4',   'COGN3',   'MELK3',   'EVEN3',   'RDOR3',   'TELB4',   'VALE4',   'CTNM4',
    'EQTL3',   'BRAP4',   'HAPV3',   'ABEV3',   'MGLU3',   'PRIO3',   'CRFB3',   'NTCO3',   'CPFE3',   'CMIG3',   'SAPR11',  'MTSA3',   'LIPR3',
    'VSTE3',   'JBSS3',   'GFSA3',   'KLBN4',   'USIM5',   'CSAN3',   'BEES3',   'AZUL4',   'TCSA3',   'MULT3',   'DASA3',   'LREN3',
]"""
ATIVOS = [
'MRVE3',
'GOAU4',
'COGN3',
'QUAL3',
'SUZB3',
'SANB11',
'B3SA3',
'MRFG3',
'KLBN11',
'UGPA3',
'CSNA3',
'PETR3',
'TIMS3',
'EZTC3',
'ELET6',
'HYPE3',
'AMER3',
'CCRO3',
'BRDT3',
'SBSP3',
'EQTL3',
'BPAN4',
'ALSO3',
'ASAI3',
'LWSA3',
'CMIN3',
'ABEV3',
'TAEE11',
'VIVT3',
'ITSA4',
'BRKM5',
'EGIE3',
'ENGI11',
'PCAR3',
'USIM5',
'ECOR3',
'ELET3',
'IRBR3',
'DXCO3',
'MGLU3',
'LREN3',
'JHSF3',
'BBDC3',
'VALE3',
'FLRY3',
'IGTI11',
'BEEF3',
'PETZ3',
'BRAP4',
'CPLE6',
'VBBR3',
'NTCO3',
'CPFE3',
'JBSS3',
'CIEL3',
'BRFS3',
'RADL3',
'AZUL4',
'RENT3',
'SOMA3',
'VIIA3',
'RAIL3',
'PRIO3',
'ENBR3',
'WEGE3',
'BPAC11',
'PETR4',
'ITUB4',
'MULT3',
'BBDC4',
'ALPA4',
'POSI3',
'CVCB3',
'CRFB3',
'CMIG4',
'BBSE3',
'CYRE3',
'GOLL4',
'BBAS3',
'SLCE3',
'ENEV3',
'TOTS3',
'RDOR3',
'GGBR4',
'EMBR3',
'CASH3',
'HAPV3',
'CSAN3',
'RRRP3',
'YDUQ3',
]

FERIADOS_B3 = [
    '2018-01-01',  '2018-01-25',  '2018-02-12',  '2018-02-13',
    '2018-03-30',  '2018-05-01',  '2018-05-31',  '2018-07-09',
    '2018-09-07',  '2018-10-12',  '2018-11-02',  '2018-11-15',
    '2018-11-20'   '2018-12-24',  '2018-12-25',  '2018-12-31',  
    '2019-01-01',  '2019-01-25',  '2019-03-04',  '2019-03-05',  
    '2019-04-19',  '2019-05-01',  '2019-06-20',  '2019-07-09',  
    '2019-11-15',  '2019-11-20',  '2019-12-24',  '2019-12-25',  
    '2019-12-31',  '2020-01-01',  '2020-02-24',  '2020-02-25',  
    '2020-04-10',  '2020-04-21',  '2020-05-01',  '2020-06-11',  
    '2020-09-07',  '2020-10-12',  '2020-11-02',  '2020-12-24',  
    '2020-12-25',  '2020-12-31',  '2021-01-01',  '2021-01-25',  
    '2021-02-15',  '2021-02-16',  '2021-04-02',  '2021-04-21',  
    '2021-06-03',  '2021-09-07',  '2021-10-12',  '2021-11-02',  
    '2021-11-15',  '2021-12-24',  '2021-12-31',  '2022-01-01',  
    '2022-01-25',  '2022-02-28',  '2022-03-01',  '2022-03-02',  
    '2022-04-15',  '2022-04-21',  '2022-06-16',  '2022-09-07',  
    '2022-10-12',  '2022-11-02',  '2022-11-15',  '2022-12-25',  
    '2022-12-30',  '2022-12-31',  '2023-02-20',  '2023-02-21',  
    '2023-04-07',  '2023-04-21',  '2023-05-01',  '2023-06-06',  
    '2023-09-07',  '2023-10-12',  '2023-11-15',  '2023-12-25',  
    '2023-12-29',
]

DIAS_DE_ABERTURAS_A_TARDE = [
    '2018-02-14', '2019-03-06', '2020-02-26', '2020-02-26', 
    '2021-02-17', '2022-03-03', '2023-02-22',
]

TEMPOS_INTRADAY = [1,  5, 10, 15, 20, 30,60] #,1,  5, 10, 15, 20, 30, 60
TEMPOS_DIARIOS = ['DIARIO'] # SAF = Sem After 'DIARIO'
HORA_ABERTURA = 1030 # HHMM

DADOS = {
    acoes: {
        tempo: {
            'DADOS_INTERESSE': [0, 0, 0, 0], #Candles, Buracos, Max_Buracos, Seq_Atual_Buracos
            'CABECARIO': ['Data', 'Hora', 'O', 'H', 'L', 'C'],
            'COTACOES': [],
            'TEMPO_ULTIMO_CANDLE': HORA_ABERTURA
        }
        for tempo in (TEMPOS_DIARIOS + TEMPOS_INTRADAY)
    }
    for acoes in ATIVOS
}

DIRETORIO_TEMPOS_GRAFICOS = r'D:\DADOS_FINANCEIROS\Database_Sematiza_TemposGraficos'
PATH_SEMATIZA = r'D:\DADOS_FINANCEIROS\Database_Sematiza'


def close_update(new_close):
    """Adjusts the closing of the candle at the turn of the hour. (EX:. 1061 --> 1101)"""
    minuts = int(str(new_close)[2:4])
    if minuts >= 60:
        hours = str(int(str(new_close)[:2]) + 1)
        minuto = str(int(str(new_close)[2:]) - 60)
        new_close = f'{hours}0{minuto}' if len(minuto) == 1 else hours + minuto

    del minuts

    return int(new_close) 


def close_market(date):
    """Return the market close in relation to the informed day."""
    if date < 20180310:
        return 1755
    elif date >= 20180310 and date < 20181105:
        return 1655
    elif date >= 20181105 and date < 20190308:
        return 1755 
    elif date >= 20190308 and date < 20191104:
        return 1655
    elif date >= 20190308 and date < 20200303:
        return 1755
    elif date >= 20200303 and date < 20201103:
        return 1655
    elif date >= 20201103 and date < 20210312:
        return 1755
    elif date >= 20210312 and date < 20211108:
        return 1655
    elif date >= 20211108 and date < 20220314:
        return 1755
    elif date >= 20220314 and date < 20221107:
        return 1655
    elif date >= 20221107 and date < 20230313:
        return 1755
    else:
        return 1655


def dias_uteis_b3(start_date, end_date, tipo_calendario='B3') -> list():
    """Retorna os dias úteis do mercado brasileiro."""
    b3_calendar = get_calendar(tipo_calendario)
    # Obter o calendário de negociações
    schedule = b3_calendar.schedule(start_date=start_date, end_date=end_date)
    # Exibir o calendário de negociações
    return list(schedule.index.astype(str)) 


def sematiza_diario():
    """."""
    from datetime import datetime
    
    HORA_ABERTURA = 1030
    for tempo in TEMPOS_DIARIOS:
        nome_pasta = tempo
        tempo_grafico_interesse = tempo

        for ativo in ATIVOS:
            nome_arquivo = os.path.join(DIRETORIO_TEMPOS_GRAFICOS, nome_pasta, f'{ativo}_{tempo_grafico_interesse}.csv')
            os.chdir(DIRETORIO_TEMPOS_GRAFICOS)
            df_diario = pd.read_csv(nome_arquivo, sep=';')

            for ticker in df_diario['<ticker>'].unique():            
                df_diario_ticker = df_diario[df_diario['<ticker>'] == ticker] 
                primeiro_dia_interesse = datetime.strptime(str(df_diario_ticker['<date>'].iloc[0]), "%Y%m%d").strftime("%Y-%m-%d")
                ultimo_dia_interesse = datetime.strptime(str(df_diario_ticker['<date>'].iloc[-1]), "%Y%m%d").strftime("%Y-%m-%d")
                
                dias_uteis = dias_uteis_b3(
                    primeiro_dia_interesse, 
                    ultimo_dia_interesse
                )
                
                for dia in dias_uteis:
                    dia_int = int(dia.replace('-', ''))
                    df_diario_filtrado = df_diario_ticker[df_diario_ticker['<date>'] == dia_int]

                    if dia in FERIADOS_B3:
                        continue

                    if df_diario_filtrado['<date>'].shape[0] == 0:
                        if len(DADOS[ticker][tempo]['COTACOES']) == 0:
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1
                        else:
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1         

                            ultima_data_registrada = str(dia_int)
                            ultimo_time_registrado = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                            ultimo_fechamento_registrado = DADOS[ticker][tempo]['COTACOES'][-1][5]
                            
                            DADOS[ticker][tempo]['COTACOES'].append(
                                [
                                    ultima_data_registrada, 
                                    ultimo_time_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado,
                                ]
                            )
                        continue
                    
                    abertura = df_diario_filtrado['<open>'].iloc[0]
                    fechamento = df_diario_filtrado['<close>'].iloc[0]
                    maxima = df_diario_filtrado['<high>'].iloc[0]
                    minima = df_diario_filtrado['<low>'].iloc[0]

                    DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                    DADOS[ticker][tempo]['COTACOES'].append(
                        [
                            str(dia_int), 
                            '1200', 
                            abertura, 
                            maxima, 
                            minima, 
                            fechamento
                        ]
                            
                    )
                    
                    maior_sequencia_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][2]
                    sequencia_atual_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                    if maior_sequencia_buraco < sequencia_atual_buraco:
                        DADOS[ticker][tempo]['DADOS_INTERESSE'][2] = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                    DADOS[ticker][tempo]['DADOS_INTERESSE'][3] = 0 

                        
        for ticker in ATIVOS:
            if not os.path.exists(os.path.join(PATH_SEMATIZA, nome_pasta)):
                os.mkdir(os.path.join(PATH_SEMATIZA, nome_pasta))

            with open(os.path.join(PATH_SEMATIZA, nome_pasta, f'{ticker}_1440.csv'), 'w', newline='') as csvfile:
                spanrows = csv.writer(csvfile, delimiter=',')
                spanrows.writerow(['#Candles:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][0])])
                spanrows.writerow(['#Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][1])])
                spanrows.writerow(['#Max.Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][2])])
                csvfile.close()
            with open(os.path.join(PATH_SEMATIZA, nome_pasta, f'{ticker}_1440.csv'), 'a', newline='') as csvfile:
                spanrows = csv.writer(csvfile, delimiter='\t')
                spanrows.writerow(DADOS[ticker][tempo]['CABECARIO'])
                for dados in DADOS[ticker][tempo]['COTACOES']:
                    spanrows.writerow(dados)


            print('FIM')


def intraday():
    """."""
    from datetime import datetime
    
    HORA_ABERTURA = 1030
    for tempo in TEMPOS_INTRADAY:
        if len(str(tempo)) == 1:
            nome_pasta = f'0{tempo}_MINUTO'
            tempo_grafico_interesse = f'0{tempo}_MINUTO'
        else:
            nome_pasta = f'{tempo}_MINUTO'
            tempo_grafico_interesse = f'{tempo}_MINUTO'

        for ticker in ATIVOS:
            print(nome_pasta)
            os.chdir(DIRETORIO_TEMPOS_GRAFICOS)
            nome_arquivo = os.path.join(DIRETORIO_TEMPOS_GRAFICOS, nome_pasta, f'{ticker}_{tempo_grafico_interesse}.csv')
            df_minuto = pd.read_csv(nome_arquivo, sep=';')
            df_minuto['<time>'] = df_minuto['<time>'] // 100

            df_minuto['<date>'] = pd.to_datetime(df_minuto['<date>'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

            primeiro_dia_interesse = df_minuto['<date>'].iloc[1]
            ultimo_dia_interesse = df_minuto['<date>'].iloc[-1]
            
            dias_uteis = dias_uteis_b3(
                primeiro_dia_interesse, 
                ultimo_dia_interesse
            )
            
            for dia in dias_uteis:
                df_minuto_filtrado = df_minuto[df_minuto['<date>'] == dia]
                dia_int = int(dia.replace('-', ''))
                
                if dia in DIAS_DE_ABERTURAS_A_TARDE:
                    DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = 1330
                    time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                    df_minuto_filtrado = df_minuto_filtrado[
                        (df_minuto_filtrado['<time>'] >= 1330) & 
                        (df_minuto_filtrado['<time>'] <= (close_market(dia_int) - 25))
                    ]
                else:
                    if close_market(dia_int) == 1755 and dia_int < 20121203:
                        DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = 1130
                        HORA_ABERTURA = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                    else:
                        DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = HORA_ABERTURA

    
                    time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                    df_minuto_filtrado = df_minuto_filtrado[
                        (df_minuto_filtrado['<time>'] >= HORA_ABERTURA) & 
                        (df_minuto_filtrado['<time>'] <= (close_market(dia_int) - 25))
                    ]


                if df_minuto_filtrado['<date>'].shape[0] == 0:
                    if dia in FERIADOS_B3:
                        continue
                    else:
                        time_atual = (close_market(dia_int) - 25)
                        
                        while time_atual >= time_ideal:
                            if time_atual >= time_ideal and len(DADOS[ticker][tempo]['COTACOES']) == 0:
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1
                                DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 
                                
                            elif time_atual >= time_ideal:
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                                DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1         

                                ultima_data_registrada = str(dia_int)
                                ultimo_time_registrado = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                                ultimo_fechamento_registrado = DADOS[ticker][tempo]['COTACOES'][-1][5]
                                
                                DADOS[ticker][tempo]['COTACOES'].append(
                                    [
                                        ultima_data_registrada, 
                                        ultimo_time_registrado, 
                                        ultimo_fechamento_registrado, 
                                        ultimo_fechamento_registrado, 
                                        ultimo_fechamento_registrado, 
                                        ultimo_fechamento_registrado,
                                    ]
                                )
                                DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 
                            
                            time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                        continue
                                
                for _, row in df_minuto_filtrado.iterrows():
                    time_atual = row['<time>']
                    time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                    
                    while time_atual > time_ideal:
                        if time_atual > time_ideal and len(DADOS[ticker][tempo]['COTACOES']) == 0:
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1
                            DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 
                        elif time_atual > time_ideal:
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][1] += 1
                            DADOS[ticker][tempo]['DADOS_INTERESSE'][3] += 1         

                            ultima_data_registrada = str(dia_int)
                            ultimo_time_registrado = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                            ultimo_fechamento_registrado = DADOS[ticker][tempo]['COTACOES'][-1][5]
               
                            DADOS[ticker][tempo]['COTACOES'].append(
                                [
                                    ultima_data_registrada, 
                                    ultimo_time_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado, 
                                    ultimo_fechamento_registrado,
                                ]
                            )
                            DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 
                        
                        time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                
                    abertura = row['<open>']
                    fechamento = row['<close>']
                    maxima = row['<high>']
                    minima = row['<low>']

                    DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                    DADOS[ticker][tempo]['COTACOES'].append(
                        [
                            str(dia_int), 
                            time_atual, 
                            abertura, 
                            maxima, 
                            minima, 
                            fechamento,
                        ]
                            
                    )
                    
                    DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 

                    maior_sequencia_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][2]
                    sequencia_atual_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                    if maior_sequencia_buraco < sequencia_atual_buraco:
                        DADOS[ticker][tempo]['DADOS_INTERESSE'][2] = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                    DADOS[ticker][tempo]['DADOS_INTERESSE'][3] = 0 

                        
        for ticker in ATIVOS:
            if not os.path.exists(os.path.join(PATH_SEMATIZA, nome_pasta)):
                os.mkdir(os.path.join(PATH_SEMATIZA, nome_pasta))

            with open(os.path.join(PATH_SEMATIZA, nome_pasta, f'{ticker}_{tempo}.csv'), 'w', newline='') as csvfile:
                spanrows = csv.writer(csvfile, delimiter=',')
                spanrows.writerow(['#Candles:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][0])])
                spanrows.writerow(['#Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][1])])
                spanrows.writerow(['#Max.Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][2])])
                csvfile.close()
            with open(os.path.join(PATH_SEMATIZA, nome_pasta, f'{ticker}_{tempo}.csv'), 'a', newline='') as csvfile:
                spanrows = csv.writer(csvfile, delimiter='\t')
                spanrows.writerow(DADOS[ticker][tempo]['CABECARIO'])
                for dados in DADOS[ticker][tempo]['COTACOES']:
                    spanrows.writerow(dados)
            print('FIM')


def remove_folders(directory):
    """Delete all folders"""
    for root, _, files in os.walk(directory):
        for file in files:
            os.remove(os.path.join(root, file))


if __name__ == '__main__':
    remove_folders(PATH_SEMATIZA)
    sematiza_diario()
    intraday()
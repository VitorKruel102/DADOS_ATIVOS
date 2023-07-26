import csv
import os
import pandas as pd
from pandas_market_calendars import get_calendar
from time import sleep

DIRETORIO_TEMPOS_GRAFICOS = r'D:\DADOS_FINANCEIROS\Database_Sematiza_TemposGraficos'
DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE = r'D:\DADOS_FINANCEIROS\Database_DadosParaAjuste'
PATH_SEMATIZA = r'D:\DADOS_FINANCEIROS\Database_Sematiza'


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

ATIVOS = []
for _, _, arquivos in os.walk(DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE):
    for arquivo in arquivos:
        nome_ticker = arquivo.split('_')[0]
        ATIVOS.append(nome_ticker)   

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
            df_minuto['<time>'] = df_minuto['<time>']

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
    # sematiza_diario()
    intraday()
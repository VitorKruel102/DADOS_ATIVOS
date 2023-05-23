import csv
import os
import pandas_market_calend
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
ATIVOS = ['ABEV3'] # 'ITUB4',   'ITSA4',   'BRFS3',   'B3SA3',   'LOGG3', 'VALE3', 'PETR3', 'BBSE3', 'PETR4',  
FERIADOS_B3 = [
    '20180101',  '20180125',  '20180212',  '20180213',
    '20180330',  '20180501',  '20180531',  '20180709',
    '20180907',  '20181012',  '20181102',  '20181115',
    '20181224',  '20181225',  '20181231',  '20190101', 
    '20190125',  '20190304',  '20190305',  '20190419',
    '20190501',  '20190620',  '20190709',  '20191115',
    '20191120',  '20191224',  '20191225',  '20191231',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
]

TEMPOS_INTRADAY = [1,  5, 10, 15, 20, 30] #,1,  5, 10, 15, 20, 30
TEMPOS_DIARIOS = ['DIARIO'] # SAF = Sem After
HORA_ABERTURA = 1030 # HHMM

DADOS = {
    acoes: {
        tempo: {
            'DADOS_INTERESSE': [0, 0, 0, 0], #Candles, Buracos, Max_Buracos, Seq_Atual_Buracos
            'CABECARIO': ['Data', 'Hora', 'O', 'H', 'L', 'C', 'TEM_BURACO?'],
            'COTACOES': [],
            'TEMPO_ULTIMO_CANDLE': HORA_ABERTURA
        }
        for tempo in (TEMPOS_DIARIOS + TEMPOS_INTRADAY)
    }
    for acoes in ATIVOS
}

DIRETORIO_TEMPOS_GRAFICOS = r'D:\DADOS_FINANCEIROS\Database_TemposGraficos'
PATH_SEMATIZA = r'C:\Users\diaxt\Desktop\SEMATIZA\DADOS_SEMATIZA'


def close_update(new_close):
    """Adjusts the closing of the candle at the turn of the hour. (EX:. 1061 --> 1101)"""
    minuts = int(str(new_close)[2:4])
    if minuts >= 60:
        hours = str(int(str(new_close)[:2]) + 1)
        minuto = '0'+ str(new_close)[3]
        new_close = (hours + minuto).zfill(4)

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


def horario_abertura(hh) -> int():
    """Retorna a hora de abertura em HHMM."""
    if str(hh)[:2] == 10:
        return 1030
    return 1330


def sematiza_diario():
    """."""
    for tempo in TEMPOS_DIARIOS:
        nome_arquivo = f'{tempo}.csv'

        os.chdir(DIRETORIO_TEMPOS_GRAFICOS)
        with open(nome_arquivo, newline='') as csvfile:
            arquivo = csv.reader(csvfile, delimiter=';')
            dia_atual = None

            for linha in arquivo:
            
                ticker = linha[0]
                
                if not ticker in ATIVOS:
                    continue

                data = int(linha[1])
            
                if not dia_atual:
                    dia_atual = data

                if dia_atual != data:
                    dia_atual = data

                

                abertura = float(linha[7])
                fechamento = float(linha[4])
                maxima = float(linha[6])
                minima = float(linha[5])

                DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                DADOS[ticker][tempo]['COTACOES'].append(
                    [
                        data, 
                        1200, 
                        abertura, 
                        maxima, 
                        minima, 
                        fechamento,
                    ]
                        
                )
            
            
            for ticker in ATIVOS:
                print(DADOS[ticker][tempo]['DADOS_INTERESSE'][2])
                
                os.chdir(PATH_SEMATIZA)
                with open(os.path.join(PATH_SEMATIZA, f'{ticker}_1440.csv'), 'w', newline='') as csvfile:
                    spanrows = csv.writer(csvfile, delimiter=',')
                    spanrows.writerow(['#Candles:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][0])])
                    spanrows.writerow(['#Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][1])])
                    spanrows.writerow(['#Max.Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][2])])
                    spanrows.writerow(DADOS[ticker][tempo]['CABECARIO'])
                    csvfile.close()
                print('FIM')
                with open(os.path.join(PATH_SEMATIZA, f'{ticker}_1440.csv'), 'a', newline='') as csvfile:
                    spanrows = csv.writer(csvfile, delimiter='\t')
                    for dados in DADOS[ticker][tempo]['COTACOES']:
                        spanrows.writerow(dados)

                print('FIM')


def intraday():
    """."""
    HORA_ABERTURA = 1030
    for tempo in TEMPOS_INTRADAY:
        if len(str(tempo)) == 1:
            nome_arquivo = f'0{tempo}_MINUTO.csv'
        else:
            nome_arquivo = f'{tempo}_MINUTO.csv'

        os.chdir(DIRETORIO_TEMPOS_GRAFICOS)
        with open(nome_arquivo, newline='') as csvfile:
            arquivo = csv.reader(csvfile, delimiter=';')
            dia_atual = None

            for linha in arquivo:
            
                ticker = linha[0]
                if not ticker in ATIVOS:
                    continue

                data = int(linha[1])
                time_atual = int(linha[2][:-2]) 
                
                if time_atual < 1030 or time_atual > (close_market(data) - 25):
                    DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = HORA_ABERTURA
                    continue

                if not dia_atual:
                    dia_atual = data
                    HORA_ABERTURA = horario_abertura(time_atual)
                    DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = HORA_ABERTURA

                if dia_atual != data:
                    dia_atual = data
                    HORA_ABERTURA = horario_abertura(time_atual)
                    DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = HORA_ABERTURA

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

                        ultima_data_registrada = DADOS[ticker][tempo]['COTACOES'][-1][0]
                        ultimo_time_registrado = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
                        ultima_abertura_registrada = DADOS[ticker][tempo]['COTACOES'][-1][2]
                        ultima_maxima_registra = DADOS[ticker][tempo]['COTACOES'][-1][3]
                        ultima_minima_registrada = DADOS[ticker][tempo]['COTACOES'][-1][4]
                        ultimo_fechamento_registrado = DADOS[ticker][tempo]['COTACOES'][-1][5]
                        print(dia_atual)
                        DADOS[ticker][tempo]['COTACOES'].append(
                            [
                                ultima_data_registrada, 
                                ultimo_time_registrado, 
                                ultima_abertura_registrada, 
                                ultima_maxima_registra, 
                                ultima_minima_registrada, 
                                ultimo_fechamento_registrado,
                                1
                            ]
                        )
                        DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 
                    
                    time_ideal = DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE']
            
                abertura = float(linha[7])
                fechamento = float(linha[4])
                maxima = float(linha[6])
                minima = float(linha[5])

                DADOS[ticker][tempo]['DADOS_INTERESSE'][0] += 1
                DADOS[ticker][tempo]['COTACOES'].append(
                    [
                        data, 
                        time_atual, 
                        abertura, 
                        maxima, 
                        minima, 
                        fechamento,
                        0
                    ]
                        
                )
                DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] = close_update(DADOS[ticker][tempo]['TEMPO_ULTIMO_CANDLE'] + tempo) 

                maior_sequencia_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][2]
                sequencia_atual_buraco = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                if maior_sequencia_buraco < sequencia_atual_buraco:
                    DADOS[ticker][tempo]['DADOS_INTERESSE'][2] = DADOS[ticker][tempo]['DADOS_INTERESSE'][3]

                DADOS[ticker][tempo]['DADOS_INTERESSE'][3] = 0            
            
            for ticker in ATIVOS: 
                with open(os.path.join(PATH_SEMATIZA, f'{ticker}_{tempo}.csv'), 'w', newline='') as csvfile:
                    spanrows = csv.writer(csvfile, delimiter=',')
                    spanrows.writerow(['#Candles:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][0])])
                    spanrows.writerow(['#Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][1])])
                    spanrows.writerow(['#Max.Buracos:' + str(DADOS[ticker][tempo]['DADOS_INTERESSE'][2])])
                    spanrows.writerow(DADOS[ticker][tempo]['CABECARIO'])
                    csvfile.close()
                with open(os.path.join(PATH_SEMATIZA, f'{ticker}_{tempo}.csv'), 'a', newline='') as csvfile:
                    spanrows = csv.writer(csvfile, delimiter='\t')
                    for dados in DADOS[ticker][tempo]['COTACOES']:
                        spanrows.writerow(dados)

                print('FIM')


if __name__ == '__main__':
    #sematiza_diario()
    intraday()
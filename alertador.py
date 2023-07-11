import os 
import pandas as pd
from pandas_market_calendars import get_calendar
from datetime import datetime



DIRETORIO_INTRADAY = r'D:\DADOS_FINANCEIROS\Database_PrincipaisAcoes'
DIRETORIO_DIARIO_AJUSTADO = r'D:\DADOS_FINANCEIROS\Database_ProfitDiario_SPLIT'
DIRETORIO_DIARIO_SEM_AJUSTE = r'D:\DADOS_FINANCEIROS\Database_ProfitDiario'
PATH_ALERTADOR = r'D:\DADOS_FINANCEIROS\Database_Alertador'


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


REGISTROS = {
    'Data': [],
    'Ticker' : [],
    'Abertura_Diario': [],
    'Abertura_Minuto': [],
    'Diferenca_Abertura': [],
    'Fechamento_Diario': [],
    'Fechamento_Minuto': [],
    'Diferenca_Fechamento': [],
    'Moda': [],
    'Problema': [],
}

DADOS_ESTATISTICOS = {
    'Ticker': [],
    'Periodo_Inicial': [],
    'Periodo_Final': [],
    'Total_dias': [],
    'Dias_com_Erros': [],
    'Percentual_Integridade': [],
}

ERROS = {
    'Ticker': [],
    'Motivo': [],
    'Data': [],
}

def main():
    global REGISTROS, ERROS, DADOS_ESTATISTICOS
    for path, dir, files in os.walk(DIRETORIO_INTRADAY):
        for file in files:
            ativo = file.split('_')[0]
            print(ativo)

            df_intraday = pd.read_csv(os.path.join(DIRETORIO_INTRADAY, file), sep=';')
            periodo_dados = len(df_intraday['<date>'].iloc[1:].unique())    

            start_date = datetime.strptime(str(df_intraday['<date>'].iloc[0]), "%Y%m%d").strftime("%Y-%m-%d")
            end_date = datetime.strptime(str(df_intraday['<date>'].iloc[-1]), "%Y%m%d").strftime("%Y-%m-%d")
            dias_uteis = dias_uteis_b3(start_date, end_date, tipo_calendario='B3')
            
            for dia in dias_uteis:
                dia_int = int(dia.replace('-', ''))
                df_intraday_unique = df_intraday[df_intraday['<date>'] == dia_int]

                if dia in FERIADOS_B3:
                    continue

                if dia_int < 20211013:                
                    df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_AJUSTADO, f'{ativo}_DIARIO_NAS.csv'), sep=';')
                    is_adjusted = True
                    df_diario['Data'] = df_diario['Data'].astype(int)

                elif dia_int == 20211013:
                    df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_SEM_AJUSTE, f'{ativo}_DIARIO.csv'), sep=';') 
                    is_adjusted = False
                    
                    df = pd.DataFrame(REGISTROS)
                    moda_diferenca_abertura = df['Diferenca_Abertura'].mode()[0]
                    print(moda_diferenca_abertura)
                    print(df)
                    df = df[df['Diferenca_Abertura'] != moda_diferenca_abertura]
                    df['Moda'] = moda_diferenca_abertura
                    REGISTROS = df.to_dict('list')
                    if len(REGISTROS['Data']) == 0:
                        REGISTROS = {
                            item: []
                            for item in df.to_dict()
                        }

                    df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
                    df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
                    df_diario['Data'] = df_diario['Data'].astype(int)
                else:
                    df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_SEM_AJUSTE, f'{ativo}_DIARIO.csv'), sep=';') 
                    is_adjusted = False
                    
                    df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
                    df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
                    df_diario['Data'] = df_diario['Data'].astype(int)
                    ...


                abertura_diario = df_diario['Abertura']
                fechamento_diario = df_diario['Fechamento']


                if df_intraday_unique.shape[0] == 0:
                    REGISTROS['Data'].append(dia_int)
                    REGISTROS['Ticker'].append(ativo)
                    REGISTROS['Abertura_Diario'].append(abertura_diario)
                    REGISTROS['Abertura_Minuto'].append(0)
                    REGISTROS['Diferenca_Abertura'].append(0)
                    REGISTROS['Fechamento_Diario'].append(fechamento_diario)
                    REGISTROS['Fechamento_Minuto'].append(0)
                    REGISTROS['Diferenca_Fechamento'].append(0)
                    
                    if is_adjusted:
                        REGISTROS['Problema'].append('Ajustado')
                        REGISTROS['Moda'].append(0)
                    else:
                        REGISTROS['Problema'].append('Nao_Ajustado')
                        REGISTROS['Moda'].append(1)                

                df_diario_filtrado = df_diario[df_diario['Data'] == dia_int] 
                
                try:
                    abertura_diario = round(df_diario_filtrado['Abertura'].iloc()[0], 2)
                    fechamento_diario = round(df_diario_filtrado['Fechamento'].iloc()[0], 2)
                    abertura_intraday = round(df_intraday_unique['<open>'].iloc()[0], 2)
                    fechamento_intraday = round(df_intraday_unique['<close>'].iloc()[-1], 2)
                except IndexError as err:
                    ERROS['Ticker'].append(ativo)
                    ERROS['Motivo'].append(err)
                    ERROS['Data'].append(dia_int)
                    continue
                    
                
                
                if (abertura_intraday == abertura_diario) and (fechamento_intraday == fechamento_diario):
                    continue
                else:
                    REGISTROS['Data'].append(dia_int)
                    REGISTROS['Ticker'].append(ativo)
                    REGISTROS['Abertura_Diario'].append(abertura_diario)
                    REGISTROS['Abertura_Minuto'].append(abertura_intraday)
                    REGISTROS['Diferenca_Abertura'].append(round(abertura_diario / abertura_intraday, 3))
                    REGISTROS['Fechamento_Diario'].append(fechamento_diario)
                    REGISTROS['Fechamento_Minuto'].append(fechamento_intraday)
                    REGISTROS['Diferenca_Fechamento'].append(round(fechamento_diario / fechamento_intraday, 3))
                    
                    if is_adjusted:
                        REGISTROS['Problema'].append('Ajustado')
                        REGISTROS['Moda'].append(0)
                    else:
                        REGISTROS['Problema'].append('Nao_Ajustado')
                        REGISTROS['Moda'].append(1)

            
            df = pd.DataFrame(REGISTROS)
            periodo_com_problemas = df.shape[0]
            if periodo_com_problemas == 0: 
                continue
            
            df_intraday['<date>'] = df_intraday['<date>'].astype(str)
            df_intraday['<date>'] = pd.to_datetime(df_intraday['<date>'])

            DADOS_ESTATISTICOS['Ticker'].append(ativo)
            DADOS_ESTATISTICOS['Periodo_Inicial'].append(df_intraday['<date>'].min())
            DADOS_ESTATISTICOS['Periodo_Final'].append(df_intraday['<date>'].max())
            DADOS_ESTATISTICOS['Total_dias'].append(df_intraday['<date>'].max() - df_intraday['<date>'].min())
            DADOS_ESTATISTICOS['Dias_com_Erros'].append(periodo_com_problemas)
            DADOS_ESTATISTICOS['Percentual_Integridade'].append(round(((periodo_dados - periodo_com_problemas) / periodo_dados) * 100, 2))

            
            df.to_csv(os.path.join(PATH_ALERTADOR, f'{ativo}_estudo.csv'), sep=';', index=False)

            for colunas in REGISTROS.keys():
                REGISTROS[colunas].clear()

    df = pd.DataFrame(DADOS_ESTATISTICOS)                
    df.to_csv(os.path.join(PATH_ALERTADOR, f'Estatistica_Tickes.csv'), sep=';', index=False)

    for colunas in DADOS_ESTATISTICOS.keys():
        DADOS_ESTATISTICOS[colunas].clear()

    print(ERROS)
    
def comparacao_volume_abertura():
    """."""
    ARQUIVO_B3 = r'D:\DADOS_FINANCEIROS\Database_PrincipaisAcoes\ABEV3_BMF_I.csv'
    DADOS = {
        '<data>': [],
        '<ticker>': [],
        '<volume_abertura>': [],
        '<volume_segundo>': [],
        '<comparacao>': [],
    }

    df_arquivo = pd.read_csv(ARQUIVO_B3, sep=';')
    print(df_arquivo)
    for data in df_arquivo['<date>'].unique():
        df_filtro = df_arquivo[df_arquivo['<date>'] == data]

        volume_abertura = df_filtro['<vol>'].iloc[0]
        volume_segundo = df_filtro['<vol>'].iloc[1]
        comparacao = volume_abertura / volume_segundo

        DADOS['<data>'].append(data)
        DADOS['<ticker>'].append(df_filtro['<ticker>'].iloc[0])
        DADOS['<volume_abertura>'].append(volume_abertura)
        DADOS['<volume_segundo>'].append(volume_segundo)
        DADOS['<comparacao>'].append(comparacao)


    df_comparacao = pd.DataFrame(DADOS)
    print(df_comparacao)
    df_comparacao.to_csv(os.path.join(PATH_ALERTADOR,  'comparacao.csv'), sep=';', index=False)


def dias_uteis_b3(start_date, end_date, tipo_calendario='B3') -> list():
    """Retorna os dias úteis do mercado brasileiro."""
    b3_calendar = get_calendar(tipo_calendario)
    # Obter o calendário de negociações
    schedule = b3_calendar.schedule(start_date=start_date, end_date=end_date)
    # Exibir o calendário de negociações
    return list(schedule.index.astype(str)) 


def main_CO41():
    global REGISTROS, ERROS, DADOS_ESTATISTICOS

    ativo = 'BBAS3'

    df_intraday = pd.read_csv(r"C:\Users\diaxt\Desktop\CO41BBAS3.CSV", sep=';')

  
    start_date = datetime.strptime(str(df_intraday['Data'].iloc[1]), "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(str(list(df_intraday['Data'].unique())[-1]), "%Y%m%d").strftime("%Y-%m-%d")
    dias_uteis = dias_uteis_b3(start_date, end_date, tipo_calendario='B3')
    
    for dia in dias_uteis:
        dia_int = int(dia.replace('-', ''))
        df_intraday_unique = df_intraday[df_intraday['Data'] == dia_int]
        # print(df_intraday_unique)
        if dia in FERIADOS_B3:
            continue

        if dia_int < 20211013:                
            df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_AJUSTADO, f'{ativo}_DIARIO_S.csv'), sep=';')
            is_adjusted = True
            df_diario['Data'] = df_diario['Data'].astype(int)

        elif dia_int == 20211013:
            df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_SEM_AJUSTE, f'{ativo}_DIARIO.csv'), sep=';') 
            is_adjusted = False
            
            df = pd.DataFrame(REGISTROS)
            moda_diferenca_abertura = df['Diferenca_Abertura'].mode()[0]
            print(moda_diferenca_abertura)
            print(df)
            df = df[df['Diferenca_Abertura'] != moda_diferenca_abertura]
            df['Moda'] = moda_diferenca_abertura
            REGISTROS = df.to_dict('list')
            if len(REGISTROS['Data']) == 0:
                REGISTROS = {
                    item: []
                    for item in df.to_dict()
                }
            df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
            df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
            df_diario['Data'] = df_diario['Data'].astype(int)
        else:
            df_diario = pd.read_csv(os.path.join(DIRETORIO_DIARIO_SEM_AJUSTE, f'{ativo}_DIARIO.csv'), sep=';') 
            is_adjusted = False
            
            df_diario['Data'] = pd.to_datetime(df_diario['Data'], format='%d/%m/%Y')
            df_diario['Data'] = df_diario['Data'].dt.strftime('%Y%m%d')
            df_diario['Data'] = df_diario['Data'].astype(int)
            ...
        abertura_diario = df_diario['Abertura']
        fechamento_diario = df_diario['Fechamento']
        
        if df_intraday_unique.shape[0] == 0:
            REGISTROS['Data'].append(dia_int)
            REGISTROS['Ticker'].append(ativo)
            REGISTROS['Abertura_Diario'].append(abertura_diario)
            REGISTROS['Abertura_Minuto'].append(0)
            REGISTROS['Diferenca_Abertura'].append(0)
            REGISTROS['Fechamento_Diario'].append(fechamento_diario)
            REGISTROS['Fechamento_Minuto'].append(0)
            REGISTROS['Diferenca_Fechamento'].append(0)
            
            if is_adjusted:
                REGISTROS['Problema'].append('Ajustado')
                REGISTROS['Moda'].append(0)
            else:
                REGISTROS['Problema'].append('Nao_Ajustado')
                REGISTROS['Moda'].append(1)                

        df_diario_filtrado = df_diario[df_diario['Data'] == dia_int] 
        
        try:
            abertura_diario = round(df_diario_filtrado['Abertura'].iloc()[0], 2)
            fechamento_diario = round(df_diario_filtrado['Fechamento'].iloc()[0], 2)
            abertura_intraday = round(df_intraday_unique['Abertura'].iloc()[0], 2)
            fechamento_intraday = round(df_intraday_unique['Fechamento'].iloc()[-1], 2)
        except IndexError as err:
            ERROS['Ticker'].append(ativo)
            ERROS['Motivo'].append(err)
            ERROS['Data'].append(dia_int)
            continue
            
        
        
        if (abertura_intraday == abertura_diario):
            print(abertura_intraday, abertura_diario, sep='->')
            continue
        else:
            REGISTROS['Data'].append(dia_int)
            REGISTROS['Ticker'].append(ativo)
            REGISTROS['Abertura_Diario'].append(abertura_diario)
            REGISTROS['Abertura_Minuto'].append(abertura_intraday)
            REGISTROS['Diferenca_Abertura'].append(round(abertura_diario / abertura_intraday, 3))
            REGISTROS['Fechamento_Diario'].append(fechamento_diario)
            REGISTROS['Fechamento_Minuto'].append(fechamento_intraday)
            REGISTROS['Diferenca_Fechamento'].append(round(fechamento_diario / fechamento_intraday, 3))
            
            if is_adjusted:
                REGISTROS['Problema'].append('Ajustado')
                REGISTROS['Moda'].append(0)
            else:
                REGISTROS['Problema'].append('Nao_Ajustado')
                REGISTROS['Moda'].append(1)

    df = pd.DataFrame(REGISTROS)
    periodo_com_problemas = df.shape[0]
    if periodo_com_problemas == 0: 
        return None
    
      
    periodo_dados = len(df_intraday['Data'].iloc[1:].unique()) 
    print(df_intraday['Data'].iloc[1:].unique())   

    df_intraday['Data'] = df_intraday['Data'].astype(str)
    df_intraday['Data'] = pd.to_datetime(df_intraday['Data'])

    DADOS_ESTATISTICOS['Ticker'].append(ativo)
    DADOS_ESTATISTICOS['Periodo_Inicial'].append(df_intraday['Data'].min())
    DADOS_ESTATISTICOS['Periodo_Final'].append(df_intraday['Data'].max())
    DADOS_ESTATISTICOS['Total_dias'].append(periodo_dados)
    DADOS_ESTATISTICOS['Dias_com_Erros'].append(periodo_com_problemas)
    DADOS_ESTATISTICOS['Percentual_Integridade'].append(round(((periodo_dados - periodo_com_problemas) / periodo_dados) * 100, 3))

    
    df.to_csv(os.path.join(PATH_ALERTADOR, f'{ativo}_estudo.csv'), sep=';', index=False)

    for colunas in REGISTROS.keys():
        REGISTROS[colunas].clear()

    df = pd.DataFrame(DADOS_ESTATISTICOS)                
    df.to_csv(os.path.join(PATH_ALERTADOR, f'Estatistica_Tickes.csv'), sep=';', index=False)

    for colunas in DADOS_ESTATISTICOS.keys():
        DADOS_ESTATISTICOS[colunas].clear()

    print(ERROS)
 
    
if __name__ == '__main__':
    main_CO41()
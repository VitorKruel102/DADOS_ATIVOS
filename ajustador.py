import os 
import pandas as pd
from pprint import pprint


DIRETORIO_DIARIO = r'D:\DADOS_FINANCEIROS\Dadabase_Profit_NA_split'
DIRETORIO_SEM_AJUSTE_MINUTO = r'D:\DADOS_FINANCEIROS\Database_Minuto'
DIRETORIO_AJUSTADO = r'C:\Users\diaxt\Desktop\SEMATIZA\MINUTO_AJUSTADO'

ATIVOS_INTERESSE = [
    'PETR4',  'ELET6',  'BBDC4',  'CMIG4',  'ELET3',  'EMBR3',  'PETR3',  'USIM5',
    'CPLE6',  'INEP4',  'ITSA4',  'CSNA3',  'GEPA4',  'CMIG3',  'MGLU3',  'KLBN4',
    'BBAS3',  'SBSP3',  'BRAP4',  'CGAS5',  'BRKM5',  'VALE3',  'GOAU4',  'CCRO3',
    'LIGT3',  'CSAN3',  'GOLL4',  'CYRE3',  'CPFE3',  'LREN3',  'GFSA3',  'JBSS3',
    'USIM3',  'RSID3',  'ITUB4',  'BRFS3',  'MRVE3',  'PDGR3',  'CIEL3',  'SANB11',
    'MRFG3',  'HYPE3',  'UGPA3',  'RENT3',  'DASA3',  'OIBR4',  'OIBR3',  'ENBR3',
    'BRPR3',  'BBDC3',  'ABEV3',  'BBSE3',  'QUAL3',  'ECOR3',  'EVEN3',  'KLBN11',
    'POMO4',  'RADL3',  'EQTL3',  'WEGE3',  'TAEE11', 'FLRY3',  'SUZB3',  'TRPL4',
    'SAPR11', 'CVCB3',  'LOGG3',  'IRBR3',  'AZUL4',  'BPAC11', 'HAPV3',  'TOTS3',
    'CRFB3',  'ENGI11', 'PCAR3',  'BEEF3',  'PRIO3',  'EZTC3',  'RDOR3',  'ENEV3',
    'VIVT3',  'ASAI3',  'ALSO3',  'LWSA3',  'BPAN4',  'PETZ3',  'CASH3',  'CMIN3',
    'SOMA3',  'POSI3',  'RRRP3',  'SLCE3',  'GGBR4',  'EGIE3',  'NTCO3',  'B3SA3',
    'AMER3',  'SYNE3',  'DXCO3',  'TIMS3',  'COGN3',  'YDUQ3',  'RAIL3',  'VIIA3',
    'VBBR3',  'ALPA4',  'JHSF3',  'BOVA11',
]

ATIVOS_INTERESSE = ['BOVA11', 'BBDC4',  'GGBR4',   'USIM5',   'GOAU4',   'CSNA3', 'CSAN3', 'BRKM5']

DADOS = {
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

ERROS = {
    'Ticker': [],
    'Motivo': [],
    'Data': []
}

def ajustador():
    """."""
    for ativo in ATIVOS_INTERESSE:
        ...
        arquivo_minuto = os.path.join(DIRETORIO_SEM_AJUSTE_MINUTO, f'{ativo}_BMF_I.csv')
        arquivo_diario = os.path.join(DIRETORIO_DIARIO, f'{ativo}_DIARIO_NAS.csv')

        df_diario = pd.read_csv(arquivo_diario, sep=';')
        df_diario['Data'] = df_diario['Data'].astype(int)

        df_minuto = pd.read_csv(arquivo_minuto, sep=';')

        for data in df_diario['Data']:
            df_diario_filtrado = df_diario[df_diario['Data'] == data]
            df_minuto_filtrado = df_minuto[df_minuto['<date>'] == data]
            
            try: 
                fechamento_diario = df_diario_filtrado['Fechamento'].iloc[0]
                fechamento_minuto = df_minuto_filtrado['<close>'].iloc[-1]
                fator_diferenca = (fechamento_diario / fechamento_minuto)
            except IndexError as err:
                ERROS['Ticker'].append(ativo)
                ERROS['Motivo'].append(err)
                ERROS['Data'].append(data)
                continue

            DADOS['<ticker>'] += list(df_minuto_filtrado['<ticker>'])
            DADOS['<trades>'] += list(df_minuto_filtrado['<trades>'])
            DADOS['<date>'] += list(df_minuto_filtrado['<date>'])
            DADOS['<time>'] += list(df_minuto_filtrado['<time>'])
            DADOS['<high>'] += list(df_minuto_filtrado['<high>'] * fator_diferenca)
            DADOS['<open>'] += list(df_minuto_filtrado['<open>'] * fator_diferenca)
            DADOS['<close>'] += list(df_minuto_filtrado['<close>'] * fator_diferenca)
            DADOS['<low>'] += list(df_minuto_filtrado['<low>'] * fator_diferenca)
            DADOS['<vol>'] += list(df_minuto_filtrado['<vol>'])    
            DADOS['<qty>'] += list(df_minuto_filtrado['<qty>'])    
            DADOS['<aft>'] += list(df_minuto_filtrado['<aft>'])    

        df = pd.DataFrame(DADOS)
        print(df)
        
        df.to_csv(os.path.join(DIRETORIO_AJUSTADO, f'{ativo}_BMF_I.csv'), sep=';', index=False)

        for colunas in DADOS.keys():
            DADOS[colunas].clear()
        
    pprint(ERROS)

    
ajustador()
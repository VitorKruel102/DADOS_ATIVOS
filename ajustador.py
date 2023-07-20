import os 
import pandas as pd
from pprint import pprint
from time import sleep

from Sematiza_ConcatenaTrocaDeTicker import ConcatenaTrocaAtivos

DIRETORIO_DIARIO = r'D:\DADOS_FINANCEIROS\Dadabase_Profit_NA_split'  # Dados com Ajuste e com Split
DIRETORIO_DIARIO_SPLIT = r'D:\DADOS_FINANCEIROS\Database_ProfitDiario_SPLIT'  # Dados com Ajuste e com Split
DIRETORIO_SEM_AJUSTE_MINUTO = r'D:\DADOS_FINANCEIROS\Database_Minuto' #E:\DADOS_FINANCEIROS\DADOS\Database_PrincipaisAcoes
DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE = r'D:\DADOS_FINANCEIROS\Database_DadosParaAjuste'
DIRETORIO_SEM_AJUSTE_MINUTO_CO41 = r'D:\DADOS_FINANCEIROS\Database_CO41'
DIRETORIO_AJUSTADO = r'D:\DADOS_FINANCEIROS\Database_MinutoAjustado'

ATIVOS_INTERESSE = [
    'IGTA3',
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
    'LAME4',
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

def copia_dados_para_pasta_dados_de_interesse_para_ajuste():
    """."""
    lista_de_ativos =  [f'{x}_BMF_I.csv' for x in ATIVOS_INTERESSE]

    for path, diretorios, arquivos in os.walk(DIRETORIO_SEM_AJUSTE_MINUTO):
        for arquivo in lista_de_ativos:
            if arquivo in lista_de_ativos:
                
                df = pd.read_csv(os.path.join(path, arquivo), sep=';')
                df.to_csv(os.path.join(DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE, arquivo), sep=';', index=False)   


def ajustador():
    """."""
    ATIVOS_INTERESSE = []
    for _, _, arquivos in os.walk(DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE):
        for arquivo in arquivos:
            nome_ticker = arquivo.split('_')[0]
            ATIVOS_INTERESSE.append(nome_ticker)

    for ativo in ATIVOS_INTERESSE:
        ...
        arquivo_minuto = os.path.join(DIRETORIO_SEM_AJUSTE_MINUTO, f'{ativo}_BMF_I.csv')
        arquivo_diario = os.path.join(DIRETORIO_DIARIO, f'{ativo}_DIARIO_NAS.csv')

        df_diario = pd.read_csv(arquivo_diario, sep=';')
        df_diario['Data'] = df_diario['Data'].astype(int)
        df_minuto = pd.read_csv(arquivo_minuto, sep=';')

        for data in df_diario['Data']:
            if data < 20211016:
                continue
            
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
        
    pprint(ERROS['Ticker'])
    pprint(ERROS['Data'])


def ajustador_co41():
    """."""
    for ativo in ATIVOS_INTERESSE:
        ...
        arquivo_minuto = os.path.join(DIRETORIO_SEM_AJUSTE_MINUTO_CO41, f'CO41{ativo}.csv')
        arquivo_diario = os.path.join(DIRETORIO_DIARIO_SPLIT, f'{ativo}_DIARIO_S.csv')

        df_diario = pd.read_csv(arquivo_diario, sep=';')
        df_diario = df_diario.fillna(0)
        print(df_diario.info())
        df_diario['Data'] = df_diario['Data'].astype(int)
        df_minuto = pd.read_csv(arquivo_minuto, sep=';')

        for data in df_diario['Data']:
            if data < 20041229:
                continue
            
            df_diario_filtrado = df_diario[df_diario['Data'] == data]
            df_minuto_filtrado = df_minuto[df_minuto['Data'] == data]

            try: 
                fechamento_diario = df_diario_filtrado['Fechamento'].iloc[0]
                fechamento_minuto = df_minuto_filtrado['Fechamento'].iloc[-1]
                fator_diferenca = (fechamento_diario / fechamento_minuto)
            except IndexError as err:
                ERROS['Ticker'].append(ativo)
                ERROS['Motivo'].append(err)
                ERROS['Data'].append(data)
                continue

            DADOS['<ticker>'] += list(df_minuto_filtrado['^Papel'])
            DADOS['<trades>'] += list(df_minuto_filtrado['Negocios'])
            DADOS['<date>'] += list(df_minuto_filtrado['Data'])
            DADOS['<time>'] += list(df_minuto_filtrado['Hora'])
            DADOS['<high>'] += list(df_minuto_filtrado['Maximo'] * fator_diferenca)
            DADOS['<open>'] += list(df_minuto_filtrado['Abertura'] * fator_diferenca)
            DADOS['<close>'] += list(df_minuto_filtrado['Fechamento'] * fator_diferenca)
            DADOS['<low>'] += list(df_minuto_filtrado['Minimo'] * fator_diferenca)
            DADOS['<vol>'] += list(df_minuto_filtrado['Volume'])    
            DADOS['<qty>'] += list(df_minuto_filtrado['Quantidade'])      
            DADOS['<aft>'] += list([0] * df_minuto_filtrado.shape[0])      

        df = pd.DataFrame(DADOS)
        print(df)
        
        df.to_csv(os.path.join(DIRETORIO_AJUSTADO, f'{ativo}_BMF_I.csv'), sep=';', index=False)

        for colunas in DADOS.keys():
            DADOS[colunas].clear()
        
    pprint(ERROS['Ticker'])
    pprint(ERROS['Data'])


def remove_folders(directory):
    """Delete all folders"""
    for root, _, files in os.walk(directory):
        for file in files:
            os.remove(os.path.join(root, file))


if __name__ == '__main__':
    remove_folders(DIRETORIO_AJUSTADO)
    # remove_folders(DIRETORIO_PRICIPAIS_TICKER_PARA_AJUSTE)
    # copia_dados_para_pasta_dados_de_interesse_para_ajuste()
    ConcatenaTrocaAtivos()
    ajustador()
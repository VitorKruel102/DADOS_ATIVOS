import os 

BASE_DIR = os.getcwd()

COMPUTADOR_DIAX = False

if COMPUTADOR_DIAX:
    DIRETORIO_PROFIT_AJUSTADO_COM_SPLIT = r'E:\DADOS_FINANCEIROS\Profit\Database_NAS'
    DIRETORIO_PROFIT_SEM_AJUSTE = r'E:\DADOS_FINANCEIROS\Profit\Database_ProfitDiario'
    DIRERORIO_ALERTADOR = r'E:\DADOS_FINANCEIROS\DADOS\Database_Alertador' 
    DIRETORIO_DADOS_PARA_AJUSTE = r'D:\DADOS_FINANCEIROS\Database_DadosParaAjuste'
    DIRETORIO_INTRADAY_MINUTOS = r''
else:
    DIRETORIO_PROFIT_AJUSTADO_COM_SPLIT = r'E:\DADOS_FINANCEIROS\Profit\Database_NAS'
    DIRETORIO_PROFIT_SEM_AJUSTE = r'E:\DADOS_FINANCEIROS\Profit\Database_ProfitDiario'
    DIRERORIO_ALERTADOR = r'E:\DADOS_FINANCEIROS\DADOS\Database_Alertador' 
    DIRETORIO_DADOS_PARA_AJUSTE = r'E:\DADOS_FINANCEIROS\DADOS\Database_DadosParaAjuste'
    DIRETORIO_INTRADAY_MINUTOS = r'E:\DADOS_FINANCEIROS\DADOS\Database_PrincipaisAcoes'
    # ARRUMAR PATH----^

PATH_ARQUIVO_FERIADOS = os.path.join(BASE_DIR, 'feriados-b3.json')
PATH_ATIVOS_QUE_MUDARAM_NOME = os.path.join(BASE_DIR, 'ativos-mudaram-ticker.json')
PATH_ARQUIVO_DADOS_INTERESSE = os.path.join(BASE_DIR, 'dados-interesse.json')

DIRETORIO_LOG = os.path.join(BASE_DIR, 'log', 'log-desempenho-alertador.txt')





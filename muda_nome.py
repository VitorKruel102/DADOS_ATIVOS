import os 
import pandas as pd

DIRETORIO = r'D:\DADOS_FINANCEIROS\Database_DiarioProfit'
DIARIO_MINUTO = r''


"""for path, dirs, files in os.walk(DIRETORIO):
    for file in files:
        is_file_old = file.split('_')[-1][:-4] == 'DIARIO'
        file_name = file.split('_')[0]
        new_name = f'{file_name}_DIARIO_NA'

        if not is_file_old:
           continue

        df = pd.read_csv(os.path.join(DIRETORIO, file), sep=';')
        df.to_csv(os.path.join(DIRETORIO, new_name), sep=';', index=False)

        os.remove(os.path.join(DIRETORIO, file))"""


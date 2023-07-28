import csv
import sys
import os

from abc import ABC, abstractmethod

sys.path.insert(0, os.path.join(os.getcwd(), 'config'))
import settings as _settings

class Log(ABC):


    @abstractmethod
    def _log(self, mensagem) -> None:
        pass

    def error(self, mensagem):
        """."""
        return self._log(f'ERROR NO PROCESSAMENTO: {mensagem}')
    
    def success(self, mensagem):
        """."""
        return self._log(mensagem)


class LogFileMixin(Log):


    def __init__(self, nome_arquivo , reiniciar_arquivo=False):
        self.reiniciar_arquivo  = reiniciar_arquivo
        self.nome_arquivo = nome_arquivo

    def _log(self, mensagem):
        """."""
        mensagem_formatada = f'{mensagem}'

        path_log = os.path.join(_settings.DIRETORIO_LOG, self.nome_arquivo)
        if self.reiniciar_arquivo:
            if os.path.exists(path_log):
                os.remove(path_log)

        with open(path_log, 'a', newline='', encoding='utf-8') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([mensagem_formatada])


class LogPrintMixin(Log):

    
     def _log(self, mensagem):
        """."""
        print(f'{mensagem}')


if __name__ == '__main__':
    objeto_print = LogPrintMixin()
    objeto_print.log_error('qualquer coisa')
    objeto_print.log_success('qualquer coisa')
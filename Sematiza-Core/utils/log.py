from abc import ABC, abstractmethod


class Log(ABC):


    @abstractmethod
    def _log(self, mensagem) -> None:
        pass

    def log_error(self, mensagem):
        """."""
        return self._log(f'Error: {mensagem}')
    
    def log_success(self, mensagem):
        """."""
        return self._log(f'Success: {mensagem}')


class LogFileMixin(Log):


    def _log(self, mensagem):
        """."""
        mensagem_formatada = f'{mensagem} ({self.__class__.__name__})'
        print(f'Salvando no log: {mensagem}')

        with open(LOG_FILE, 'a', encoding='utf-8') as arquivo:
            arquivo.write(mensagem_formatada)
            arquivo.write('\n')


class LogPrintMixin(Log):

    
     def _log(self, mensagem):
        """."""
        print(f'{mensagem} ({self.__class__.__name__})')


if __name__ == '__main__':
    objeto_print = LogPrintMixin()
    objeto_print.log_error('qualquer coisa')
    objeto_print.log_success('qualquer coisa')
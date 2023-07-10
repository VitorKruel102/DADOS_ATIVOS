# Projeto Semantiza

Esse projeto se divide em três parter:
- `ajustador` Utilizado para transformas as cotações em cotações com ajuste.
- `sematiza` Elimina os buracos dentro do dados.
- `alertador` Avisa quando os dados sem ajustes e compara com o fechamento e a abertura do Diario sem ajuste. 
- `Sematiza_TemposGraficos` Utilizados para criar os tempos gráficos.

### Sematiza_TempoGrafico.py

Projeto solicitado pelo Luciano para criação de planilhas para os ativos de interesse conforme
os tempos gráficos solicidados, sendo eles:

    - 1, 5, 10, 15, 20, 30, 60 minutos e Diario;

Os ativos precisam estar ajustados por dividendos. 

Para esse código, foi solicitado que esses tempos gráficos, o inicio do dia deve começar 30 depois da abertura oficial do mercado, ou seja, 
começar as 10h30.

Os dados são utilizados no formato do TradeIntraday.

Colunas:

    <ticker>;<date>;<time>;<trades>;<close>;<low>;<high>;<open>;<vol>;<qty>;<aft>

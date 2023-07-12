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


## Sematiza_Alertador.py

Alertador - Realiza uma análise estatística para identificar problemas nos dados. 
Sua avaliação é feita comparando os dados intraday de abertura e fechamento com 
os dados diários do Profit Pro.

É importante lembrar que existem dados ajustados e não ajustados no histórico:
    - DIA < 20211013 (DADOS AJUSTADOS)
    - DIA >= 20211013 (DADOS NÃO AJUSTADOS)

Portanto, é necessário identificar a data para determinar se devemos usar o Profit ajustado ou não ajustado na análise.

Serão realizados até três testes antes de confirmar os problemas nos dados:

    - Comparar a Abertura do dia no Intraday com o Diario;
    - Comparar Fechamentos do dia no Intraday com o Diario;
    - Os dias que não passaram no teste, será realizado o calculo da Moda para ver se mantém constantes a variação.
    (Precisa ter no mínimo 20 dias com problemas para tirar a moda).

Após uma conversa com Luciano, foi acordado que, se a variação for diferente no período de dados ajustados por dividendos, 
mas se mantiver estável no período, consideraremos os dados como íntegros. Para isso, todos os dados que não foram 
inicialmente validados pelo fechamento e abertura precisarão ter sua moda calculada. Todos os dias que forem iguais 
à moda serão considerados corretos e passarão no teste.

Para executar o código, são necessários os dados do Intraday e do Profit Diário no mesmo período em ambos os arquivos, 
para garantir a validação correta.

Será necessário obter os dias úteis da B3 para ver se há dias faltantes na base de dados.

Será criado um arquivo para cada ativo, contendo os dias e informações sobre os problemas, mostrando as diferenças 
entre as comparações de fechamento e abertura, bem como a diferença em relação à moda no segundo teste.

Ao final, será gerado um relatório geral com todos os ativos e suas respectivas integridades em porcentagem.

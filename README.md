# Manipulação de Dados da B3 - Projeto Semantiza

Este projeto tem como objetivo realizar a manipulação dos dados provenientes da B3 (Bolsa de Valores do Brasil). Ele oferece ferramentas e códigos que possibilitam o processamento dos dados para serem utilizados na Cointegração dos códigos do Luciano.

## Estrutura do Projeto

O projeto é organizado com os seguintes arquivos e pastas:

- **config**: Contém os arquivos de configurações de variáveis de ambiente necessárias para a execução do projeto.
- **log**: Armazena todos os arquivos de log gerados durante a execução dos códigos do projeto.
- **sematiza**: Esta pasta abriga os códigos responsáveis por manipular os dados. Cada arquivo neste diretório possui um prefixo "Sematiza_" seguido pelo nome correspondente à sua função dentro do projeto.
- **Utils**: Contém arquivos Python com utilidades específicas para o projeto.

## Arquivos na Base do Projeto

- **ativos-mudaram-ticker.json**: Um dicionário contendo informações sobre todos os ativos que alteraram de nome a partir 2021.
- **dados-interesse.json**: Uma lista com os ativos de interesse no projeto.
- **feriado-b3.json**: Contém todos os feriados da B3 de 2021 até 2023.
- **main.py**: O arquivo "main.py" é o gerenciador do projeto. Ele executa todos os códigos necessários para o processamento dos dados de forma a manter a aplicação funcional.

## Configuração do Ambiente

Para executar o projeto, é recomendado criar um ambiente virtual e instalar as bibliotecas necessárias a partir do arquivo "requirements.txt". O ambiente virtual isola as dependências do projeto e garante que todas as bibliotecas estejam na versão correta.

Siga os passos abaixo para configurar o ambiente:

1. Instale o Virtualenv (caso ainda não tenha instalado):

```bash
pip install virtualenv
```

2. Crie um ambiente virtual:

```bash
virtualenv nome_do_seu_ambiente
```

3. Ative o ambiente virtual:
- No Windows:
```bash
nome_do_seu_ambiente\Scripts\activate
```
- No Linux/macOS:
```bash
source nome_do_seu_ambiente/bin/activate
```

3. Instale as dependências::

```bash
pip install -r requirements.txt
```

## Funcionalidades dos Arquivos Sematiza

### Sematiza_Ajustador: 

- Responsável por realizar ajustes nos dados do intraday.

### Sematiza_Alertador:  

- Responsável por alertar e fazer a curadoria dos dados faltantes.

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

Após uma conversa com Luciano, foi acordado que, se a diferença(abertura e fechamento) for diferente no período de dados ajustados por dividendos, 
mas se mantiver estável no período, consideraremos os dados como íntegros. Para isso, todos os dados que não foram 
inicialmente validados pelo fechamento e abertura precisarão ter sua moda calculada. Todos os dias que forem iguais 
à moda serão considerados corretos e passarão no teste.

Para executar o código, são necessários os dados do Intraday e do Profit Diário no mesmo período em ambos os arquivos, 
para garantir a validação correta.

Será necessário obter os dias úteis da B3 para ver se há dias faltantes na base de dados.

Será criado um arquivo para cada ativo, contendo os dias e informações sobre os problemas, mostrando as diferenças 
entre as comparações de fechamento e abertura, bem como a diferença em relação à moda no segundo teste.

Ao final, será gerado um relatório geral com todos os ativos e suas respectivas integridades em porcentagem.


### Sematiza_ConcatenaTrocaDeTicker: 

- Responsável por concatenar os dados dos ativos que mudaram de nomes.

### Sematiza_TempoGrafico: 

- Cria as pastas e monta os tempos gráficos de interesse.

Projeto solicitado pelo Luciano para criação de planilhas para os ativos de interesse conforme
os tempos gráficos solicidados, sendo eles:

    - 1, 5, 10, 15, 20, 30, 60 minutos e Diario;

Os ativos precisam estar ajustados por dividendos. 

Para esse código, foi solicitado que esses tempos gráficos, o inicio do dia deve começar 30 depois da abertura oficial do mercado, ou seja, 
começar as 10h30.

Os dados são utilizados no formato do TradeIntraday.

Colunas:

    <ticker>;<date>;<time>;<trades>;<close>;<low>;<high>;<open>;<vol>;<qty>;<aft>

### Sematiza: 

- Prepara os dados dos tempos gráficos para o formato adequado para a cointegração.


## Contato

Se tiver dúvidas ou sugestões, não hesite em entrar em contato pelo e-mail [vkruel.programador@gmail.com] ou pelas redes sociais [@vitor_kruel].

## Estado do Projeto

O projeto está ativo e em constante desenvolvimento para aprimorar suas funcionalidades e fornecer uma plataforma sólida de manipulação de dados da B3.
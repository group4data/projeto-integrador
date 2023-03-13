# Projeto Integrador

## Sobre o projeto :id: :

Desafio final do curso de Data Engineer - Azure Cloud, ofertado pela Gama Academy, em parceria com a Accenture, no período de Janeiro a Março de 2023.

## Grupo composto por :princess: :

● Ana Paula Lima

● Beatriz Brandão

● Gabriela Nogueira

● Larissa Arruda

● Luana Rodrigues

● Pamella Farias

● Tássia Gonçalves

## Objetivo final :dart: :

Desenvolver uma aplicação em Python para carga de arquivos em um banco de dados SQL e gerar relatórios estatísticos visando a descoberta de fraudes em cartão de crédito.

## Dados :open_file_folder: :

Segue um arquivo zip com vários arquivos mostrando possíveis movimentações bancárias:

● transaction-in-001.csv, transaction-in-002.csv …

● transaction-out-001.csv, transaction-out-002.csv …

● clients-001.csv, clients-002.csv …

● Link Download: https://drive.google.com/file/d/1nXHnNt9dj03GB42SErcrNvZOzHwOyAcx/view?usp=sharing

## Instruções :bulb: :

O objetivo inicial é analisar estes arquivos criando uma base de dados relacional para fazer a carga e depois analisá-la. 

O cartão fraudado, será aquele que tiver movimentações abaixo de 2 minutos de espaçamento entre as transações.

## O que é esperado do projeto :warning: :

● Script de migração em Python

● Modelo de Entidades e Relacionamentos

● Relatórios de análise em SQL e PowerBI

● Códigos versionados no github.com

## O que será avaliado :eyes: :

● Semântica do código

● Lógica desenvolvida

● Organização do sistema migratório

● Documentação de utilização

● Hospedagem do sistema (nuvem Azure) para rodar os Scripts

● Análise relacional do banco de dados

● Objetivo alcançado com os relatórios apresentados no PowerBI

## Organização das tarefas :memo: :

https://github.com/orgs/group4data/projects/3/views/1

## Iniciando o projeto :bar_chart: :

1. Clonar o repósitorio;

2. Fazer o download e abrir o Docker Desktop;

3. Abrir pasta do projeto no VSCode; 

4. Escolher "Reopen Container" no VSCode.

Atenção: Para continuar funcionando, o Docker Desktop precisa estar aberto.

## Configurando o Banco de Dados :inbox_tray::

1. Criar um banco de dados no Portal Azure;

2. Criar um arquivo chamado .env com as informações do seu banco de dados;

3. Salvar o arquivo criado no repositório clonado.

Obs.: No repositório encontra-se um arquivo exemplificando como deve ser escrito.

## Modelo de entidade de relacionamento :



## Links e Softwares utilizados :paperclip: :

● VSCode: https://code.visualstudio.com/download

● Spark: https://spark.apache.org/downloads.html

● Python: https://www.python.org/downloads/

● Docker Desktop: https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=module

● Jupyter Notebook: https://jupyter.org/install

● SQL Server: https://learn.microsoft.com/en-us/sql/azure-data-studio/download-azure-data-studio

● Power BI: https://powerbi.microsoft.com/pt-br/downloads/

● Portal Azure: https://portal.azure.com/#home

## Referências bibliográficas:

https://betterprogramming.pub/pyspark-development-made-simple-9449a893ab17

https://docs.docker.com/desktop/windows/wsl/

https://learn.microsoft.com/pt-br/azure/data-factory/connector-sql-server?tabs=data-factory

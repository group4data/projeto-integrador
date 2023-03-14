# Projeto Integrador

[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://docs.python.org/3.9/)
[![PySpark](https://img.shields.io/badge/PySpark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)](https://spark.apache.org/docs/latest/api/python/)
[![Jupyter](https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white)](https://docs.jupyter.org/en/latest/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://docs.docker.com/)
[![Git](https://img.shields.io/badge/GIT-E44C30?style=for-the-badge&logo=git&logoColor=white)](https://git-scm.com/doc)
[![VSCode](https://img.shields.io/badge/VSCode-0078D4?style=for-the-badge&logo=visual%20studio%20code&logoColor=white)](https://code.visualstudio.com/docs)
[![MicrosoftSQLServer](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white)](https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16)
[![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://learn.microsoft.com/en-us/azure/?product=popular)
[![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://learn.microsoft.com/en-us/power-bi/)


## Descrição do projeto :id:

Uma aplicação desenvolvida em Python que possibilite a carregar arquivos em um banco de dados relacional hospedado na Azure e gerar relatórios estatísticos no PowerBI visando a descoberta de fraudes em transações bancárias.

No escopo desse projeto, fraudes são caracterizadas pela identificação de movimentações abaixo de 2 minutos de espaçamento entre transações realizadas pelo mesmo cliente.

O projeto consiste em um processo de ETL - Extract, Transform and Load (Extrair, Transformar e Carregar).

## Objetivos :dart:
:heavy_check_mark: Script de migração em Python  
:heavy_check_mark: Modelo de Entidades e Relacionamentos  
:heavy_check_mark: Relatórios de análise em SQL e PowerBI  
:heavy_check_mark: Versionamento do código e documentação  

## Dados :open_file_folder:

Os arquivos .csv referentes aos dados brutos encontram-se na pasta Data, que apresenta as subpastas Clients e Transactions.

>./Data/Clients

* clients-001.csv, clients-002.csv …  

|id|nome|email|data_cadastro|telefone|
| -------- | -------- | -------- |-------- | -------- |

>./Data/Transactions

* transaction-in-001.csv, transaction-in-002.csv …

* transaction-out-001.csv, transaction-out-002.csv …

|id|cliente_id|valor|data|
| -------- | -------- | -------- |-------- |


[Fonte dos dados](https://drive.google.com/file/d/1nXHnNt9dj03GB42SErcrNvZOzHwOyAcx/view?usp=sharing)


## Organização das tarefas :memo:

[Quadro de Gerenciamento do Projeto](https://github.com/orgs/group4data/projects/3/views/1)

## Pré-requisitos
Antes de rodar a aplicação, verifique se possui os seguintes itens instalados na máquina:
* [<b>Git</b>](https://git-scm.com/download/)
* [<b>Docker Desktop</b>](https://docs.docker.com/get-docker/)
* Uma IDE que possibilite rodar um Docker Container.  
<i>Sugestão</i>: [<b>Visual Studio Code</b>](https://code.visualstudio.com/download) com as extensões Docker e Dev Container.

:warning: O <b>Docker</b> precisa ser iniciado antes de abrir o container no VSCode.

## Como rodar a aplicação :arrow_forward:

1. Clonar o repósitorio usando o terminal
```
git clone git@github.com:group4data/projeto-integrador.git
```
2. Vá para o diretório do projeto
```
cd ./projeto-integrador/
```
3. Abra a pasta do projeto no VSCode
```
code .
```
4. Pressione <b>F1</b> e digite <b>Dev Containers: Reopen in Container</b> e escolha a opção <b>From 'Dockerfiler'</b>.  
<i>Obs</i>: A construção do container poderá ser demorada.

5. Adicione as variáveis de ambiente em arquivo .env para configurar os acessos necessários de acordo com o conteúdo do arquivo .env.example  
<i>Obs</i>: As informações de acesso referentes ao servidor e banco de dados hospedados na nuvem podem ser encontradas no portal da Azure. Certique-se de ter acesso liberado pelo IP da máquina utilizada no firewall do servidor antes de rodar a aplicação.

```dosini
# .env.example
server_name =
database_name =
username =
password =
```

6. Abra o terminal e rode o script principal
```
python3 main.py
```
ou rode o arquivo Jupyter Notebook: <b>jupyter_code.ipynb</b>

## Modelo de entidade de relacionamento

## Consultas e Análises no Banco de Dados SQL Server na [Azure Data Studio](https://azure.microsoft.com/pt-br/products/data-studio) :inbox_tray:
No arquivo sql_query.sql encontra-se o script das consultas e análises realizadas no banco de dados, incluindo a criação da tabela defrauded_clients.  
O conteúdo do arquivo deverá ser utilizado no Azure Data Studio para as análises.

## Relatório no PowerBI :bar_chart:


## Dependências e bibliotecas utilizadas :books:
* [pyspark](https://spark.apache.org/docs/latest/api/python/)
* [pyodc](https://pypi.org/project/pyodbc/)
* [os](https://docs.python.org/3/library/os.html)
* [glob](https://docs.python.org/3/library/glob.html)
* [python-dotenv](https://pypi.org/project/python-dotenv/)

## Alternativas para o processo de ETL na Azure :cloud:
O portal Azure possui diversos recursos para a manipulação de dados, logo o processo descrito acima também pode ser realizado com a utilização destes recursos.  
Abaixo tem-se a representação de possíveis alternativas.

### Azure Data Factory - ADF
Serviço de ETL na nuvem, sem necessidade de código, pois apresenta uma interface para o usuário.  

Após a execução do código que realiza a transformação e carregamento dos DataFrames para as tabelas CLIENTS, TRANSACTIONS_IN e TRANSACTIONS_OUT no banco de dados SQL Sever hospedado na Azure, utilizou-se o Azure Data Factory para a realização das seguintes atividades: 
1. Criação de linked service para extração dos dados do SQL Server
2. Criação de um pipeline para inserir as atividades do processo ETL
3. Criação de datasets para as atividades de script para criação de tabela e views, copy data para carregamento dos dados e lookup para as consultas (cada atividade somente será executada após o êxito da anterior)
4. Criação de linked service para carregar os dados em um Storage Account (Azure Blob)
5. Para possibilitar o uso da pipeline diversas vezes de acordo com a atualização do banco de dados, ao final da execução foram inseridas atividades de scripts com função de drop table e drop view.

Após a execução do pipeline, realizou-se conexão do Azure Blob a um PowerBI Dashboard para a geração de relatórios através da plataforma PowerBI Desktop.


## Desenvolvedoras :princess:

| [<img src="https://avatars.githubusercontent.com/u/94936213?v=4" width=115><br><sub>Ana Paula Lima</sub>](https://github.com/anapaaula) |  [<img src="https://avatars.githubusercontent.com/u/117690786?v=4" width=115><br><sub>Beatriz Brandão</sub>](https://github.com/biacbrandao) |  [<img src="https://avatars.githubusercontent.com/u/101880070?v=4" width=115><br><sub>Gabriela Nogueira</sub>](https://github.com/nogueiragabriela/) | [<img src="https://avatars.githubusercontent.com/u/127163325?v=4" width=115><br><sub>Larissa Arruda</sub>](https://github.com/LarissaArruda08) | [<img src="https://avatars.githubusercontent.com/u/85495937?v=4" width=115><br><sub>Luana Rodrigues</sub>](https://github.com/lurodig) | [<img src="https://avatars.githubusercontent.com/u/111579939?v=4" width=115><br><sub>Pamella Farias</sub>](https://github.com/PamellaFarias) | [<img src="https://avatars.githubusercontent.com/u/121309155?v=4" width=115><br><sub>Tássia Gonçalves</sub>](https://github.com/goncalvestassia) |
| :----: | :----: | :----: | :----: | :----: | :----: | :----: |

<b>Orientador:</b> Danilo Aparecido dos Santos

<b>Professora:</b> Camila Ávila

<b>Yellow Belt:</b> Ynnaê Melo


## Referências

1. [PySpark Development](https://betterprogramming.pub/pyspark-development-made-simple-9449a893ab17)

2. [WSL](https://docs.docker.com/desktop/windows/wsl/)

3. [Azure Data Factory](https://learn.microsoft.com/pt-br/azure/data-factory/connector-sql-server?tabs=data-factory)

# Projeto Integrador

## Sobre o projeto :id: :

Desafio final do curso de Data Engineer - Azure Cloud, ofertado pela Gama Academy, em parceria com a Accenture, no período de Janeiro a Março de 2023.

## Grupo composto por :princess: :

| [<img src="https://avatars.githubusercontent.com/u/94936213?v=4" width=115><br><sub>Ana Paula Lima</sub>](https://github.com/anapaaula) |  [<img src="https://avatars.githubusercontent.com/u/117690786?v=4" width=115><br><sub>Beatriz Brandão</sub>](https://github.com/biacbrandao) |  [<img src="https://avatars.githubusercontent.com/u/101880070?v=4" width=115><br><sub>Gabriela Nogueira</sub>](https://github.com/nogueiragabriela/) | [<img src="https://avatars.githubusercontent.com/u/127163325?v=4" width=115><br><sub>Larissa Arruda</sub>](https://github.com/LarissaArruda08) | [<img src="https://avatars.githubusercontent.com/u/85495937?v=4" width=115><br><sub>Luana Rodrigues</sub>](https://github.com/lurodig) | [<img src="https://avatars.githubusercontent.com/u/111579939?v=4" width=115><br><sub>Pamella Farias</sub>](https://github.com/PamellaFarias) | [<img src="https://avatars.githubusercontent.com/u/121309155?v=4" width=115><br><sub>Tássia Gonçalves</sub>](https://github.com/goncalvestassia) |
| :----: | :----: | :----: | :----: | :----: | :----: | :----: |

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

## Pré-requisitos
Antes de rodar a aplicação, verifique se possui os seguintes itens instalados na máquina:
* [<b>Git</b>](https://git-scm.com/download/)
* [<b>Docker</b>](https://docs.docker.com/get-docker/)
* Uma IDE que possibilite rodar um Docker Container.  
<i>Sugestão</i>: [<b>Visual Sutdio Code</b>](https://code.visualstudio.com/download) com as extensões Docker e Dev Container.

:warning: O <b>Docker</b> precisa ser iniciado antes de rodar a aplicação

## Como rodar a aplicação :arrow_forward:

1. Clonar o repósitorio usando o terminal;
```
git clone git@github.com:group4data/projeto-integrador.git
```
2. Vá para o diretório do projeto; 
```
cd ./projeto-integrador/
```
3. Abra a pasta do projeto no VSCode;
```
code .
```
4. Pressione <b>F1</b> e digite <b>Dev Containers: Reopen in Container</b> e escolha a opção <b>From 'Dockerfiler'</b>.
5. Abra o terminal e rode o script
```
python3 main.py
```

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

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

## Sobre o projeto :id:

Desafio final do curso de Data Engineer - Azure Cloud, ofertado pela Gama Academy, em parceria com a Accenture, no período de Janeiro a Março de 2023.


## Objetivo final :dart:

Desenvolver uma aplicação em Python para carga de arquivos em um banco de dados SQL e gerar relatórios estatísticos visando a descoberta de fraudes em cartão de crédito.

## Dados :open_file_folder:

Segue um arquivo zip com vários arquivos mostrando possíveis movimentações bancárias:

● transaction-in-001.csv, transaction-in-002.csv …

● transaction-out-001.csv, transaction-out-002.csv …

● clients-001.csv, clients-002.csv …

● Link Download: https://drive.google.com/file/d/1nXHnNt9dj03GB42SErcrNvZOzHwOyAcx/view?usp=sharing

## Instruções :bulb:

O objetivo inicial é analisar estes arquivos criando uma base de dados relacional para fazer a carga e depois analisá-la. 

O cartão fraudado, será aquele que tiver movimentações abaixo de 2 minutos de espaçamento entre as transações.

## O que é esperado do projeto :warning:

● Script de migração em Python

● Modelo de Entidades e Relacionamentos

● Relatórios de análise em SQL e PowerBI

● Códigos versionados no github.com

## O que será avaliado :eyes:

● Semântica do código

● Lógica desenvolvida

● Organização do sistema migratório

● Documentação de utilização

● Hospedagem do sistema (nuvem Azure) para rodar os Scripts

● Análise relacional do banco de dados

● Objetivo alcançado com os relatórios apresentados no PowerBI

## Organização das tarefas :memo:

https://github.com/orgs/group4data/projects/3/views/1

## Pré-requisitos
Antes de rodar a aplicação, verifique se possui os seguintes itens instalados na máquina:
* [<b>Git</b>](https://git-scm.com/download/)
* [<b>Docker</b>](https://docs.docker.com/get-docker/)
* Uma IDE que possibilite rodar um Docker Container.  
<i>Sugestão</i>: [<b>Visual Studio Code</b>](https://code.visualstudio.com/download) com as extensões Docker e Dev Container.

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
<i>Obs</i>: A construção do container poderá ser demorada.
5. Abra o terminal e rode o script principal
```
python3 main.py
```
ou rode o arquivo Jupyter Notebook: <b>jupyter_code.ipynb</b>


## Configurando o Banco de Dados :inbox_tray:

1. Criar um banco de dados no Portal Azure;

2. Criar um arquivo chamado .env com as informações do seu banco de dados;

3. Salvar o arquivo criado no repositório clonado.

Obs.: No repositório encontra-se um arquivo exemplificando como deve ser escrito.

## Modelo de entidade de relacionamento :



## Dependências e bibliotecas utilizadas :books:
* pyspark
* pyodc
* os
* glob
* python-dotenv

## Desenvolvedoras :princess:

| [<img src="https://avatars.githubusercontent.com/u/94936213?v=4" width=115><br><sub>Ana Paula Lima</sub>](https://github.com/anapaaula) |  [<img src="https://avatars.githubusercontent.com/u/117690786?v=4" width=115><br><sub>Beatriz Brandão</sub>](https://github.com/biacbrandao) |  [<img src="https://avatars.githubusercontent.com/u/101880070?v=4" width=115><br><sub>Gabriela Nogueira</sub>](https://github.com/nogueiragabriela/) | [<img src="https://avatars.githubusercontent.com/u/127163325?v=4" width=115><br><sub>Larissa Arruda</sub>](https://github.com/LarissaArruda08) | [<img src="https://avatars.githubusercontent.com/u/85495937?v=4" width=115><br><sub>Luana Rodrigues</sub>](https://github.com/lurodig) | [<img src="https://avatars.githubusercontent.com/u/111579939?v=4" width=115><br><sub>Pamella Farias</sub>](https://github.com/PamellaFarias) | [<img src="https://avatars.githubusercontent.com/u/121309155?v=4" width=115><br><sub>Tássia Gonçalves</sub>](https://github.com/goncalvestassia) |
| :----: | :----: | :----: | :----: | :----: | :----: | :----: |

<b>Orientador:</b> Danilo Aparecido dos Santos

<b>Professora:</b> Camila Ávila

<b>Yellow Belt:</b> Ynnae Melo


## Referências bibliográficas:

https://betterprogramming.pub/pyspark-development-made-simple-9449a893ab17

https://docs.docker.com/desktop/windows/wsl/

https://learn.microsoft.com/pt-br/azure/data-factory/connector-sql-server?tabs=data-factory

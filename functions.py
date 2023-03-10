import glob
import csv
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import *

def transform_csv_to_df(spark, path, schema):
    if not os.path.isdir(path):
        raise ValueError(f"{path} não é um diretório válido.")

    list_paths_csv = glob.glob(os.path.join(path, '*.csv'))

    if not list_paths_csv:
        raise ValueError(f"Não foram encontrados arquivos csv em {path}.")

    df = spark.read.csv(list_paths_csv, sep=';', schema=schema, inferSchema=True)

    df = df.filter(~col('id').contains('id'))

    return df

def verify_empty_data(df):
    for col_name in df.columns:
        data_type = df.schema[col_name].dataType
        if data_type == StringType():
            count_empty = df.filter((col(col_name) == '') | isnull(col_name) | isnan(col_name) | (col(col_name).isNull())).count()
            if count_empty != 0:
                print(f"Column '{col_name}' has {count_empty} empty/null/none/NaN values.")
        elif data_type == IntegerType():
            count_null = df.filter(col(col_name).isNull()).count()
            if count_null != 0:
                print(f"Column '{col_name}' has {count_null} null values.")
        elif data_type == TimestampType():
            count_null = df.filter(col(col_name).isNull()).count()
            if count_null != 0:
                print(f"Column '{col_name}' has {count_null} null values.")

def correcting_data(spark, df):
    df = df.withColumn("valor", round(col("valor"), 2))
    df = df.withColumn('valor', expr('abs(valor)'))
    return df

def add_state_column(df):
    # Cria coluna do DDD a partir da coluna Telefone
    df = df.withColumn('DDD', split(df['telefone'], r'[()]+').getItem(1))
    # Substitui os DDDs pelo Estado correspondente
    df = df.withColumn('estado', when(col('DDD') == '20', 'Inválido')
                        .when(col('DDD') == '21', 'Rio de Janeiro')
                        .when(col('DDD') == '22', 'Rio de Janeiro')
                        .when(col('DDD') == '23', 'Inválido')
                        .when(col('DDD') == '24', 'Rio de Janeiro')
                        .when(col('DDD') == '25', 'Inválido')
                        .when(col('DDD') == '26', 'Inválido')
                        .when(col('DDD') == '27', 'Espírito Santo')
                        .when(col('DDD') == '28', 'Espírito Santo')
                        .when(col('DDD') == '29', 'Inválido')
                        .when(col('DDD') == '30', 'Inválido')
                        .otherwise('Inválido'))
    # Apagando coluna DDD
    df = df.drop('DDD')
    return df

def format_names(df):
    # Separando os nomes dos sobrenomes
    df = df.withColumn("nome_split", split(df.nome, " "))
    df = df.withColumn("nome", df.nome_split[0])
    df = df.withColumn("sobrenome1", df.nome_split[1])
    df = df.withColumn("sobrenome2", df.nome_split[2])
    df = df.withColumn("sobrenome3", df.nome_split[3])
    df = df.withColumn("sobrenome4", df.nome_split[4])
    df = df.withColumn("sobrenome5", df.nome_split[5])
    df = df.withColumn("sobrenome6", df.nome_split[6])
    df = df.withColumn("sobrenome", concat_ws(" ", "sobrenome1", "sobrenome2", "sobrenome3", "sobrenome4", "sobrenome5", "sobrenome6"))
    df = df.drop("nome_split", "sobrenome1", "sobrenome2", "sobrenome3", "sobrenome4", "sobrenome5", "sobrenome6")
    # Colocando primeira letra de cada nome Maiúscula
    df = df.withColumn("nome", initcap(df.nome))
    df = df.withColumn("sobrenome", initcap(df.sobrenome))
    # Substituindo linhas vazias por Não Informado
    df = df.withColumn("sobrenome", when(df.sobrenome == "", "Não informado").otherwise(df.sobrenome))
    # Ordenando as colunas
    df = df.select("id", "nome", "sobrenome", "email", "data_cadastro", "telefone", "estado")
    return df
    

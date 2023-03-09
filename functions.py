import glob
import csv
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import *


# Transforma varios arquivos csv em um Data Frame 
def transform_csv_to_df(spark, path, columns):
    # Verifica se path existe e é um diretório
    if not os.path.isdir(path):
        raise ValueError(f"{path} não é um diretório válido.")

    list_paths_csv = glob.glob(os.path.join(path, '*.csv'))

    # Verifica se há arquivos csv no diretório especificado
    if not list_paths_csv:
        raise ValueError(f"Não foram encontrados arquivos csv em {path}.")

    # Verifica se columns é uma lista de strings válidas
    if not all(isinstance(col, str) for col in columns):
        raise ValueError("columns deve ser uma lista de strings.")

    df_header = None
    for file in list_paths_csv:
        with open(file, 'r') as file_csv:
            leitor = csv.reader(file_csv)
            first_line = next(leitor)[0].split(';')

            if first_line == columns:
                df_header = spark.read.csv(file, sep=';', header=True, inferSchema=True)
                list_paths_csv.remove(file)
                break

    if df_header is None:
        raise ValueError(f"Nenhum arquivo com a primeira linha igual a {columns} foi encontrado.")

    try:
        df_no_header = spark.read.csv(list_paths_csv, sep=';', header=False, inferSchema=True)
        return df_header.unionAll(df_no_header)
    except Exception as e:
        raise RuntimeError(f"Ocorreu um erro ao unir os DataFrames: {str(e)}")


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

def correcting_data(spark, df, columns):
    if columns == df.columns:
        df = df.withColumn("valor", round(col("valor"), 2))
        df = df.withColumn('valor', expr('abs(valor)'))
        return df


def add_state_column(df):
    # Cria coluna do DDD a partir da coluna Telefone
    df = df.withColumn('DDD', split(df['telefone'], r'[()]+').getItem(1))
    # Substitui os DDDs pelo Estado correspondente
    df = df.withColumn('Estado', when(col('DDD') == '20', 'Inválido')
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

def verify_schema(df, expected_schema, expected_columns):
    actual_schema = df.schema

    if actual_schema == expected_schema:
        print("The schema is correct!")
    else:
        actual_columns = [(f.name, f.dataType) for f in actual_schema.fields]
        missing_columns = set(expected_columns) - set([c[0] for c in actual_columns])
        unexpected_columns = set([c[0] for c in actual_columns]) - set(expected_columns)
        incorrect_types = [(c[0], c[1]) for c in actual_columns if c[0] in expected_columns and c[1] != expected_schema[c[0]].dataType]
        if missing_columns:
            print("Error: The following columns are missing: {}".format(missing_columns))
        if unexpected_columns:
            print("Error: The following columns are unexpected: {}".format(unexpected_columns))
        if incorrect_types:
            print("Error: The following columns have incorrect data types: {}".format(incorrect_types))
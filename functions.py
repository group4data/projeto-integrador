import glob
import csv
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import col,isnan,when,count, isnull


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


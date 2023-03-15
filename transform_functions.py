import glob
import csv
import os

from pyspark.sql.types import *
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
                df = df.fillna({col_name: 'Não informado'})
        elif data_type == IntegerType():
            count_null = df.filter(col(col_name).isNull()).count()
            if count_null != 0:
                print(f"Column '{col_name}' has {count_null} null values.")
                df = df.fillna({col_name: 0})
        elif data_type == DoubleType():
            count_null = df.filter(col(col_name).isNull()).count()
            if count_null != 0:
                print(f"Column '{col_name}' has {count_null} null values.")
                df = df.fillna({col_name: 0.00})
        elif data_type == TimestampType():
            count_null = df.filter(col(col_name).isNull()).count()
            if count_null != 0:
                print(f"Column '{col_name}' has {count_null} null values.")
                df = df.fillna({col_name: '1900-01-01 00:00:00'})
    return df

def correcting_data(df):
    df = df.withColumn("value", round(col("value"), 2))
    df = df.withColumn("value", expr('abs(value)'))
    return df

def add_state_column(df):
    df = df.withColumn('DDD', split(df['phone_number'], r'[()]+').getItem(1))
    df = df.withColumn('state', when(col('DDD') == '20', 'Paraíba')
                        .when(col('DDD') == '21', 'Rio de Janeiro')
                        .when(col('DDD') == '22', 'Mato Grosso')
                        .when(col('DDD') == '23', 'Pernambuco')
                        .when(col('DDD') == '24', 'Rio de Janeiro')
                        .when(col('DDD') == '25', 'Bahia')
                        .when(col('DDD') == '26', 'Minas Gerais')
                        .when(col('DDD') == '27', 'Espírito Santo')
                        .when(col('DDD') == '28', 'Roraima')
                        .when(col('DDD') == '29', 'São Paulo')
                        .when(col('DDD') == '30', 'Maranhão')
                        .otherwise('Inválido'))
    df = df.drop('DDD')
    return df

def format_names(df):
    df = df.withColumn("name_split", split(df.name, " "))
    df = df.withColumn("name", df.name_split[0])
    df = df.withColumn("last_name1", df.name_split[1])
    df = df.withColumn("last_name2", df.name_split[2])
    df = df.withColumn("last_name3", df.name_split[3])
    df = df.withColumn("last_name4", df.name_split[4])
    df = df.withColumn("last_name5", df.name_split[5])
    df = df.withColumn("last_name6", df.name_split[6])
    df = df.withColumn("last_name", concat_ws(" ", "last_name1", "last_name2", "last_name3", "last_name4", "last_name5", "last_name6"))
    df = df.drop("name_split", "last_name1", "last_name2", "last_name3", "last_name4", "last_name5", "last_name6")
    df = df.withColumn("name", initcap(df.name))
    df = df.withColumn("last_name", initcap(df.last_name))
    df = df.withColumn("last_name", when(df.last_name == "", "Não informado").otherwise(df.last_name))
    df = df.select("id", "name", "last_name", "email", "date_time_register", "phone_number", "state")
    return df

def verify_client_id_existence(spark, df_transactions, df_clients):
    df_ids_transactions = df_transactions.select(col('client_id'))
    df_ids_clients = df_clients.select(col('id'))
    df_new_clients = df_ids_transactions.join(df_ids_clients, df_ids_transactions.client_id == df_ids_clients.id, "leftanti")
    df_new_clients = df_new_clients.distinct()
    df_new_clients = df_new_clients.withColumnRenamed("client_id", "id")
    df_new_clients = df_new_clients.withColumn('name', lit('Não localizado'))
    df_new_clients = df_new_clients.withColumn('last_name', lit('Não localizado'))
    df_new_clients = df_new_clients.withColumn('email', lit('Não localizado'))
    df_new_clients = df_new_clients.withColumn('date_time_register', lit('1900-01-01 00:00:00').cast('timestamp'))
    df_new_clients = df_new_clients.withColumn('phone_number', lit('Não localizado'))
    df_new_clients = df_new_clients.withColumn('state', lit('Não localizado'))
    df_clients = df_clients.unionAll(df_new_clients)
    return df_clients

def renamed_column(df, previous_column, new_column):
     return df.withColumnRenamed(previous_column, new_column)

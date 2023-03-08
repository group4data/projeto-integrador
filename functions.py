import glob
import csv
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import col,isnan,when,count


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

def validate_clients(spark, path, columns):
    #path = './Data/Clients'
    clients = spark.read.csv(path, sep=';', inferSchema=True)
    
    clients = clients.toDF(*columns)

    clients = clients.filter(~col('id').contains('id'))

    clients = clients.withColumn("data_cadastro", f.to_timestamp("data_cadastro"))
    clients = clients.withColumn("id",clients.id.cast(IntegerType()))

    return clients


def verify_empty_clients_data(clients):
    clients_string_columns = ['nome','email','telefone']
    count_empty_strings = clients.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in clients_string_columns])
    
    print("Number of null or empty values in the following columns.")
    count_empty_strings.show()

    clients_number_columns = ['id','data_cadastro']
    count_empty_numbers = clients.select([count(when(col(c).isNull(), c)).alias(c)
                    for c in clients_number_columns])
    
    print("Number of null or empty values in the following columns.")
    count_empty_numbers.show()


def verify_clients_schema(clients, columns):
    expected_columns = columns
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("data_cadastro", TimestampType(), True),
        StructField("telefone", StringType(), True)
    ])

    actual_schema = clients.schema

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


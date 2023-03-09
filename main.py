from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from functions import transform_csv_to_df, verify_empty_data, correcting_data, add_state_column
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

    
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4051') \
    .getOrCreate()

clients = "Data/Clients"
transactions_in = "Data/Transactions-in"
transactions_out = "Data/Transactions-out"

columns_clients = ['id', 'nome', 'email', 'data_cadastro', 'telefone']
columns_transactions = ['id', 'cliente_id', 'valor', 'data']


try:
    print("Transformando os arquivos CSVs em data frames...")
    df_clients = transform_csv_to_df(spark, clients, columns_clients)
    df_transactions_in = transform_csv_to_df(spark, transactions_in, columns_transactions)
    df_transactions_out = transform_csv_to_df(spark, transactions_out , columns_transactions)
    
    print("Verificando se há dados não informados nas colunas dos DataFrames...")
    verify_empty_data(df_clients)
    verify_empty_data(df_transactions_in)
    verify_empty_data(df_transactions_out)

    print("Corrigindo os dados da coluna valor dos DataFrames de transações...")
    df_transactions_in = correcting_data(spark, df_transactions_in, columns_transactions)
    df_transactions_out = correcting_data(spark, df_transactions_out, columns_transactions)
        
    print("Adicionando a coluna de estado na planilha de clientes...")
    df_clients = add_state_column(df_clients)
    
    print("-" * 20)
    print("Transações in")
    df_transactions_in.show()
    print("-" * 20)
    print("Transações out")
    df_transactions_out.show()
    print("-" * 20)
    print("Dados dos clientes")
    df_clients.show()
    
except Exception as e:
    print(f"Ocorreu o seguinte erro: {e}!")
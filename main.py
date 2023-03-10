from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from functions import transform_csv_to_df, verify_empty_data, correcting_data, add_state_column, format_names
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functions_database import connection_database, create_table_clients, create_table_transactions
import pyodbc

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4051') \
    .getOrCreate()

clients = "Data/Clients"
transactions_in = "Data/Transactions-in"
transactions_out = "Data/Transactions-out"

clients_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("data_cadastro", TimestampType(), True),
        StructField("telefone", StringType(), True)
    ])

transactions_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("cliente_id", IntegerType(), True),
        StructField("valor", DoubleType(), True),
        StructField("data", TimestampType(), True),
    ])

try:
    print("Transformando os arquivos CSVs em data frames...")
    df_clients = transform_csv_to_df(spark, clients, clients_schema)
    df_transactions_in = transform_csv_to_df(spark, transactions_in, transactions_schema)
    df_transactions_out = transform_csv_to_df(spark, transactions_out , transactions_schema)
   
    print("Verificando se há dados não informados nas colunas dos DataFrames...")
    verify_empty_data(df_clients)
    verify_empty_data(df_transactions_in)
    verify_empty_data(df_transactions_out)
    
    print("Corrigindo os dados da coluna valor dos DataFrames de transações...")
    df_transactions_in = correcting_data(spark, df_transactions_in)
    df_transactions_out = correcting_data(spark, df_transactions_out)

    print("Adicionando a coluna de estado na planilha de clientes...")
    df_clients = add_state_column(df_clients)
    
    print("Formatando a coluna de nomes dos clientes...")
    df_clients = format_names(df_clients)
        
    print("-" * 30)
    print("Transações in")
    df_transactions_in.show()
    print("-" * 30)
    print("Transações out")
    df_transactions_out.show()
    print("-" * 30)
    print("Dados dos clientes")
    df_clients.show()

    '''
    try:
        print("Conectando com o banco de dados...")
        conn = connection_database()
    except Exception:
        print("Não foi possivel se conectar com o banco de dados!")
    else:
        print("Criando tabela de clientes no banco de dados!")
        create_table_clients(conn, df_clients)
        
        df_transactions_in = df_transactions_in.join(df_clients, df_clients.id == df_transactions_in.cliente_id, "leftsemi")
        df_transactions_out = df_transactions_out.join(df_clients, df_clients.id == df_transactions_out.cliente_id, "leftsemi")

        print("Criando as tabelas de transações no banco de dados!")
        create_table_transactions(conn, df_transactions_in, "transactions_in")
        create_table_transactions(conn, df_transactions_out, "transactions_out")
    '''
    
except Exception as e:
    print(f"Ocorreu o seguinte erro: {e}!")
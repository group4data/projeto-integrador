from pyspark.sql import SparkSession

from transform_functions import transform_csv_to_df, verify_empty_data, correcting_data, add_state_column, format_names, verify_client_id_existence, renamed_column
from database_functions import connection_database, create_table_clients, create_table_transactions, insert_df_into_db

from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
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
    print("OK")

    print("Verificando se há dados não informados nas colunas dos DataFrames...")
    df_clients = verify_empty_data(df_clients)
    df_transactions_in = verify_empty_data(df_transactions_in)
    df_transactions_out = verify_empty_data(df_transactions_out)
    print("OK")

    print("Corrigindo os dados da coluna valor dos DataFrames de transações...")
    df_transactions_in = correcting_data(df_transactions_in)
    df_transactions_out = correcting_data(df_transactions_out)
    print("OK")

    print("Formatando o DataFrame de clientes...")
    df_clients = add_state_column(df_clients)
    df_clients = format_names(df_clients)
    df_clients = verify_client_id_existence(spark, df_transactions_in, df_clients)
    df_clients = verify_client_id_existence(spark, df_transactions_out, df_clients)
    print("OK")
    
    print("Alterando o nome das colunas de data e hora dos DataFrames...")
    df_clients = renamed_column(df_clients,"data_cadastro", "data_hora_cadastro")
    df_transactions_in = renamed_column(df_transactions_in, "data", "data_hora")
    df_transactions_out = renamed_column(df_transactions_out, "data", "data_hora")
    print("OK")

    try:
        print("Conectando com o banco de dados...")
        conn = connection_database()
        print("OK")
    except Exception as e:
        print(f"Não foi possivel se conectar com o banco de dados! Por causa do seguinte erro: {e}")
    else:
        print("\nCriando tabela de clientes no banco de dados!")
        create_table_clients(conn, df_clients)
        
        print("\nInserindo dados na tabela...")
        insert_df_into_db(conn, df_clients, "clientes")
            
        print("\nCriando a tabela de transações in no banco de dados!")
        create_table_transactions(conn, df_transactions_in, "transactions_in")

        print("\nInserindo dados na tabela...")
        insert_df_into_db(conn, df_transactions_in, "transactions_in")
    
        print("\nCriando a tabela de transações out no banco de dados!")
        create_table_transactions(conn, df_transactions_out, "transactions_out")

        print("\nInserindo dados na tabela...")
        insert_df_into_db(conn, df_transactions_out, "transactions_out")
       
    print("\n")
    print("-" * 30)
    print("Transações in")
    df_transactions_in.show()
    print("-" * 30)
    print("Transações out")
    df_transactions_out.show()
    print("-" * 30)
    print("Dados dos clientes")
    df_clients.show()
    
except Exception as e:
    print(f"Ocorreu o seguinte erro: {e}!")


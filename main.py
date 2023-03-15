from pyspark.sql import SparkSession

from transform_functions import transform_csv_to_df, verify_empty_data, correcting_data, add_state_column, format_names, verify_client_id_existence, renamed_column
from database_functions import connection_database, create_table_clients, create_table_transactions, insert_df_into_db

from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Starting Spark Session") \
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
    print("Transforming CSV files into DataFrame....")
    df_clients = transform_csv_to_df(spark, clients, clients_schema)
    df_transactions_in = transform_csv_to_df(spark, transactions_in, transactions_schema)
    df_transactions_out = transform_csv_to_df(spark, transactions_out , transactions_schema)
    print("OK")

    print("Renaming DataFrames columns...")
    df_clients = renamed_column(df_clients,"nome", "name")
    df_clients = renamed_column(df_clients,"data_cadastro", "date_time_register")
    df_clients = renamed_column(df_clients,"telefone", "phone_number")

    df_transactions_in = renamed_column(df_transactions_in, "cliente_id", "client_id")
    df_transactions_in = renamed_column(df_transactions_in, "valor", "value")
    df_transactions_in = renamed_column(df_transactions_in, "data", "date_time")

    df_transactions_out = renamed_column(df_transactions_out, "cliente_id", "client_id")
    df_transactions_out = renamed_column(df_transactions_out, "valor", "value")
    df_transactions_out = renamed_column(df_transactions_out, "data", "date_time")
    print("OK")

    print("Checking for missing data in DataFrames columns...")
    df_clients = verify_empty_data(df_clients)
    df_transactions_in = verify_empty_data(df_transactions_in)
    df_transactions_out = verify_empty_data(df_transactions_out)
    print("OK")

    print("Correcting data in the value column of transactions DataFrames..")
    df_transactions_in = correcting_data(df_transactions_in)
    df_transactions_out = correcting_data(df_transactions_out)
    print("OK")

    print("Formatting the clients DataFrame...")
    df_clients = add_state_column(df_clients)
    df_clients = format_names(df_clients)
    df_clients = verify_client_id_existence(df_transactions_in, df_clients)
    df_clients = verify_client_id_existence(df_transactions_out, df_clients)
    print("OK")
    
    try:
        print("Connecting to the database...")
        conn = connection_database()
        print("OK")
    except Exception as e:
        print(f"Unable to connect to the database! The following error occurred: {e}")
    else:
        print("\nCreating clients table in database...")
        create_table_clients(conn)
        
        print("\nInserting data into the table...")
        insert_df_into_db(conn, df_clients, "clients")
            
        print("\nCreating the transaction_in table in the database...")
        create_table_transactions(conn, "transactions_in")

        print("\nInserting data into the table...")
        insert_df_into_db(conn, df_transactions_in, "transactions_in")
    
        print("\nCreating the transaction_out table in the database...")
        create_table_transactions(conn, "transactions_out")

        print("\nInserting data into the table...")
        insert_df_into_db(conn, df_transactions_out, "transactions_out")
       
    print("\n")
    print("-" * 30)
    print("Transactions in")
    df_transactions_in.show()
    print("-" * 30)
    print("Transactions out")
    df_transactions_out.show()
    print("-" * 30)
    print("Clients")
    df_clients.show()
    
except Exception as e:
    print(f"The following error occurred: {e}!")

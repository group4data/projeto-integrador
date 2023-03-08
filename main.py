from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from functions import transform_csv_to_df

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
    print("Transformando os arquivos CSVs em data frames!")
    df_clients = transform_csv_to_df(spark, clients, columns_clients)
    df_transactions_in = transform_csv_to_df(spark, transactions_in, columns_transactions)
    df_transactions_out = transform_csv_to_df(spark, transactions_out , columns_transactions)

    
    df_clients.show()
    df_transactions_in.show()
    df_transactions_out.show()

except Exception as e:
    print(f"Ocorreu o seguinte erro: {e}!")



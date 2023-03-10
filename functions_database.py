import pyodbc
from pyspark.sql.types import *

def connection_database():
    server_name = 'server'
    database_name = 'banco'
    username = 'user'
    password = 'pwd'

    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server_name},1433;Database={database_name};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(connection_string)

 

def create_table_clients(conn, df):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'clientes'")
    if cursor.fetchone()[0] == 0:
        create_table_query = '''CREATE TABLE clientes (
                                    id INTEGER PRIMARY KEY,
                                    nome VARCHAR(255),
                                    sobrenome VARCHAR(255),
                                    email VARCHAR(255),
                                    data_hora_cadastro DATETIME,
                                    telefone VARCHAR(255),
                                    estado VARCHAR(255)
                                    );'''

        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela clientes criada com sucesso!")
        insert_df_into_db(conn, df, "clientes")
    else:
        print("A tabela clientes já está no banco de dados!")

def create_table_transactions(conn, df, name_table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{name_table}'")
    if cursor.fetchone()[0] == 0:
        create_table_query = f"CREATE TABLE {name_table} (\
                                id INTEGER PRIMARY KEY,\
                                cliente_id INTEGER REFERENCES clientes (id),\
                                valor DECIMAL(10,2),\
                                data_hora DATETIME,\
                            );"

        cursor.execute(create_table_query)
        conn.commit()
        print(f"Tabela {name_table} criada com sucesso!")
        insert_df_into_db(conn, df, name_table)
    else: 
        print(f"A tabela {name_table} já está no banco de dados!")


def insert_df_into_db(conn, df, table_name):
    cursor = conn.cursor()
    print("Inserindo os dados na tabela...")
    try:
        columns = ",".join(df.columns)
        placeholders = ",".join("?" for _ in df.columns)
        df = df.rdd.collect()

        for values in df:
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
            cursor.commit()
        print("Os dados foram inseridos com sucesso na tabela.")
    except Exception as e:
        print(f"Ocorreu um erro ao inserir os dados na tabela: {e}")
        conn.rollback()
    finally:
        cursor.close()








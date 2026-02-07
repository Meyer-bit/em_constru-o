import os
import pandas as pd
import psycopg
from dotenv import load_dotenv
load_dotenv(encoding="utf-8")




# Carregar variáveis do arquivo .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")



# Criar conexão com o banco de dados
def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )




# Criar tabela baseada em um arquivo Parquet
def create_table_from_parquet(table_name, parquet_path):
    """
    Cria uma tabela no banco baseada nas colunas
    e tipos do arquivo .parquet
    """

    df = pd.read_parquet(parquet_path)

    # Mapeamento de tipos pandas -> PostgreSQL
    dtype_mapping = {
        "object": "TEXT",
        "string": "TEXT",
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP"
    }

    columns_sql = []

    for col, dtype in df.dtypes.items():
        sql_type = dtype_mapping.get(str(dtype), "TEXT")
        columns_sql.append(f'"{col}" {sql_type}')

    columns_sql = ",\n".join(columns_sql)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
"""


    conn = get_connection()
    cur = conn.cursor()
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()
    conn.close()



# Carregar dados do Parquet para o banco
def load_parquet_to_db(table_name, parquet_path):
    """
    Insere os dados de um arquivo .parquet
    dentro da tabela do banco
    """

    df = pd.read_parquet(parquet_path)

    conn = get_connection()
    cur = conn.cursor()

    columns = list(df.columns)
    columns_sql = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()


# Carregar todos os Parquets de uma pasta
def load_all_parquets(directory_path):
    """
    Para cada arquivo .parquet:
    - cria a tabela
    - carrega os dados
    """

    for file in os.listdir(directory_path):
        if file.endswith(".parquet"):
            table_name = os.path.splitext(file)[0]
            parquet_path = os.path.join(directory_path, file)

            print(f"📦 Processando {file}...")

            create_table_from_parquet(table_name, parquet_path)
            load_parquet_to_db(table_name, parquet_path)

            print(f"✅ Tabela '{table_name}' criada e dados carregados")



# Execução direta
if __name__ == "__main__":
    PARQUET_DIR = "data/processed"

    load_all_parquets(PARQUET_DIR)
    print("🎉 Todos os arquivos foram carregados no banco!")


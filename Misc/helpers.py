import os, pyodbc
from dotenv import load_dotenv
load_dotenv()
def sql_server_conn():
    cs = (
        f"DRIVER={os.environ['AZURE_SQL_DRIVER']};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
        f"UID={os.environ['AZURE_SQL_USERNAME']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        f"PORT={os.environ['AZURE_SQL_PORT']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(cs, autocommit=False)
    conn.fast_executemany = True
    return conn
def truncate_then_insert(cursor, table, df):
    cursor.execute(f'TRUNCATE TABLE {table};')
    if df.empty:
        return
    ph = ','.join('?' * len(df.columns))
    sql = f'INSERT INTO {table} VALUES ({ph});'
    cursor.executemany(sql, df.itertuples(index=False, name=None))

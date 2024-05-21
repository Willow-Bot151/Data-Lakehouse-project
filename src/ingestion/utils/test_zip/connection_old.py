from pg8000.native import Connection
from config import *

def connect_to_db():
    return Connection(
        user=user,
        password=password,
        database=database,
        host=host,
        port=port
    )

def close_connection(conn):
    conn.close()
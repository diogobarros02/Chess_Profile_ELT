from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_CHESS_ELT", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(cur, conn):    
    cur.close()
    conn.close()

def create_schema(schema):

    conn, cur = get_conn_cursor()
    
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(create_schema_query)
    conn.commit()
    
    close_conn_cursor(cur, conn)
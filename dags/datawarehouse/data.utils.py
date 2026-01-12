from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "chess_api"

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

def create_table(schema):

    conn, cur = get_conn_cursor()
    
    if schema == 'staging':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            player_id BIGINT PRIMARY KEY,
            player_url_id TEXT,
            url TEXT,
            name TEXT,
            username VARCHAR UNIQUE,
            followers INT,
            country_url TEXT,
            last_online TIMESTAMP,
            joined TIMESTAMP,
            status VARCHAR,
            is_streamer BOOLEAN,
            verified BOOLEAN,
            league VARCHAR,
            streaming_platforms JSONB,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    cur.execute(create_table_query)
    conn.commit()
    
    close_conn_cursor(cur, conn)

def get_player_ids(cur, schema):
    cur.execute(f"SELECT player_id FROM {schema}.{table};")
    ids = cur.fetchall()
    player_ids = [row['player_id'] for row in ids]
    return player_ids
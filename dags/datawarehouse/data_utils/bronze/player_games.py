from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor

table = "player_games"

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == 'bronze':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            uuid UUID PRIMARY KEY,
            username VARCHAR NOT NULL,

            game_url TEXT,
            rated BOOLEAN,
            time_class VARCHAR,
            time_control VARCHAR,
            rules VARCHAR,

            start_time TIMESTAMP,
            end_time TIMESTAMP,

            white_username VARCHAR,
            white_rating INT,
            white_result VARCHAR,

            black_username VARCHAR,
            black_rating INT,
            black_result VARCHAR,

            eco TEXT,
            tournament TEXT,

            pgn TEXT,
            fen TEXT,
            initial_setup TEXT,

            extracted_at TIMESTAMP,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        cur.execute(create_table_query)
        conn.commit()

    close_conn_cursor(cur, conn)

def get_game_uuids(cur, schema):
    cur.execute(f"SELECT uuid FROM {schema}.{table};")
    rows = cur.fetchall()
    game_uuids = [row['uuid'] for row in rows]
    return game_uuids
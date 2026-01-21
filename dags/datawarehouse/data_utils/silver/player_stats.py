from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor

table = "player_stats"

def create_table_stats(schema):

    conn, cur = get_conn_cursor()
    
    if schema == 'silver':
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.player_stats (  
            username TEXT PRIMARY KEY,
            -- chess_daily
            chess_daily_last_rating INT,
            chess_daily_last_date TIMESTAMP,
            chess_daily_last_rd INT,
            chess_daily_best_rating INT,
            chess_daily_best_date TIMESTAMP,
            chess_daily_best_game TEXT,
            chess_daily_record_win INT,
            chess_daily_record_loss INT,
            chess_daily_record_draw INT,
            chess_daily_record_time_per_move INT,
            chess_daily_record_timeout_percent INT,
            chess_daily_tournament_points INT,
            chess_daily_tournament_withdraw INT,
            chess_daily_tournament_count INT,
            chess_daily_tournament_highest_finish INT,
            -- chess_rapid
            chess_rapid_last_rating INT,
            chess_rapid_last_date TIMESTAMP,
            chess_rapid_last_rd INT,
            chess_rapid_best_rating INT,
            chess_rapid_best_date TIMESTAMP,
            chess_rapid_best_game TEXT,
            chess_rapid_record_win INT,
            chess_rapid_record_loss INT,
            chess_rapid_record_draw INT,
            -- chess_bullet
            chess_bullet_last_rating INT,
            chess_bullet_last_date TIMESTAMP,
            chess_bullet_last_rd INT,
            chess_bullet_best_rating INT,
            chess_bullet_best_date TIMESTAMP,
            chess_bullet_best_game TEXT,
            chess_bullet_record_win INT,
            chess_bullet_record_loss INT,
            chess_bullet_record_draw INT,
            -- chess_blitz
            chess_blitz_last_rating INT,
            chess_blitz_last_date TIMESTAMP,
            chess_blitz_last_rd INT,
            chess_blitz_best_rating INT,
            chess_blitz_best_date TIMESTAMP,
            chess_blitz_best_game TEXT,
            chess_blitz_record_win INT,
            chess_blitz_record_loss INT,
            chess_blitz_record_draw INT,
            fide INT,
            -- tactics
            tactics_highest_rating INT,
            tactics_highest_date TIMESTAMP,
            tactics_lowest_rating INT,
            tactics_lowest_date TIMESTAMP,
            -- puzzle_rush
            puzzle_rush_best_total_attempts INT,
            puzzle_rush_best_score INT,
            puzzle_rush_daily_total_attempts INT,
            puzzle_rush_daily_score INT,
            fetched_at TIMESTAMP DEFAULT now()
        );
        """
    cur.execute(create_table_query)
    conn.commit()
    
    close_conn_cursor(cur, conn)

def get_player_username(cur, schema):
    cur.execute(f"SELECT username FROM {schema}.{table};")
    user = cur.fetchall()
    player_names = [row['username'] for row in user]
    return player_names

def fetch_bronze_rows_stats(cur, bronze_schema):
    cur.execute(f"""
        SELECT *
        FROM {bronze_schema}.player_stats
    """)
    return cur.fetchall()
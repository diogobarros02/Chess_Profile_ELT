from datawarehouse.data_utils.bronze.player_games import create_table
from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from datawarehouse.data_modification.bronze.player_games import insert_raw_player_games
from datawarehouse.data_loading.bronze.player_games import load_raw_games_from_file


import sys
import os

# Add the 'datawarehouse' folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "player_games"

@task
def bronze_table_player_games():
    schema = 'bronze'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # Ensure schema and table exist
        create_schema(schema)
        create_table(schema)

        # Load JSON instead of calling API
        chess_data = load_raw_games_from_file()
        if not chess_data:
            logger.warning("⚠️ No games data found, skipping insert")
            return

        # chess_data["data"] contains {username: [games]}
        users_games = chess_data.get("data", {})

        for username, games in users_games.items():
            insert_raw_player_games(
                cur=cur,
                conn=conn,
                schema=schema,
                username=username,
                json_file=chess_data  # keep same signature as your insert function
            )

        logger.info(f"✅ Raw/bronze games data inserted successfully into {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Error inserting raw games data into {schema}.{table}: {e}")
        raise

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
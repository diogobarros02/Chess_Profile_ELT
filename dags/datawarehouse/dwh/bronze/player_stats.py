from datawarehouse.data_utils.bronze.player_stats import create_table_stats
from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from datawarehouse.data_modification.bronze.player_stats import insert_raw_row_stats
from datawarehouse.data_loading.bronze.player_stats import load_path_stats

import sys
import os

# Add the 'datawarehouse' folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import logging
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
table = "player_stats"

@task
def bronze_table_stats():
    schema = 'bronze'  # raw/bronze layer
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # Load raw JSON data
        chess_data = load_path_stats()

        # Ensure schema and table exist
        create_schema(schema)
        create_table_stats(schema)b 

        # Insert each row as-is
        for row in chess_data:
            print(row)
            insert_raw_row_stats(cur, conn, schema, row, chess_data)

        logger.info(f"✅ Raw/bronze data inserted successfully into {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Error inserting raw data into {schema}.{table}: {e}")
        raise e

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)

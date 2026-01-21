from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from datawarehouse.data_utils.silver.player_stats import create_table_stats, get_player_username, fetch_bronze_rows_stats
from datawarehouse.data_modification.silver.player_stats import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation.bronze.player_stats import transform_row_stats
import logging
from airflow.decorators import dag, task
from datetime import datetime, timezone

import sys
import os

# Add the 'datawarehouse' folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

logger = logging.getLogger(__name__)
table = "player_stats"

@task
def silver_table_stats():
    schema = "silver"
    bronze_schema = "bronze"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # 1️⃣ Read FROM BRONZE
        bronze_rows = fetch_bronze_rows_stats(cur, bronze_schema)

        # 2️⃣ Transform
        silver_rows = [transform_row_stats(row) for row in bronze_rows]

        # 3️⃣ Ensure schema & table
        create_schema(schema)
        create_table_stats(schema)

        # 4️⃣ UPSERT logic
        existing_username = set(get_player_username(cur, schema))

        incoming_names = set()
        for row in silver_rows:
            username = row["username"]
            incoming_names.add(username)

            if username in existing_username:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)

        # 5️⃣ Delete missing rows
        ids_to_delete = existing_username - incoming_names
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"✅ Silver load completed for {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Silver load failed for {schema}.{table}: {e}")
        raise

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
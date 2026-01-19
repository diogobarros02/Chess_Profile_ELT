from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from datawarehouse.data_utils.silver.player_details import create_table, get_player_ids, fetch_bronze_rows
from datawarehouse.data_modification.silver.player_details import insert_rows, update_rows, delete_rows
from datawarehouse.data_loading.bronze.player_details import load_path
from datawarehouse.data_transformation.bronze.player_details import transform_row
import logging
from airflow.decorators import dag, task
from datetime import datetime, timezone

import sys
import os

# Add the 'datawarehouse' folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

logger = logging.getLogger(__name__)
table = "player_details"

@task
def silver_table():
    schema = "silver"
    bronze_schema = "bronze"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # 1️⃣ Read FROM BRONZE
        bronze_rows = fetch_bronze_rows(cur, bronze_schema)

        # 2️⃣ Transform
        silver_rows = [transform_row(row) for row in bronze_rows]

        # 3️⃣ Ensure schema & table
        create_schema(schema)
        create_table(schema)

        # 4️⃣ UPSERT logic
        existing_ids = set(get_player_ids(cur, schema))

        incoming_ids = set()
        for row in silver_rows:
            incoming_ids.add(row["player_id"])

            if row["player_id"] in existing_ids:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)

        # 5️⃣ Delete missing rows
        ids_to_delete = existing_ids - incoming_ids
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"✅ Silver load completed for {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Silver load failed for {schema}.{table}: {e}")
        raise

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
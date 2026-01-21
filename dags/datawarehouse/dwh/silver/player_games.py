from datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from datawarehouse.data_utils.silver.player_games import (
    create_table,
    get_game_uuids,
    fetch_bronze_rows
)
from datawarehouse.data_modification.silver.player_games import (
    insert_rows,
    update_rows,
    delete_rows
)
from datawarehouse.data_transformation.bronze.player_games import transform_game_row

import logging
from airflow.decorators import task

import sys
import os

# Add the 'datawarehouse' folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

logger = logging.getLogger(__name__)
table = "player_games"

@task
def silver_table_player_games():
    schema = "silver"
    bronze_schema = "bronze"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # 1️⃣ Read FROM BRONZE
        bronze_rows = fetch_bronze_rows(cur, bronze_schema)

        # 2️⃣ Transform
        silver_rows = [transform_game_row(row) for row in bronze_rows]

        # 3️⃣ Ensure schema & table
        create_schema(schema)
        create_table(schema)

        # 4️⃣ UPSERT logic (by UUID)
        existing_uuids = set(get_game_uuids(cur, schema))

        incoming_uuids = set()
        for row in silver_rows:
            game_uuid = row["uuid"]
            incoming_uuids.add(game_uuid)

            if game_uuid in existing_uuids:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)

        # 5️⃣ Delete missing rows (games no longer in bronze)
        uuids_to_delete = existing_uuids - incoming_uuids
        if uuids_to_delete:
            delete_rows(cur, conn, schema, uuids_to_delete)

        logger.info(f"✅ Silver load completed for {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Silver load failed for {schema}.{table}: {e}")
        raise

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)

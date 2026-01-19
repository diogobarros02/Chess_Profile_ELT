from dags.datawarehouse.data_utils.conn import get_conn_cursor, close_conn_cursor, create_schema
from dags.datawarehouse.data_modification.bronze.player_details import insert_raw_row
from datawarehouse.data_loading.bronze.player_details import load_path

import logging
from airflow.decorators import dag, task
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
table = "player_details"

@task
def silver_table():
    schema = 'bronze'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        chess_data = load_path()

        create_schema(schema)
        create_table(schema)

        player_ids = get_player_ids(cur, schema)

        for row in chess_data:
            if len(player_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['player_id'] in player_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)

        # Delete rows that are no longer present in the source
        ids_in_json = {row['player_id'] for row in chess_data}
        ids_to_delete = set(player_ids) - ids_in_json
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"✅ Data warehouse operations completed successfully in {schema}.{table}")

    except Exception as e:
        logger.error(f"❌ Error during data warehouse operations in {schema}.{table}: {e}")
        raise e

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
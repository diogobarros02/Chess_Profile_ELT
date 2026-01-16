from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_player_ids
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_loading import load_path
from datawarehouse.data_transformation import country_mapping, transform_data

import logging
from airflow.decorators import dag, task
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
table = "chess_api"

@task
def staging_table():

    schema = 'staging'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        chess_data = load_path()

        create_schema(schema)
        create_table(schema)

        player_ids = get_player_ids(cur, schema)
        from datetime import datetime


        for row in chess_data:

            row["last_online"] = datetime.fromtimestamp(row["last_online"], tz=timezone.utc)
            row["joined"] = datetime.fromtimestamp(row["joined"], tz=timezone.utc)

            if len(player_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['player_id'] in player_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
        
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
import logging

logger = logging.getLogger(__name__)
table = "chess_api"

def insert_rows(cur,conn,schema,row):
    try:
        if schema == 'staging':
            player_id = 'player_id'
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    player_id,
                    player_url_id,
                    url,
                    name,
                    username,
                    followers,
                    country_url,
                    last_online,
                    joined,
                    status,
                    is_streamer,
                    verified,
                    league,
                    streaming_platforms
                ) VALUES (%(player_id)s, %(@id)s, %(url)s, %(name)s, %(username)s, %(followers)s, %(country)s, %(last_online)s, %(joined)s, %(status)s, %(is_streamer)s, %(verified)s, %(league)s, %(streaming_platforms)s)
                ON CONFLICT (player_id) DO NOTHING;
                """, row
            )
        # put after here if more tables 

        conn.commit()
        logger.info(f"✅ Row with player_id {row[player_id]} inserted successfully into {schema}.{table}")
    except Exception as e:
        logger.error(f"❌ Error inserting row with player_id {row[player_id]} into {schema}.{table}: {e}")
        raise e

def update_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            player_id = 'player_id'
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    player_url_id = %(@id)s,
                    url = %(url)s,
                    name = %(name)s,
                    username = %(username)s,
                    followers = %(followers)s,
                    country_url = %(country)s,
                    last_online = %(last_online)s,
                    joined = %(joined)s,
                    status = %(status)s,
                    is_streamer = %(is_streamer)s,
                    verified = %(verified)s,
                    league = %(league)s,
                    streaming_platforms = %(streaming_platforms)s
                WHERE player_id = %(player_id)s;
                """, row
            )
        # put after here if more tables 

        conn.commit()
        logger.info(f"✅ Row with player_id {row[player_id]} updated successfully in {schema}.{table}")
    except Exception as e:
        logger.error(f"❌ Error updating row with player_id {row[player_id]} in {schema}.{table}: {e}")
        raise e

def delete_rows(cur, conn, schema, player_id):
    try:

        ids_to_delete = f"""({', '.join(str(pid) for pid in player_id)})"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE player_id = %s;
            """, (ids_to_delete)
        )
        # put after here if more tables 

        conn.commit()
        logger.info(f"✅ Row with player_id {player_id} deleted successfully from {schema}.{table}")
    except Exception as e:
        logger.error(f"❌ Error deleting row with player_id {player_id} from {schema}.{table}: {e}")
        raise e
import logging


logger = logging.getLogger(__name__)
table = "player_details"

def insert_raw_row_stats(cur, conn, schema, row):
    try:
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
            )
            VALUES (%(player_id)s, %(@id)s, %(url)s, %(name)s, %(username)s, %(followers)s, %(country)s, %(last_online)s, %(joined)s, %(status)s, %(is_streamer)s, %(verified)s, %(league)s, %(streaming_platforms)s)
            ON CONFLICT (player_id) DO NOTHING;
            """,
            row
        )
        conn.commit()
        logger.info(f"✅ Row with player_id {row['player_id']} inserted successfully into {schema}.{table}")
    except Exception as e:
        logger.error(f"❌ Error inserting row with player_id {row['player_id']} into {schema}.{table}: {e}")
        raise e
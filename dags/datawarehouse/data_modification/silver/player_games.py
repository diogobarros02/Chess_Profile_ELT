import logging

logger = logging.getLogger(__name__)
table = "player_games"

def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'silver':
            game_id_key = 'uuid'

            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    uuid,
                    username,
                    game_url,
                    rated,
                    time_class,
                    time_control,
                    rules,
                    start_time,
                    end_time,
                    white_username,
                    white_rating,
                    white_result,
                    black_username,
                    black_rating,
                    black_result,
                    eco,
                    tournament,
                    pgn,
                    fen,
                    initial_setup,
                    extracted_at
                )
                VALUES (
                    %(uuid)s,
                    %(username)s,
                    %(game_url)s,
                    %(rated)s,
                    %(time_class)s,
                    %(time_control)s,
                    %(rules)s,
                    %(start_time)s,
                    %(end_time)s,
                    %(white_username)s,
                    %(white_rating)s,
                    %(white_result)s,
                    %(black_username)s,
                    %(black_rating)s,
                    %(black_result)s,
                    %(eco)s,
                    %(tournament)s,
                    %(pgn)s,
                    %(fen)s,
                    %(initial_setup)s,
                    %(extracted_at)s
                )
                ON CONFLICT (uuid) DO NOTHING;
                """,
                row
            )

        conn.commit()
        logger.info(
            f"✅ Game {row[game_id_key]} inserted into {schema}.{table}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error inserting game {row.get('uuid')}: {e}"
        )
        raise

def update_rows(cur, conn, schema, row):
    try:
        if schema == 'silver':
            game_id_key = 'uuid'

            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    username = %(username)s,
                    game_url = %(game_url)s,
                    rated = %(rated)s,
                    time_class = %(time_class)s,
                    time_control = %(time_control)s,
                    rules = %(rules)s,
                    start_time = %(start_time)s,
                    end_time = %(end_time)s,
                    white_username = %(white_username)s,
                    white_rating = %(white_rating)s,
                    white_result = %(white_result)s,
                    black_username = %(black_username)s,
                    black_rating = %(black_rating)s,
                    black_result = %(black_result)s,
                    eco = %(eco)s,
                    tournament = %(tournament)s,
                    pgn = %(pgn)s,
                    fen = %(fen)s,
                    initial_setup = %(initial_setup)s,
                    extracted_at = %(extracted_at)s
                WHERE uuid = %(uuid)s;
                """,
                row
            )

        conn.commit()
        logger.info(
            f"✅ Game {row[game_id_key]} updated in {schema}.{table}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error updating game {row.get('uuid')}: {e}"
        )
        raise

def delete_rows(cur, conn, schema, game_uuids):
    try:
        if not game_uuids:
            return

        if not isinstance(game_uuids, (list, tuple)):
            game_uuids = [game_uuids]

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE uuid = ANY(%s);
            """,
            (game_uuids,)
        )

        conn.commit()
        logger.info(
            f"✅ Deleted game(s): {game_uuids} from {schema}.{table}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error deleting game(s) {game_uuids}: {e}"
        )
        raise
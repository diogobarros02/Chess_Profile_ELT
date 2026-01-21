import logging

logger = logging.getLogger(__name__)
table = "player_stats"

def insert_rows(cur,conn,schema,row):
    try:
        if schema == 'silver':
            username = 'username'
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    username,
                    chess_daily_last_rating,
                    chess_daily_last_date,
                    chess_daily_last_rd,
                    chess_daily_best_rating,
                    chess_daily_best_date,
                    chess_daily_best_game,
                    chess_daily_record_win,
                    chess_daily_record_loss,
                    chess_daily_record_draw,
                    chess_daily_record_time_per_move,
                    chess_daily_record_timeout_percent,
                    chess_daily_tournament_points,
                    chess_daily_tournament_withdraw,
                    chess_daily_tournament_count,
                    chess_daily_tournament_highest_finish,
                    chess_rapid_last_rating,
                    chess_rapid_last_date,
                    chess_rapid_last_rd,
                    chess_rapid_best_rating,
                    chess_rapid_best_date,
                    chess_rapid_best_game,
                    chess_rapid_record_win,
                    chess_rapid_record_loss,
                    chess_rapid_record_draw,
                    chess_bullet_last_rating,
                    chess_bullet_last_date,
                    chess_bullet_last_rd,
                    chess_bullet_best_rating,
                    chess_bullet_best_date,
                    chess_bullet_best_game,
                    chess_bullet_record_win,
                    chess_bullet_record_loss,
                    chess_bullet_record_draw,
                    chess_blitz_last_rating,
                    chess_blitz_last_date,
                    chess_blitz_last_rd,
                    chess_blitz_best_rating,
                    chess_blitz_best_date,
                    chess_blitz_best_game,
                    chess_blitz_record_win,
                    chess_blitz_record_loss,
                    chess_blitz_record_draw,
                    fide,
                    tactics_highest_rating,
                    tactics_highest_date,
                    tactics_lowest_rating,
                    tactics_lowest_date,
                    puzzle_rush_best_total_attempts,
                    puzzle_rush_best_score,
                    puzzle_rush_daily_total_attempts,
                    puzzle_rush_daily_score,
               ) VALUES (
                %(username)s,
                %(chess_daily_last_rating)s,
                %(chess_daily_last_date)s,
                %(chess_daily_last_rd)s,
                %(chess_daily_best_rating)s,
                %(chess_daily_best_date)s,
                %(chess_daily_best_game)s,
                %(chess_daily_record_win)s,
                %(chess_daily_record_loss)s,
                %(chess_daily_record_draw)s,
                %(chess_daily_record_time_per_move)s,
                %(chess_daily_record_timeout_percent)s,
                %(chess_daily_tournament_points)s,
                %(chess_daily_tournament_withdraw)s,
                %(chess_daily_tournament_count)s,
                %(chess_daily_tournament_highest_finish)s,
                %(chess_rapid_last_rating)s,
                %(chess_rapid_last_date)s,
                %(chess_rapid_last_rd)s,
                %(chess_rapid_best_rating)s,
                %(chess_rapid_best_date)s,
                %(chess_rapid_best_game)s,
                %(chess_rapid_record_win)s,
                %(chess_rapid_record_loss)s,
                %(chess_rapid_record_draw)s,
                %(chess_bullet_last_rating)s,
                %(chess_bullet_last_date)s,
                %(chess_bullet_last_rd)s,
                %(chess_bullet_best_rating)s,
                %(chess_bullet_best_date)s,
                %(chess_bullet_best_game)s,
                %(chess_bullet_record_win)s,
                %(chess_bullet_record_loss)s,
                %(chess_bullet_record_draw)s,
                %(chess_blitz_last_rating)s,
                %(chess_blitz_last_date)s,
                %(chess_blitz_last_rd)s,
                %(chess_blitz_best_rating)s,
                %(chess_blitz_best_date)s,
                %(chess_blitz_best_game)s,
                %(chess_blitz_record_win)s,
                %(chess_blitz_record_loss)s,
                %(chess_blitz_record_draw)s,
                %(fide)s,
                %(tactics_highest_rating)s,
                %(tactics_highest_date)s,
                %(tactics_lowest_rating)s,
                %(tactics_lowest_date)s,
                %(puzzle_rush_best_total_attempts)s,
                %(puzzle_rush_best_score)s,
                %(puzzle_rush_daily_total_attempts)s,
                %(puzzle_rush_daily_score)s
            )
            ON CONFLICT (username) DO NOTHING;
            """, row
            )
        # put after here if more tables 

        conn.commit()
        logger.info(f"✅ Row with player_id {row['username']} inserted successfully into {schema}.{table}")
    except Exception as e:
        logger.error(f"❌ Error inserting row with player_id {row['username']} into {schema}.{table}: {e}")
        raise e
def update_rows(cur, conn, schema, row):
    try:
        if schema == 'silver':
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    chess_daily_last_rating = %(chess_daily_last_rating)s,
                    chess_daily_last_date = %(chess_daily_last_date)s,
                    chess_daily_last_rd = %(chess_daily_last_rd)s,
                    chess_daily_best_rating = %(chess_daily_best_rating)s,
                    chess_daily_best_date = %(chess_daily_best_date)s,
                    chess_daily_best_game = %(chess_daily_best_game)s,
                    chess_daily_record_win = %(chess_daily_record_win)s,
                    chess_daily_record_loss = %(chess_daily_record_loss)s,
                    chess_daily_record_draw = %(chess_daily_record_draw)s,
                    chess_daily_record_time_per_move = %(chess_daily_record_time_per_move)s,
                    chess_daily_record_timeout_percent = %(chess_daily_record_timeout_percent)s,
                    chess_daily_tournament_points = %(chess_daily_tournament_points)s,
                    chess_daily_tournament_withdraw = %(chess_daily_tournament_withdraw)s,
                    chess_daily_tournament_count = %(chess_daily_tournament_count)s,
                    chess_daily_tournament_highest_finish = %(chess_daily_tournament_highest_finish)s,
                    chess_rapid_last_rating = %(chess_rapid_last_rating)s,
                    chess_rapid_last_date = %(chess_rapid_last_date)s,
                    chess_rapid_last_rd = %(chess_rapid_last_rd)s,
                    chess_rapid_best_rating = %(chess_rapid_best_rating)s,
                    chess_rapid_best_date = %(chess_rapid_best_date)s,
                    chess_rapid_best_game = %(chess_rapid_best_game)s,
                    chess_rapid_record_win = %(chess_rapid_record_win)s,
                    chess_rapid_record_loss = %(chess_rapid_record_loss)s,
                    chess_rapid_record_draw = %(chess_rapid_record_draw)s,
                    chess_bullet_last_rating = %(chess_bullet_last_rating)s,
                    chess_bullet_last_date = %(chess_bullet_last_date)s,
                    chess_bullet_last_rd = %(chess_bullet_last_rd)s,
                    chess_bullet_best_rating = %(chess_bullet_best_rating)s,
                    chess_bullet_best_date = %(chess_bullet_best_date)s,
                    chess_bullet_best_game = %(chess_bullet_best_game)s,
                    chess_bullet_record_win = %(chess_bullet_record_win)s,
                    chess_bullet_record_loss = %(chess_bullet_record_loss)s,
                    chess_bullet_record_draw = %(chess_bullet_record_draw)s,
                    chess_blitz_last_rating = %(chess_blitz_last_rating)s,
                    chess_blitz_last_date = %(chess_blitz_last_date)s,
                    chess_blitz_last_rd = %(chess_blitz_last_rd)s,
                    chess_blitz_best_rating = %(chess_blitz_best_rating)s,
                    chess_blitz_best_date = %(chess_blitz_best_date)s,
                    chess_blitz_best_game = %(chess_blitz_best_game)s,
                    chess_blitz_record_win = %(chess_blitz_record_win)s,
                    chess_blitz_record_loss = %(chess_blitz_record_loss)s,
                    chess_blitz_record_draw = %(chess_blitz_record_draw)s,
                    fide = %(fide)s,
                    tactics_highest_rating = %(tactics_highest_rating)s,
                    tactics_highest_date = %(tactics_highest_date)s,
                    tactics_lowest_rating = %(tactics_lowest_rating)s,
                    tactics_lowest_date = %(tactics_lowest_date)s,
                    puzzle_rush_best_total_attempts = %(puzzle_rush_best_total_attempts)s,
                    puzzle_rush_best_score = %(puzzle_rush_best_score)s,
                    puzzle_rush_daily_total_attempts = %(puzzle_rush_daily_total_attempts)s,
                    puzzle_rush_daily_score = %(puzzle_rush_daily_score)s
                WHERE player_id = %(player_id)s;
                """,
                row
            )

        conn.commit()
        logger.info(
            f"✅ Row with player_id {row['username']} updated successfully in {schema}.{table}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error updating row with player_id {row['username']} in {schema}.{table}: {e}"
        )
        raise

def delete_rows(cur, conn, schema, player_ids):
    try:
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE player_id = ANY(%s);
            """,
            (player_ids,)
        )

        conn.commit()
        logger.info(
            f"✅ Rows with player_id {player_ids} deleted successfully from {schema}.{table}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error deleting rows with player_id {player_ids} from {schema}.{table}: {e}"
        )
        raise 
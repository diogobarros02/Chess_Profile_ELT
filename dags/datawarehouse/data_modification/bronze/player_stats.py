import logging


logger = logging.getLogger(__name__)
table = "player_stats"

def g(d, *keys):
    for k in keys:
        if d is None:
            return None
        d = d.get(k)
    return d


def build_stats_row(username, s):
    return {
        "username": username,
        "daily_last_rating": g(s, "chess_daily", "last", "rating"),
        "daily_last_date": g(s, "chess_daily", "last", "date"),
        "daily_last_rd": g(s, "chess_daily", "last", "rd"),
        "daily_best_rating": g(s, "chess_daily", "best", "rating"),
        "daily_best_date": g(s, "chess_daily", "best", "date"),
        "daily_best_game": g(s, "chess_daily", "best", "game"),
        "daily_win": g(s, "chess_daily", "record", "win"),
        "daily_loss": g(s, "chess_daily", "record", "loss"),
        "daily_draw": g(s, "chess_daily", "record", "draw"),
        "daily_time_per_move": g(s, "chess_daily", "record", "time_per_move"),
        "daily_timeout_percent": g(s, "chess_daily", "record", "timeout_percent"),
        "daily_tournament_points": g(s, "chess_daily", "tournament", "points"),
        "daily_tournament_withdraw": g(s, "chess_daily", "tournament", "withdraw"),
        "daily_tournament_count": g(s, "chess_daily", "tournament", "count"),
        "daily_tournament_highest_finish": g(s, "chess_daily", "tournament", "highest_finish"),
        "rapid_last_rating": g(s, "chess_rapid", "last", "rating"),
        "rapid_last_date": g(s, "chess_rapid", "last", "date"),
        "rapid_last_rd": g(s, "chess_rapid", "last", "rd"),
        "rapid_best_rating": g(s, "chess_rapid", "best", "rating"),
        "rapid_best_date": g(s, "chess_rapid", "best", "date"),
        "rapid_best_game": g(s, "chess_rapid", "best", "game"),
        "rapid_win": g(s, "chess_rapid", "record", "win"),
        "rapid_loss": g(s, "chess_rapid", "record", "loss"),
        "rapid_draw": g(s, "chess_rapid", "record", "draw"),
        "blitz_last_rating": g(s, "chess_blitz", "last", "rating"),
        "blitz_last_date": g(s, "chess_blitz", "last", "date"),
        "blitz_last_rd": g(s, "chess_blitz", "last", "rd"),
        "blitz_best_rating": g(s, "chess_blitz", "best", "rating"),
        "blitz_best_date": g(s, "chess_blitz", "best", "date"),
        "blitz_best_game": g(s, "chess_blitz", "best", "game"),
        "blitz_win": g(s, "chess_blitz", "record", "win"),
        "blitz_loss": g(s, "chess_blitz", "record", "loss"),
        "blitz_draw": g(s, "chess_blitz", "record", "draw"),
        "bullet_last_rating": g(s, "chess_bullet", "last", "rating"),
        "bullet_last_date": g(s, "chess_bullet", "last", "date"),
        "bullet_last_rd": g(s, "chess_bullet", "last", "rd"),
        "bullet_best_rating": g(s, "chess_bullet", "best", "rating"),
        "bullet_best_date": g(s, "chess_bullet", "best", "date"),
        "bullet_best_game": g(s, "chess_bullet", "best", "game"),
        "bullet_win": g(s, "chess_bullet", "record", "win"),
        "bullet_loss": g(s, "chess_bullet", "record", "loss"),
        "bullet_draw": g(s, "chess_bullet", "record", "draw"),
        "fide": s.get("fide"),
        "tactics_highest_rating": g(s, "tactics", "highest", "rating"),
        "tactics_highest_date": g(s, "tactics", "highest", "date"),
        "tactics_lowest_rating": g(s, "tactics", "lowest", "rating"),
        "tactics_lowest_date": g(s, "tactics", "lowest", "date"),
        "puzzle_rush_best_attempts": g(s, "puzzle_rush", "best", "total_attempts"),
        "puzzle_rush_best_score": g(s, "puzzle_rush", "best", "score"),
        "puzzle_rush_daily_attempts": g(s, "puzzle_rush", "daily", "total_attempts"),
        "puzzle_rush_daily_score": g(s, "puzzle_rush", "daily", "score"),
    }

def insert_raw_row_stats(cur, conn, schema, row, chess_data):
    """
    row = username (e.g. 'barrotelli')
    chess_data = full JSON dict
    """
    try:
        stats = chess_data.get(row)
        print(stats)
        if not stats:
            logger.warning(f"⚠️ No stats found for {row}")
            return

        cur.execute(
            f"""
            INSERT INTO {schema}.{table} VALUES (
                %(username)s,
                %(daily_last_rating)s,
                %(daily_last_date)s,
                %(daily_last_rd)s,
                %(daily_best_rating)s,
                %(daily_best_date)s,
                %(daily_best_game)s,
                %(daily_win)s,
                %(daily_loss)s,
                %(daily_draw)s,
                %(daily_time_per_move)s,
                %(daily_timeout_percent)s,
                %(daily_tournament_points)s,
                %(daily_tournament_withdraw)s,
                %(daily_tournament_count)s,
                %(daily_tournament_highest_finish)s,
                %(rapid_last_rating)s,
                %(rapid_last_date)s,
                %(rapid_last_rd)s,
                %(rapid_best_rating)s,
                %(rapid_best_date)s,
                %(rapid_best_game)s,
                %(rapid_win)s,
                %(rapid_loss)s,
                %(rapid_draw)s,
                %(blitz_last_rating)s,
                %(blitz_last_date)s,
                %(blitz_last_rd)s,
                %(blitz_best_rating)s,
                %(blitz_best_date)s,
                %(blitz_best_game)s,
                %(blitz_win)s,
                %(blitz_loss)s,
                %(blitz_draw)s,
                %(bullet_last_rating)s,
                %(bullet_last_date)s,
                %(bullet_last_rd)s,
                %(bullet_best_rating)s,
                %(bullet_best_date)s,
                %(bullet_best_game)s,
                %(bullet_win)s,
                %(bullet_loss)s,
                %(bullet_draw)s,
                %(fide)s,
                %(tactics_highest_rating)s,
                %(tactics_highest_date)s,
                %(tactics_lowest_rating)s,
                %(tactics_lowest_date)s,
                %(puzzle_rush_best_attempts)s,
                %(puzzle_rush_best_score)s,
                %(puzzle_rush_daily_attempts)s,
                %(puzzle_rush_daily_score)s
            )
            ON CONFLICT (username) DO NOTHING;
            """,
            build_stats_row(row, stats)
        )

        conn.commit()
        logger.info(f"✅ Stats inserted for {row}")

    except Exception as e:
        logger.error(f"❌ Error inserting stats for {row}: {e}")
        raise
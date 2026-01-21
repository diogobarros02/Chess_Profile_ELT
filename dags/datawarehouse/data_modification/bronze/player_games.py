import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
table = "player_games"

def g(d, *keys):
    for k in keys:
        if d is None:
            return None
        d = d.get(k)
    return d

def ts_to_utc(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def build_game_row(username: str, game: dict, extracted_at: str):
    return {
        "username": username,
        "game_url": game.get("url"),
        "uuid": game.get("uuid"),
        "rated": game.get("rated"),
        "time_class": game.get("time_class"),
        "time_control": game.get("time_control"),
        "rules": game.get("rules"),

        "start_time": ts_to_utc(game.get("start_time")),
        "end_time": ts_to_utc(game.get("end_time")),

        "white_username": g(game, "white", "username"),
        "white_rating": g(game, "white", "rating"),
        "white_result": g(game, "white", "result"),

        "black_username": g(game, "black", "username"),
        "black_rating": g(game, "black", "rating"),
        "black_result": g(game, "black", "result"),

        "eco": game.get("eco"),
        "tournament": game.get("tournament"),

        "pgn": game.get("pgn"),
        "fen": game.get("fen"),
        "initial_setup": game.get("initial_setup"),

        "extracted_at": extracted_at
    }

def insert_raw_player_games(cur, conn, schema, username, json_file):
    """
    username: 'barrotelli'
    json_file: full JSON dict (year/month/data)
    """
    try:
        extracted_at = json_file.get("extracted_at")
        games = g(json_file, "data", username)

        if not games:
            logger.warning(f"⚠️ No games found for {username}")
            return

        for game in games:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    username,
                    game_url,
                    uuid,
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
                    %(username)s,
                    %(game_url)s,
                    %(uuid)s,
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
                build_game_row(username, game, extracted_at)
            )

        conn.commit()
        logger.info(f"✅ Games inserted for {username}")

    except Exception as e:
        logger.error(f"❌ Error inserting games for {username}: {e}")
        raise
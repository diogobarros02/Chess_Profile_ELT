from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import json
import requests
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging
logger = logging.getLogger(__name__)
@task
def extract_usernames_from_db():
    """
    Reads usernames from player_details table
    """
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_CHESS_ELT")

    records = hook.get_records(
        """
        SELECT username
        FROM bronze.player_details
        WHERE username IS NOT NULL
        """
    )

    # records = [('barrotelli',), ('sousa16',)]
    usernames = [row[0] for row in records]


@task
def extract_usernames_from_db_for_games():
    """
    Reads usernames from player_details table
    """
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_CHESS_ELT")

    records = hook.get_records(
        """
        SELECT username
        FROM bronze.player_details
        WHERE username IS NOT NULL
        """
    )

    # records = [('barrotelli',), ('sousa16',)]
    usernames = [row[0] for row in records]

    return usernames

@task
def fetch_current_month_games(usernames: list[str]) -> dict:
        """
        Fetches finished games for the current month per user
        """
        headers = {"User-Agent": "ChessDataPipeline/1.0"}
        now = datetime.now(timezone.utc)
        year = now.year
        month = f"{now.month:02d}"

        all_games = {}

        for username in usernames:
            url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month}"

            try:
                response = requests.get(url, headers=headers, timeout=30)

                if response.status_code == 404:
                    logger.info(f"No games for {username} in {year}-{month}")
                    all_games[username] = []
                    continue

                response.raise_for_status()
                data = response.json()

                games = data.get("games", [])
                all_games[username] = games

                logger.info(
                    f"{username}: fetched {len(games)} games for {year}-{month}"
                )

            except Exception as e:
                logger.error(f"Failed fetching games for {username}: {e}")
                raise

        return {
            "year": year,
            "month": month,
            "extracted_at": now.isoformat(),
            "data": all_games,
        }

@task
def save_raw_games(payload: dict) -> str:
        """
        Saves raw games JSON for the current month
        """
        year = payload["year"]
        month = payload["month"]

        base_path = "/opt/airflow/data/chess_player_games"
        os.makedirs(base_path, exist_ok=True)

        file_path = f"{base_path}/games_{year}_{month}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)

        logger.info(f"Saved raw games to {file_path}")
        return file_path


@task
def save_raw_stats(stats: dict):

    os.makedirs("/opt/airflow/data", exist_ok=True)

    file_path = f"/opt/airflow/data/chess_player_stats/{datetime.now().date()}.json"

    with open(file_path , "w", encoding="utf-8") as json_file:
        json.dump(stats, json_file, ensure_ascii=False, indent=4)
    return file_path


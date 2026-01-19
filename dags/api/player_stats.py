from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import json
import requests
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

@task
def extract_usernames_from_db():
    """
    Reads usernames from player_details table
    """
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_CHESS_ELT")

    records = hook.get_records(
        """
        SELECT username
        FROM staging.player_details
        WHERE username IS NOT NULL
        """
    )

    # records = [('barrotelli',), ('sousa16',)]
    usernames = [row[0] for row in records]

    return usernames

@task
def fetch_player_stats(usernames: list[str]):
    """
    Fetches player stats from chess.com API for given usernames
    """
    headers = {"User-Agent": "ChessDataPipeline/1.0"}
    all_stats = {}

    for username in usernames:
        url = f"https://api.chess.com/pub/player/{username}/stats"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        all_stats[username] = response.json()

    return all_stats

@task
def save_raw_stats(stats: dict):

    os.makedirs("/opt/airflow/data", exist_ok=True)

    file_path = f"/opt/airflow/data/chess_player_stats_{datetime.now().date()}.json"

    with open(file_path , "w", encoding="utf-8") as json_file:
        json.dump(stats, json_file, ensure_ascii=False, indent=4)
    return file_path


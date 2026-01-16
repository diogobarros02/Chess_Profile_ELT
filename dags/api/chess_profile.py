import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import date

from airflow.decorators import dag, task
from airflow.models import Variable

USERNAMES = Variable.get(
    "chess_usernames",
    default_var="barrotelli,sousa16"
).split(",")

@task
def player_details(usernames: list[str]):
    headers = {"User-Agent": "ChessDataPipeline/1.0"}
    players = []

    for username in usernames:
        url = f"https://api.chess.com/pub/player/{username}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        players.append(response.json())

    return players


@task
def save_to_json(players: list[dict]):
    os.makedirs("/opt/airflow/data", exist_ok=True)

    file_path = f"/opt/airflow/data/chess_players_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(players, json_file, ensure_ascii=False, indent=4)

    return file_path



if __name__ == "__main__":
    load_dotenv()

    os.makedirs("./data", exist_ok=True)

    try:
        data = player_details(username)
        save_to_json(data, username)
        print(f"✅ Data for {username} saved successfully")
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error: {e}")
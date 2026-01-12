import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import date

from airflow.decorators import dag, task
from airflow.models import Variable

USERNAME = Variable.get("chess_username", default_var="barrotelli")

@task
def player_details():
    url = f"https://api.chess.com/pub/player/{USERNAME}"
    headers = {"User-Agent": "ChessDataPipeline/1.0"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


@task
def save_to_json(player_data):
    os.makedirs("/opt/airflow/data", exist_ok=True)

    file_path = f"/opt/airflow/data/{USERNAME}_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(player_data, json_file, ensure_ascii=False, indent=4)



if __name__ == "__main__":
    load_dotenv()

    username = "barrotelli"

    os.makedirs("./data", exist_ok=True)

    try:
        data = player_details(username)
        save_to_json(data, username)
        print(f"✅ Data for {username} saved successfully")
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error: {e}")
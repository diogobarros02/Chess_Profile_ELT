import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import date

def player_details(username):
    BASE_URL = f"https://api.chess.com/pub/player/{username}"

    headers = {
        "User-Agent": "Mozilla/5.0 (ChessDataPipeline/1.0)"
    }

    response_data = requests.get(BASE_URL, headers=headers)

    if response_data.status_code != 200:
        raise Exception(
            f"Failed to fetch data for user {username}: {response_data.status_code}"
        )

    return response_data.json()



def save_to_json(response_data, username):
    file_path = f"./data/{username}_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(response_data, json_file, ensure_ascii=False, indent=4)



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
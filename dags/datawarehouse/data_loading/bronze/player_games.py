import requests
import logging
from datetime import datetime, timezone
import json
import os 
logger = logging.getLogger(__name__)

def load_raw_games_from_file() -> dict:
    """
    Loads the raw games JSON file already saved for the current month.
    """
    base_path = "/opt/airflow/data/chess_player_games"
    now = datetime.now()
    file_path = f"{base_path}/games_{now.year}_{now.month:02d}.json"

    if not os.path.exists(file_path):
        logger.error(f"❌ File not found: {file_path}")
        return {}

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        logger.info(f"✅ Loaded raw games from {file_path}")
        return data
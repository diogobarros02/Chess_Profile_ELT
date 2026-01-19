import json
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)

def load_path_stats():
    file_path = f"/opt/airflow/data/chess_player_stats/{datetime.now().date()}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
            logger.info(f"✅ Data loaded successfully from {file_path}")
            return data
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"❌ Error decoding JSON from file: {file_path}")
        return None
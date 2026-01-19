import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_path():
    file_path = f"/opt/airflow/data/chess_players_{date.today()}.json"
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
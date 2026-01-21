import json
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def transform_game_row(row: dict) -> dict:
    """
    Applies all transformations to a single row:
    1. Maps Chess.com country URL to full country name.
    2. Converts timestamps (last_online, joined) to UTC datetime.
    3. Serializes dict fields to JSON.
    """

    # --- Country mapping ---
    country_url = row.get("country_url") or row.get("country")

    common_countries = {
        "PT": "Portugal",
        "US": "United States",
        "RU": "Russia",
        "IN": "India",
        "CN": "China",
        "DE": "Germany",
        "FR": "France",
        "GB": "United Kingdom",
        "BR": "Brazil",
        "ES": "Spain",
    }

    if country_url:
        country_code = country_url.rstrip("/").split("/")[-1].upper()
        row["country"] = common_countries.get(country_code)
    else:
        row["country"] = None

    # --- Timestamp transformation ---
    if row.get("last_online") is not None:
        row["last_online"] = datetime.fromtimestamp(
            int(row["last_online"]), tz=timezone.utc
        )

    if row.get("joined") is not None:
        row["joined"] = datetime.fromtimestamp(
            int(row["joined"]), tz=timezone.utc
        )

    # --- Dict → JSON ---
    if isinstance(row.get("streaming_platforms"), dict):
        row["streaming_platforms"] = json.dumps(row["streaming_platforms"])

    return row
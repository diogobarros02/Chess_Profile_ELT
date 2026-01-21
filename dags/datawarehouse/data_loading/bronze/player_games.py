import requests
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE_URL = "https://api.chess.com/pub/player"


def load_current_month_games(username: str) -> list[dict]:
    """
    Loads raw games for the current month only.
    Safe to re-run multiple times (idempotent when combined with DB constraints).
    """

    now = datetime.now(timezone.utc)
    year = now.year
    month = f"{now.month:02d}"

    url = f"{BASE_URL}/{username}/games/{year}/{month}"

    try:
        logger.info(f"📥 Fetching current month games for {username} ({year}-{month})")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        games = data.get("games", [])

        logger.info(f"✅ Retrieved {len(games)} games for {username} ({year}-{month})")
        return games

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            # Chess.com returns 404 when there are no games yet
            logger.info(f"ℹ️ No games yet for {username} ({year}-{month})")
            return []
        logger.error(f"❌ HTTP error fetching games: {e}")
        raise

    except Exception as e:
        logger.error(f"❌ Unexpected error fetching games: {e}")
        raise
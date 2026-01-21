from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def transform_row_stats(row: dict) -> dict:
    """
    Transforms numeric timestamp fields in player_stats rows
    from Unix epoch (seconds) to UTC datetime.
    """

    date_fields = [
        "chess_daily_last_date",
        "chess_daily_best_date",
        "chess_rapid_last_date",
        "chess_rapid_best_date",
        "chess_bullet_last_date",
        "chess_bullet_best_date",
        "chess_blitz_last_date",
        "chess_blitz_best_date",
        "tactics_highest_date",
        "tactics_lowest_date"
        
    ]

    for field in date_fields:
        value = row.get(field)

        if value is not None:
            try:
                row[field] = datetime.fromtimestamp(
                    int(value), tz=timezone.utc
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Failed to convert %s=%s to datetime: %s",
                    field, value, e
                )
                row[field] = None

    return row
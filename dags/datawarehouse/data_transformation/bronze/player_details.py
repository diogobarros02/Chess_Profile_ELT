from datetime import datetime, timezone

def transform_row(row: dict) -> dict:
    """
    Applies all transformations to a single row:
    1. Maps country URL to full country name.
    2. Converts timestamps (last_online, joined) to UTC datetime.
    """
    # --- Country mapping ---
    country_url = row.get("country_url", "")
    if country_url:
        country_code = country_url.split("/")[-1].upper()
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
            "ES": "Spain"
        }
        row["country"] = common_countries.get(country_code, country_code)
    else:
        row["country"] = None

    # --- Timestamp transformation ---
    if "last_online" in row and row["last_online"] is not None:
        row["last_online"] = datetime.fromtimestamp(row["last_online"], tz=timezone.utc)
    if "joined" in row and row["joined"] is not None:
        row["joined"] = datetime.fromtimestamp(row["joined"], tz=timezone.utc)

    return row

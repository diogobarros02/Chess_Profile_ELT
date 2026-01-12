def country_mapping(country_url: str) -> str | None:
    """
    Converts Chess.com country URL to full country name for the most common countries.
    If country is not in the mapping, returns the original country code.
    """
    if not country_url:
        return None

    # Extract the country code from URL
    country_code = country_url.split("/")[-1].upper()

    # Manual mapping for most common countries
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

    # Return mapped name if exists, else the original code
    return common_countries.get(country_code, country_code)


def transform_data(row):
   complete_country = country_mapping(row.get("country_url", ""))
   row["country"] = complete_country
   return row

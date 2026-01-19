import requests
import json

# Replace with your usernames
USERNAMES = ["barrotelli"]

HEADERS = {"User-Agent": "ChessDataPipeline/1.0"}

def fetch_player_profile(username: str) -> dict:
    """Fetch basic player profile data."""
    url = f"https://api.chess.com/pub/player/{username}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print(f"Failed to fetch profile for {username}: {r.status_code}")
        return {}
    return r.json()

def fetch_player_stats(username: str) -> dict:
    """Fetch stats for each time control (rapid, blitz, bullet, daily)."""
    url = f"https://api.chess.com/pub/player/{username}/stats"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print(f"Failed to fetch stats for {username}: {r.status_code}")
        return {}
    return r.json()

def fetch_monthly_games(username: str, year: int, month: int) -> dict:
    """Fetch all games for a given month."""
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print(f"Failed to fetch games for {username} {year}-{month:02d}: {r.status_code}")
        return {}
    return r.json()

# --- MAIN EXPLORATION ---
for username in USERNAMES:
    print(f"\n===== {username.upper()} =====\n")
    
    profile = fetch_player_profile(username)
    print("Profile:")
    print(json.dumps(profile, indent=2))
    
    stats = fetch_player_stats(username)
    print("\nStats:")
    print(json.dumps(stats, indent=2))
    
    # Fetch last 2 months of games for example
    from datetime import datetime
    today = datetime.today()
    for m_offset in range(2):
        year = today.year
        month = today.month - m_offset
        if month <= 0:
            month += 12
            year -= 1
        games = fetch_monthly_games(username, year, month)
        print(f"\nGames {year}-{month:02d}:")
        print(json.dumps(games, indent=2))
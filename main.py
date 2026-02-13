import os
import requests
import sqlite3
import json
import time
from datetime import datetime

# --- CONFIGURATION ---
DB_PATH = "nhldb.db"
BASE_DATA_DIR = "data"

def get_past_game_ids():
    """Queries the database for all game IDs that have already taken place."""
    today = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # We only care about regular season (2) or playoff (3) games that are in the past
        cursor.execute("""
            SELECT game_id FROM schedules
            WHERE game_date < ? AND game_type IN (2, 3)
            ORDER BY game_date ASC
        """, (today,))
        return [row[0] for row in cursor.fetchall()]

def archive_play_by_play(game_id):
    """Fetches and saves the raw JSON if it doesn't already exist locally."""
    folder_path = os.path.join(BASE_DATA_DIR, "play_by_play", str(game_id))
    os.makedirs(folder_path, exist_ok=True)

    # We'll name the file based on the game_id for easy reference
    filepath = os.path.join(folder_path, f"{game_id}_raw.json")

    # Skip if we already have it
    if os.path.exists(filepath):
        return "Exists"

    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        return "Downloaded"
    except Exception as e:
        return f"Error: {e}"

def mass_download():
    print(f"Starting mass archive at {datetime.now()}")

    game_ids = get_past_game_ids()
    total = len(game_ids)
    print(f"Found {total} past games to check.")

    downloaded = 0
    skipped = 0
    errors = 0

    for i, gid in enumerate(game_ids, 1):
        result = archive_play_by_play(gid)

        if result == "Downloaded":
            downloaded += 1
            # Be polite to the API since we are doing a massive pull
            time.sleep(0.5)
        elif result == "Exists":
            skipped += 1
        else:
            print(f"\n{result} for Game {gid}")
            errors += 1

        # Simple progress tracker
        if i % 10 == 0 or i == total:
            print(f"Progress: {i}/{total} | New: {downloaded} | Skipped: {skipped} | Errors: {errors}", end="\r")

    print(f"\n\nArchive Complete!")
    print(f"Total processed: {total}")
    print(f"Files newly downloaded: {downloaded}")

if __name__ == "__main__":
    mass_download()

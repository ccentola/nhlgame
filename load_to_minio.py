import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)

BUCKET = "nhl-raw"
LOCAL_DATA_PATH = "data"

def upload_file(local_path: str, s3_key: str):
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=open(local_path, "rb"),
        ContentType="application/json",
    )
    print(f"Uploaded: {local_path} -> s3://{BUCKET}/{s3_key}")

def upload_teams():
    teams_dir = os.path.join(LOCAL_DATA_PATH, "teams")
    for filename in os.listdir(teams_dir):
        if filename.endswith(".json"):
            upload_file(os.path.join(teams_dir, filename), f"teams/{filename}")

def upload_players():
    players_dir = os.path.join(LOCAL_DATA_PATH, "players")
    for team_abbrev in os.listdir(players_dir):
        team_path = os.path.join(players_dir, team_abbrev)
        if os.path.isdir(team_path):
            for filename in os.listdir(team_path):
                if filename.endswith(".json"):
                    upload_file(os.path.join(team_path, filename), f"players/{team_abbrev}/{filename}")

def upload_schedules():
    schedules_dir = os.path.join(LOCAL_DATA_PATH, "schedules")
    for team_abbrev in os.listdir(schedules_dir):
        team_path = os.path.join(schedules_dir, team_abbrev)
        if os.path.isdir(team_path):
            for filename in os.listdir(team_path):
                if filename.endswith(".json"):
                    upload_file(os.path.join(team_path, filename), f"schedules/{team_abbrev}/{filename}")

def upload_play_by_play():
    pbp_dir = os.path.join(LOCAL_DATA_PATH, "play-by-play")
    for game_id in os.listdir(pbp_dir):
        game_path = os.path.join(pbp_dir, game_id)
        if os.path.isdir(game_path):
            for filename in os.listdir(game_path):
                if filename.endswith(".json"):
                    upload_file(os.path.join(game_path, filename), f"play-by-play/{game_id}/{filename}")

if __name__ == "__main__":
    upload_teams()
    upload_players()
    upload_schedules()
    upload_play_by_play()

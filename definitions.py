import json
import os
import time
from datetime import date

import boto3
import requests
from dagster import AssetExecutionContext, Definitions, asset
from dagster import ConfigurableResource


class MinIOResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "nhl-raw"

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def upload(self, key: str, data: bytes):
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType="application/json",
        )

    def exists(self, key: str) -> bool:
        try:
            self.client().head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def latest_key(self, prefix: str, suffix: str) -> str:
        """Find the most recently created key matching prefix/.../suffix."""
        paginator = self.client().get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    keys.append(obj["Key"])
        if not keys:
            raise ValueError(f"No keys found under {prefix} ending with {suffix}")
        return sorted(keys)[-1]  # lexicographic sort works since dates are ISO format


@asset
def standings(context: AssetExecutionContext, minio: MinIOResource):
    """Fetch current NHL standings and store dated snapshot in MinIO."""
    today = date.today().isoformat()

    key = f"standings/{today}/standings.json"

    if minio.exists(key):
        context.log.info(f"Standings snapshot for {today} already exists, skipping fetch")
    else:
        url = "https://api-web.nhle.com/v1/standings/now"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        minio.upload(key, json.dumps(data).encode())
        context.log.info(f"Uploaded standings snapshot for {today}")

    # Always read from MinIO to return team abbreviations for downstream assets
    data = json.loads(
        minio.client()
            .get_object(Bucket=minio.bucket, Key=key)["Body"]
            .read()
    )
    team_abbrevs = [s["teamAbbrev"]["default"] for s in data["standings"]]
    context.log.info(f"Found {len(team_abbrevs)} teams")
    return team_abbrevs


@asset(deps=[standings])
def rosters(context: AssetExecutionContext, minio: MinIOResource):
    """Fetch current roster for every NHL team and store dated snapshot in MinIO."""
    today = date.today().isoformat()

    standings_key = minio.latest_key("standings/", "standings.json")
    standings_data = json.loads(
        minio.client()
            .get_object(Bucket=minio.bucket, Key=standings_key)["Body"]
            .read()
    )
    team_abbrevs = [s["teamAbbrev"]["default"] for s in standings_data["standings"]]

    uploaded = 0
    skipped = 0
    for abbrev in team_abbrevs:
        key = f"rosters/{abbrev}/{today}/roster.json"

        if minio.exists(key):
            skipped += 1
            continue

        url = f"https://api-web.nhle.com/v1/roster/{abbrev}/current"
        response = requests.get(url)
        response.raise_for_status()

        minio.upload(key, json.dumps(response.json()).encode())
        uploaded += 1
        context.log.info(f"Uploaded roster snapshot for {abbrev} on {today}")
        time.sleep(0.5)

    context.log.info(f"Uploaded {uploaded} roster snapshots, skipped {skipped} already ingested for {today}")
    return team_abbrevs


@asset(deps=[standings])
def schedules(context: AssetExecutionContext, minio: MinIOResource):
    """Fetch full season schedule for every NHL team and store in MinIO."""
    standings_key = minio.latest_key("standings/", "standings.json")
    standings_data = json.loads(
        minio.client()
            .get_object(Bucket=minio.bucket, Key=standings_key)["Body"]
            .read()
    )
    team_abbrevs = [s["teamAbbrev"]["default"] for s in standings_data["standings"]]

    uploaded = 0
    for abbrev in team_abbrevs:
        url = f"https://api-web.nhle.com/v1/club-schedule-season/{abbrev}/now"
        response = requests.get(url)
        response.raise_for_status()

        key = f"schedules/{abbrev}/schedule.json"
        minio.upload(key, json.dumps(response.json()).encode())
        uploaded += 1
        context.log.info(f"Uploaded schedule for {abbrev}")
        time.sleep(0.5)

    context.log.info(f"Uploaded schedules for {uploaded} teams")
    return team_abbrevs


@asset(deps=[schedules])
def play_by_play(context: AssetExecutionContext, minio: MinIOResource):
    """Fetch play-by-play for all completed games across all teams."""
    game_ids = set()

    s3 = minio.client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=minio.bucket, Prefix="schedules/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("schedule.json"):
                continue
            data = json.loads(
                s3.get_object(Bucket=minio.bucket, Key=obj["Key"])["Body"].read()
            )
            for game in data.get("games", []):
                if game.get("gameState") in ("OFF", "FINAL"):
                    game_ids.add(game["id"])

    context.log.info(f"Found {len(game_ids)} unique completed games")

    fetched = 0
    skipped = 0
    for game_id in sorted(game_ids):
        key = f"play-by-play/{game_id}/play-by-play.json"
        if minio.exists(key):
            skipped += 1
            continue

        url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
        response = requests.get(url)
        response.raise_for_status()

        minio.upload(key, json.dumps(response.json()).encode())
        fetched += 1
        time.sleep(0.3)

    context.log.info(f"Fetched {fetched} new games, skipped {skipped} already ingested")


defs = Definitions(
    assets=[standings, rosters, schedules, play_by_play],
    resources={
        "minio": MinIOResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", ""),
            secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        )
    }
)

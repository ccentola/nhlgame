
import os
import boto3
import json
import requests
from dagster import ConfigurableResource, Definitions, asset, AssetExecutionContext

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

@asset
def standings(context: AssetExecutionContext, minio: MinIOResource):
    """Fetch current NHL standings and store in data lake"""
    url = "https://api-web.nhle.com/v1/standings/now"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    key = "standings/standings.json"
    minio.upload(key, json.dumps(data).encode())
    context.log.info(f"Uploaded {key} with {len(data.get('standings', []))} teams")

    # Return list of team abbreviations for downstream assets
    return [s["teamAbbrev"]["default"] for s in data["standings"]]


defs = Definitions(
    assets=[standings],
    resources={
        "minio": MinIOResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", ""),
            secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        )
    }
)

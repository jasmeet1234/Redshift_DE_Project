from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import boto3

from src.common.settings import Settings


logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, bucket: str, prefix: str, region: str):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.region = region
        self._client = boto3.client("s3", region_name=region)

    @classmethod
    def from_settings(cls, settings: Settings) -> "S3Client":
        return cls(
            bucket=settings.s3.bucket,
            prefix=settings.s3.prefix,
            region=settings.s3.region,
        )

    def upload_file(self, path: Path) -> str:
        """
        Upload a local file to S3.

        Returns the S3 key.
        """
        if not path.exists():
            raise FileNotFoundError(path)

        key = f"{self.prefix}/{path.name}"

        logger.info("Uploading to s3://%s/%s", self.bucket, key)
        self._client.upload_file(str(path), self.bucket, key)

        return key
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.client import Config


class S3FileSystem:
    def __init__(self):
        self.access_key = os.getenv("R2_ACCESS_KEY_ID")
        self.secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
        self.endpoint = os.getenv("R2_ENDPOINT", "").rstrip("/")
        if self.endpoint and "//" not in self.endpoint:
            self.endpoint = f"https://{self.endpoint}"
        self.region = os.getenv("R2_REGION", "auto")

        self.s3 = None
        if self.access_key and self.secret_key and self.endpoint:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name=self.region,
            )

    def _parse_s3_path(self, path: str) -> tuple[str, str]:
        parsed = urlparse(path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    def exists(self, path: str) -> bool:
        if not self.s3:
            return False
        bucket, key = self._parse_s3_path(path)
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            # Check if it's a prefix
            if not key.endswith("/"):
                key += "/"
            try:
                response = self.s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return "Contents" in response or "CommonPrefixes" in response
            except Exception:
                return False

    def list_status(self, path: str) -> list[str]:
        if not self.s3:
            return []
        bucket, key = self._parse_s3_path(path)
        if key and not key.endswith("/"):
            key += "/"
        
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=key, Delimiter="/")
            items = []
            # Files
            for obj in response.get("Contents", []):
                name = obj["Key"].split("/")[-1]
                if name: # Avoid the directory key itself
                    items.append(obj["Key"])
            # Directories
            for prefix in response.get("CommonPrefixes", []):
                items.append(prefix["Prefix"])
            return items
        except Exception:
            return []

    def delete(self, path: str, recursive: bool = True) -> None:
        if not self.s3:
            return
        bucket, key = self._parse_s3_path(path)
        
        if not recursive:
            self.s3.delete_object(Bucket=bucket, Key=key)
        else:
            # Delete all objects with the prefix
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=key):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    self.s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

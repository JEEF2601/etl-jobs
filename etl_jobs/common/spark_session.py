from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    """Crea una SparkSession con soporte para S3A/R2, cluster remoto y paquetes configurables via env vars."""
    spark_master_url = os.getenv("SPARK_MASTER_URL", "").strip()
    spark_packages = os.getenv("SPARK_PACKAGES", "").strip()

    builder = SparkSession.builder.appName(app_name)
    if spark_master_url:
        builder = builder.master(spark_master_url)
    if spark_packages:
        builder = builder.config("spark.jars.packages", spark_packages)

    r2_access_key = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    r2_endpoint_raw = os.getenv("R2_ENDPOINT", "").strip()

    if r2_access_key and r2_secret_key and r2_endpoint_raw:
        endpoint = r2_endpoint_raw.rstrip("/")
        if "//" not in endpoint:
            endpoint = f"https://{endpoint}"
        ssl_enabled = "true" if endpoint.startswith("https://") else "false"
        region = os.getenv("R2_REGION", "auto")

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.access.key", r2_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", r2_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        )

    return builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()

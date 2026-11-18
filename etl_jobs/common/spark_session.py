from __future__ import annotations

import os

from pyspark.sql import SparkSession

def _env(name: str, default: str = "") -> str:
    """Lee una variable de entorno y la devuelve sin espacios."""
    return os.getenv(name, default).strip()


def build_spark_session(app_name: str) -> SparkSession:
    """Crea una SparkSession (modo clásico) con soporte S3A/R2.

    - Usa SPARK_MASTER_URL para conectarse al cluster standalone; si no está
      definida, Spark corre en modo local.
    - Spark Connect ya no se usa: SPARK_REMOTE / SPARK_REMOTE_URL se ignoran.
    - SPARK_JARS / SPARK_PACKAGES permiten añadir dependencias.
    """
    spark_master_url = _env("SPARK_MASTER_URL")
    spark_jars = _env("SPARK_JARS")
    spark_packages = _env("SPARK_PACKAGES")

    builder = SparkSession.builder.appName(app_name)

    if spark_master_url:
        builder = builder.master(spark_master_url)

        # Los executors deben poder conectar de vuelta al driver.
        # El contenedor de Dagster está en varias redes Docker, así que se
        # escucha en todas las interfaces.
        builder = builder.config("spark.driver.bindAddress", "0.0.0.0")

        # Opcionales: solo se fijan si se definen. Si no, Spark usa el hostname
        # del contenedor y puertos libres (permite runs concurrentes).
        for key, env_name in (
            ("spark.driver.host", "SPARK_DRIVER_HOST"),
            ("spark.driver.port", "SPARK_DRIVER_PORT"),
            ("spark.driver.blockManager.port", "SPARK_DRIVER_BLOCKMANAGER_PORT"),
        ):
            value = _env(env_name)
            if value:
                builder = builder.config(key, value)

    if spark_jars:
        builder = builder.config("spark.jars", spark_jars)
    elif spark_packages:
        builder = builder.config("spark.jars.packages", spark_packages)

    # Configurar recursos de los executors
    builder = builder \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memoryOverhead", "1g")

    r2_access_key = _env("R2_ACCESS_KEY_ID")
    r2_secret_key = _env("R2_SECRET_ACCESS_KEY")
    r2_endpoint_raw = _env("R2_ENDPOINT")

    if r2_access_key and r2_secret_key and r2_endpoint_raw:
        endpoint = r2_endpoint_raw.rstrip("/")
        if "//" not in endpoint:
            endpoint = f"https://{endpoint}"
        ssl_enabled = "true" if endpoint.startswith("https://") else "false"
        region = _env("R2_REGION", "auto")

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
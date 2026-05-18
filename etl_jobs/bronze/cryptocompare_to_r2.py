from __future__ import annotations

from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from etl_jobs.common.partition_writer import overwrite_partitioned_dataset
from etl_jobs.common.spark_session import build_spark_session
from etl_jobs.common.utils import get_env


def _fetch_cryptocompare_data() -> list[dict[str, Any]]:
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        "fsym": "BTC",
        "tsym": "USD",
        "limit": "6",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if payload.get("Response") == "Error":
        message = payload.get("Message", "Unknown error from CryptoCompare")
        raise RuntimeError(f"CryptoCompare API error: {message}")

    data = payload.get("Data", {})
    rows = data.get("Data", []) if isinstance(data, dict) else []
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected CryptoCompare response format.")

    return rows


def _schema() -> StructType:
    return StructType(
        [
            StructField("time", LongType(), True),
            StructField("close", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("volumefrom", DoubleType(), True),
            StructField("volumeto", DoubleType(), True),
            StructField("conversionType", StringType(), True),
            StructField("conversionSymbol", StringType(), True),
        ]
    )


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    numeric_fields = ("close", "high", "low", "open", "volumefrom", "volumeto")

    for row in rows:
        normalized_row: dict[str, Any] = {
            "time": _safe_int(row.get("time")),
            "conversionType": (
                str(row.get("conversionType"))
                if row.get("conversionType") is not None
                else None
            ),
            "conversionSymbol": (
                str(row.get("conversionSymbol"))
                if row.get("conversionSymbol") is not None
                else None
            ),
        }

        for field in numeric_fields:
            normalized_row[field] = _safe_float(row.get(field))

        normalized_rows.append(normalized_row)

    return normalized_rows


def _prepare_frames(spark: SparkSession, rows: list[dict[str, Any]]):
    normalized_rows = _normalize_rows(rows)
    if not normalized_rows:
        return None, None

    df_raw = spark.createDataFrame(normalized_rows, schema=_schema()).withColumn(
        "fecha", F.from_unixtime(F.col("time")).cast("date")
    )

    df_silver = (
        df_raw.select(
            F.col("fecha"),
            F.col("close").alias("precio_cierre"),
            F.col("high").alias("maximo"),
            F.col("low").alias("minimo"),
            F.col("volumefrom").alias("volumen_btc"),
        )
        .dropna(subset=["fecha"])
        .dropDuplicates(["fecha"])
    )

    return df_raw, df_silver


def _target_paths() -> tuple[str, str]:
    bucket = get_env("R2_BUCKET")
    base_prefix = "btc-lakehouse-bucket"
    base_path = f"s3a://{bucket}/{base_prefix}/"
    return f"{base_path}bronze/", f"{base_path}silver/"


def main() -> None:
    spark = None
    try:
        spark = build_spark_session("cryptocompare_to_r2_etl")
        rows = _fetch_cryptocompare_data()

        if not rows:
            print("No rows returned from CryptoCompare. Skipping write.")
            return

        df_raw, df_silver = _prepare_frames(spark, rows)
        if df_raw is None or df_silver is None:
            print("No rows available after normalization. Skipping write.")
            return

        bronze_path, silver_path = _target_paths()
        overwrite_partitioned_dataset(spark, df_raw, bronze_path, "fecha", "bronze")
        overwrite_partitioned_dataset(spark, df_silver, silver_path, "fecha", "silver")
        print("ETL completed successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

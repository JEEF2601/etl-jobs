from __future__ import annotations

import os
import re
from typing import Any

from influxdb import InfluxDBClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from etl_jobs.common.spark_session import build_spark_session
from etl_jobs.common.utils import get_env


def _default_influx_query() -> str:
    lookback = os.getenv("INFLUXDB_LOOKBACK", "30d").strip()
    if not re.fullmatch(r"\d+(ms|s|m|h|d|w)", lookback):
        raise ValueError(
            "Invalid INFLUXDB_LOOKBACK value. Use formats like '1h', '12h', '7d', or '30d'."
        )

    return f"SELECT * FROM /.*/ WHERE time > now() - {lookback}"


def _query_influx_as_rows() -> list[dict[str, Any]]:
    influx_host = get_env("INFLUXDB_HOST")
    influx_port = int(get_env("INFLUXDB_PORT", default="8086"))
    influx_database = get_env("INFLUXDB_DATABASE")
    influx_username = get_env("INFLUXDB_USERNAME")
    influx_password = get_env("INFLUXDB_PASSWORD")

    influx_query = os.getenv("INFLUXDB_QUERY", "").strip() or _default_influx_query()

    client = InfluxDBClient(
        host=influx_host,
        port=influx_port,
        username=influx_username,
        password=influx_password,
        database=influx_database,
    )
    try:
        result: Any = client.query(influx_query)
    finally:
        client.close()

    if not result:
        return []

    rows: list[dict[str, Any]] = []
    raw_result = getattr(result, "raw", {}) or {}
    for series in raw_result.get("series", []):
        columns: list[str] = series.get("columns", [])
        values: list[list[Any]] = series.get("values", [])
        measurement = series.get("name")
        tags: dict[str, Any] = series.get("tags", {}) or {}

        for value_row in values:
            row = {
                column_name: value_row[index] if index < len(value_row) else None
                for index, column_name in enumerate(columns)
            }
            if measurement is not None:
                row["measurement"] = measurement
            for tag_name, tag_value in tags.items():
                row[f"tag_{tag_name}"] = tag_value
            rows.append(row)

    return rows


def _prepare_spark_frame(spark: SparkSession, rows: list[dict[str, Any]]):
    if not rows:
        return None

    all_columns = sorted({key for row in rows for key in row.keys()})
    if not all_columns:
        return None

    normalized_rows: list[dict[str, str | None]] = []
    for row in rows:
        normalized_row: dict[str, str | None] = {}
        for column in all_columns:
            value = row.get(column)
            if value is None:
                normalized_row[column] = None
            elif isinstance(value, (dict, list, tuple, set)):
                normalized_row[column] = str(value)
            else:
                normalized_row[column] = str(value)
        normalized_rows.append(normalized_row)

    schema = StructType([StructField(column, StringType(), True) for column in all_columns])
    spark_df = spark.createDataFrame(normalized_rows, schema=schema)
    drop_candidates = ["result", "table", "_start", "_stop"]
    columns_to_drop = [column for column in drop_candidates if column in spark_df.columns]
    if columns_to_drop:
        spark_df = spark_df.drop(*columns_to_drop)

    if "_time" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed("_time", "timestamp")
    if "time" in spark_df.columns and "timestamp" not in spark_df.columns:
        spark_df = spark_df.withColumnRenamed("time", "timestamp")

    if "timestamp" not in spark_df.columns:
        raise RuntimeError("Influx query result must include '_time' or 'timestamp'.")

    return (
        spark_df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("date", F.to_date("timestamp"))
    )


def _target_path() -> str:
    bucket = get_env("R2_BUCKET")
    prefix = os.getenv("R2_PREFIX", "influx/data").strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/"
    return f"s3a://{bucket}/"


def _cleanup_spark_staging_dirs(spark: SparkSession, target_path: str) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
    fs = path.getFileSystem(hadoop_conf)

    if not fs.exists(path):
        return

    for status in fs.listStatus(path):
        item_path = status.getPath()
        if item_path.getName().startswith(".spark-staging-"):
            fs.delete(item_path, True)
            print(f"Deleted Spark staging directory: {item_path.toString()}")


def main() -> None:
    spark = None
    try:
        spark = build_spark_session("influx_to_r2_etl")
        rows = _query_influx_as_rows()

        if not rows:
            print("No rows returned from InfluxDB. Skipping write.")
            return

        df_clean = _prepare_spark_frame(spark, rows)
        if df_clean is None:
            print("No rows available after normalization. Skipping write.")
            return

        target_path = _target_path()
        _cleanup_spark_staging_dirs(spark, target_path)
        print(f"Refreshing transformed data in {target_path} for the loaded date partitions.")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (
            df_clean.write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("date")
            .parquet(target_path)
        )
        _cleanup_spark_staging_dirs(spark, target_path)
        print("ETL completed successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

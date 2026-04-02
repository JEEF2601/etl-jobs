from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _get_filesystem(spark: SparkSession, path_str: str) -> tuple[Any, Any]:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hadoop_conf)
    return fs, path


def cleanup_spark_staging_dirs(spark: SparkSession, target_path: str) -> None:
    fs, path = _get_filesystem(spark, target_path)

    if not fs.exists(path):
        return

    for status in fs.listStatus(path):
        item_path = status.getPath()
        if item_path.getName().startswith(".spark-staging-"):
            fs.delete(item_path, True)
            print(f"Deleted Spark staging directory: {item_path.toString()}")


def delete_path(spark: SparkSession, target_path: str) -> None:
    fs, path = _get_filesystem(spark, target_path)

    if fs.exists(path):
        fs.delete(path, True)
        print(f"Deleted existing partition path: {path.toString()}")


def overwrite_partitioned_dataset(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    partition_column: str,
    label: str,
) -> None:
    df_to_write = df.where(F.col(partition_column).isNotNull())
    partition_rows = (
        df_to_write.select(partition_column)
        .dropDuplicates([partition_column])
        .orderBy(partition_column)
        .collect()
    )

    if not partition_rows:
        print(f"No partitions available for {label}. Skipping write.")
        return

    cleanup_spark_staging_dirs(spark, target_path)
    print(
        f"Refreshing {label} data in {target_path} for {len(partition_rows)} "
        f"{partition_column} partition(s)."
    )

    for row in partition_rows:
        partition_value = row[partition_column]
        partition_path = f"{target_path.rstrip('/')}/{partition_column}={partition_value}"
        delete_path(spark, partition_path)
        print(f"Writing {label} partition {partition_column}={partition_value}.")
        (
            df_to_write.where(F.col(partition_column) == F.lit(partition_value))
            .drop(partition_column)
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(partition_path)
        )
        cleanup_spark_staging_dirs(spark, target_path)
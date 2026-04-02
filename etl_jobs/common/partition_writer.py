from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from py4j.protocol import Py4JJavaError


def _get_filesystem(spark: SparkSession, path_str: str) -> tuple[Any, Any]:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hadoop_conf)
    return fs, path


def _path_exists(fs: Any, path: Any) -> bool:
    try:
        return bool(fs.exists(path))
    except Py4JJavaError as exc:
        message = str(exc.java_exception)
        if "AWSBadRequestException" in message or "FileNotFoundException" in message:
            return False
        raise


def cleanup_spark_staging_dirs(spark: SparkSession, target_path: str) -> None:
    fs, path = _get_filesystem(spark, target_path)

    if not _path_exists(fs, path):
        return

    try:
        statuses = fs.listStatus(path)
    except Py4JJavaError as exc:
        message = str(exc.java_exception)
        if "AWSBadRequestException" in message or "FileNotFoundException" in message:
            return
        raise

    for status in statuses:
        item_path = status.getPath()
        if item_path.getName().startswith(".spark-staging-"):
            fs.delete(item_path, True)
            print(f"Deleted Spark staging directory: {item_path.toString()}")


def delete_path(spark: SparkSession, target_path: str) -> None:
    fs, path = _get_filesystem(spark, target_path)

    if _path_exists(fs, path):
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

    partition_values = [row[partition_column] for row in partition_rows]
    for partition_value in partition_values:
        partition_path = f"{target_path.rstrip('/')}/{partition_column}={partition_value}"
        delete_path(spark, partition_path)

    if len(partition_values) == 1:
        partition_value = partition_values[0]
        partition_path = f"{target_path.rstrip('/')}/{partition_column}={partition_value}"
        print(f"Writing {label} partition {partition_column}={partition_value}.")
        df_to_write.drop(partition_column).write.mode("overwrite").parquet(partition_path)
    else:
        print(
            f"Writing {label} dataset in a single pass across {len(partition_values)} partitions."
        )
        (
            df_to_write.repartition(len(partition_values), partition_column)
            .write.mode("append")
            .partitionBy(partition_column)
            .parquet(target_path)
        )

    cleanup_spark_staging_dirs(spark, target_path)
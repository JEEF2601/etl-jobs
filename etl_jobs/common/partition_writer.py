from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_jobs.common.s3_utils import S3FileSystem


def cleanup_spark_staging_dirs(spark: SparkSession, target_path: str) -> None:
    """Elimina los directorios temporales .spark-staging-* generados por Spark en R2."""
    if not target_path.startswith("s3a://"):
        print(f"Cleanup skipped: {target_path} is not an S3A path.")
        return

    fs = S3FileSystem()
    try:
        items = fs.list_status(target_path)
        for item_key in items:
            name = item_key.rstrip("/").split("/")[-1]
            if name.startswith(".spark-staging-"):
                full_path = f"s3a://{fs._parse_s3_path(target_path)[0]}/{item_key.lstrip('/')}"
                fs.delete(item_key, recursive=True)
                print(f"Deleted Spark staging directory: {item_key}")
    except Exception as e:
        print(f"Warning: Failed to cleanup staging dirs at {target_path}: {e}")


def delete_path(spark: SparkSession, target_path: str) -> None:
    """Elimina una ruta (archivo o directorio) en R2."""
    if not target_path.startswith("s3a://"):
        print(f"Delete skipped: {target_path} is not an S3A path.")
        return

    fs = S3FileSystem()
    if fs.exists(target_path):
        fs.delete(target_path, recursive=True)
        print(f"Deleted existing path: {target_path}")


def overwrite_partitioned_dataset(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    partition_column: str,
    label: str,
) -> None:
    """Sobrescribe un dataset particionado, limpiando primero las particiones que se van a actualizar."""
    df_to_write = df.where(F.col(partition_column).isNotNull())
    
    # Spark Connect friendly collect for partition values
    partition_rows = (
        df_to_write.select(partition_column)
        .distinct()
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

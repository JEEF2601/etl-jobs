from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_jobs.common.postgres_writer import write_dataframe_to_postgres
from etl_jobs.common.spark_session import build_spark_session
from etl_jobs.common.utils import get_env


EXPECTED_COLUMNS = [
    "timestamp",
    "device_class_str",
    "domain",
    "entity_id",
    "friendly_name_str",
    "friendly_name",
    "state_class_str",
    "value",
    "measurement",
    "latitude",
    "longitude",
]


def _source_path() -> str:
    """
    Devuelve la ruta de origen para los datos de electricidad en formato Silver.

    Prioriza la variable de entorno SILVER_ELECTRICITY_PATH si está definida,
    de lo contrario construye la ruta usando R2_BUCKET y R2_SILVER_ELECTRICITY_PREFIX.
    """
    manual_path = os.getenv("SILVER_ELECTRICITY_PATH", "").strip()
    if manual_path:
        return manual_path

    bucket = get_env("R2_BUCKET")
    prefix = os.getenv("R2_SILVER_ELECTRICITY_PREFIX", "influx/silver/electricity").strip("/")
    return f"s3a://{bucket}/{prefix}/"


def _normalize(df: DataFrame) -> DataFrame:
    """Normaliza el DataFrame para asegurar que tenga todas las columnas esperadas y los tipos de datos correctos."""
    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None))

    return (
        df.select(*EXPECTED_COLUMNS)
        .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
        .withColumn("device_class_str", F.col("device_class_str").cast("string"))
        .withColumn("domain", F.col("domain").cast("string"))
        .withColumn("entity_id", F.col("entity_id").cast("string"))
        .withColumn("friendly_name_str", F.col("friendly_name_str").cast("string"))
        .withColumn("friendly_name", F.col("friendly_name").cast("string"))
        .withColumn("state_class_str", F.col("state_class_str").cast("string"))
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("measurement", F.col("measurement").cast("string"))
        .withColumn("latitude", F.col("latitude").cast("string"))
        .withColumn("longitude", F.col("longitude").cast("string"))
    )


def main() -> None:
    """
    Job principal para leer datos de electricidad en formato Silver desde S3 y escribirlos en una tabla PostgreSQL.
    """
    spark = None
    try:
        spark = build_spark_session("silver_electricity_to_postgres")
        source_path = _source_path()
        base_env = get_env("BASE_ENV")
        table = os.getenv("POSTGRES_TABLE", "silver_electricity")

        print(f"[silver_electricity_to_postgres] Leyendo dataset: {source_path}")
        df = spark.read.parquet(source_path)
        out_df = _normalize(df)

        print(f"[silver_electricity_to_postgres] Escribiendo tabla PostgreSQL: {table}")
        write_dataframe_to_postgres(
            out_df,
            base_env=base_env,
            table=table,
            mode="overwrite",
            truncate=True,
            create_table_column_types=(
                'timestamp TIMESTAMP, '
                "device_class_str STRING, "
                "domain STRING, "
                "entity_id STRING, "
                "friendly_name_str STRING, "
                "friendly_name STRING, "
                "state_class_str STRING, "
                "value DOUBLE, "
                "measurement STRING, "
                "latitude STRING, "
                "longitude STRING"
            ),
        )

        print("[silver_electricity_to_postgres] Job completado con éxito.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

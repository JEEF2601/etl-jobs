from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from etl_jobs.common.partition_writer import overwrite_partitioned_dataset
from etl_jobs.common.spark_session import build_spark_session
from etl_jobs.common.utils import get_env

# Clases de dispositivo eléctrico que se procesan en esta capa Silver
DEVICE_CLASSES = ["voltage", "power", "energy", "current"]

# Columnas del dominio que queremos conservar en Silver
_DOMAIN_COLUMNS = [
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver ETL: análisis de uso de energía eléctrica desde InfluxDB Bronze"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="",
        help="Fecha de inicio del período a procesar (YYYY-MM-DD). "
             "Si no se indica, se usa el lunes de la semana pasada.",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default="",
        help="Fecha de fin del período a procesar (YYYY-MM-DD). "
             "Si no se indica, se usa el domingo de la semana pasada.",
    )
    return parser.parse_args()


def _resolve_date_range(start_date: str, end_date: str) -> tuple[date, date]:
    """Devuelve el rango de fechas a procesar.

    Si se proporcionan ambos argumentos se usan directamente.
    En caso contrario el default es la semana completa anterior (lunes–domingo).
    """
    if start_date.strip() and end_date.strip():
        return date.fromisoformat(start_date.strip()), date.fromisoformat(end_date.strip())

    # Default: semana pasada completa (lunes → domingo)
    today = date.today()
    days_since_monday = today.weekday()  # 0 = lunes, 6 = domingo
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday, last_sunday


def _bronze_path() -> str:
    bucket = get_env("R2_BUCKET")
    prefix = os.getenv("R2_BRONZE_ELECTRICITY_PREFIX", "influx/data").strip("/")
    return f"s3a://{bucket}/{prefix}/"


def _silver_path() -> str:
    bucket = get_env("R2_BUCKET")
    prefix = os.getenv("R2_SILVER_ELECTRICITY_PREFIX", "influx/silver/electricity").strip("/")
    return f"s3a://{bucket}/{prefix}/"


def _build_silver_dataframe(spark: SparkSession, bronze_path: str, start_dt: date, end_dt: date):
    """Lee Bronze, filtra rango de fechas y clases de dispositivo, y aplica el schema Silver."""
    df_bronze = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(bronze_path)
        .filter(
            (F.col("date") >= F.lit(str(start_dt)))
            & (F.col("date") <= F.lit(str(end_dt)))
        )
    )

    df_filtered = df_bronze.filter(F.col("device_class_str").isin(DEVICE_CLASSES))

    # Seleccionar solo columnas disponibles del conjunto deseado + date para partición
    available = set(df_filtered.columns)
    select_cols = [c for c in _DOMAIN_COLUMNS if c in available]
    if "date" not in select_cols:
        select_cols.append("date")

    df_silver = df_filtered.select(*select_cols)

    # Conversiones de tipo para uso analítico.
    # Bronze almacena todo como StringType (BINARY en Parquet). Forzamos el cast
    # intermedio a StringType para que Spark no empuje el cast a DoubleType
    # directamente al lector Parquet (ClassCastException: [B -> Double en Spark 4).
    if "value" in df_silver.columns:
        df_silver = df_silver.withColumn(
            "value", F.col("value").cast(StringType()).cast(DoubleType())
        )

    if "timestamp" in df_silver.columns:
        df_silver = df_silver.withColumn("timestamp", F.to_timestamp("timestamp"))

    # Garantizar tipo date en la columna de partición
    df_silver = df_silver.withColumn("date", F.to_date(F.col("date")))

    return df_silver


def main() -> None:
    args = _parse_args()
    start_dt, end_dt = _resolve_date_range(args.start_date, args.end_date)

    print(f"[silver_influx_electricity] Procesando período: {start_dt} → {end_dt}")
    print(f"[silver_influx_electricity] Device classes: {DEVICE_CLASSES}")

    spark: SparkSession | None = None
    try:
        spark = build_spark_session("silver_influx_electricity_etl")
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

        bronze_path = _bronze_path()
        silver_path = _silver_path()

        print(f"[silver_influx_electricity] Leyendo Bronze: {bronze_path}")
        df_silver = _build_silver_dataframe(spark, bronze_path, start_dt, end_dt)

        row_count = df_silver.count()
        if row_count == 0:
            print(
                "[silver_influx_electricity] No se encontraron filas para el período "
                "y clases de dispositivo indicados. Saltando escritura."
            )
            return

        print(f"[silver_influx_electricity] Escribiendo {row_count} filas → {silver_path}")
        overwrite_partitioned_dataset(
            spark, df_silver, silver_path, "date", "silver_electricity"
        )
        print("[silver_influx_electricity] ETL completado con éxito.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

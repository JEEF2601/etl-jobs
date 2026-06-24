from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

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


def _read_bronze_partition(spark: SparkSession, bronze_path: str, partition_date: date) -> DataFrame | None:
    """Lee una única partición date=YYYY-MM-DD y normaliza TODOS los tipos a StringType.

    La estrategia partición-por-partición es la única que funciona de forma fiable
    cuando el Bronze tiene tipos físicos inconsistentes (DOUBLE en particiones
    antiguas, BINARY/STRING en las nuevas): cada archivo se lee con su propio
    schema nativo y el cast a String se hace en el plan lógico de Spark, no
    en el reader Parquet, evitando tanto CANNOT_MERGE_SCHEMAS como
    CAST_INVALID_INPUT.
    """
    path = f"{bronze_path}date={partition_date}/"
    try:
        df = spark.read.parquet(path)
    except Exception:
        # La partición no existe o está vacía; se omite silenciosamente.
        return None

    # Castear cada columna a StringType con su tipo físico conocido.
    # Funciona tanto si el tipo es DOUBLE como BINARY porque el cast ocurre
    # después de que el reader Parquet decodificó correctamente los bytes.
    df_str = df.select(
        [F.col(c).cast(StringType()).alias(c) for c in df.columns]
    )
    # La columna "date" es una columna de partición: no está en los archivos
    # Parquet sino en el nombre del directorio. La añadimos explícitamente.
    return df_str.withColumn("date", F.lit(str(partition_date)).cast(DateType()))


def _build_silver_dataframe(spark: SparkSession, bronze_path: str, start_dt: date, end_dt: date) -> DataFrame | None:
    """Lee Bronze día a día, normaliza tipos, filtra y aplica el schema Silver."""
    dfs: list[DataFrame] = []
    current = start_dt
    while current <= end_dt:
        df_part = _read_bronze_partition(spark, bronze_path, current)
        if df_part is not None:
            dfs.append(df_part)
        current += timedelta(days=1)

    if not dfs:
        return None

    # Unir todos los días; allowMissingColumns=True maneja columnas que no
    # existen en todas las particiones (e.g. columnas añadidas con el tiempo).
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    df_filtered = df_all.filter(F.col("device_class_str").isin(DEVICE_CLASSES))

    # Seleccionar solo las columnas del dominio + date para la partición
    available = set(df_filtered.columns)
    select_cols = [c for c in _DOMAIN_COLUMNS if c in available]
    if "date" not in select_cols:
        select_cols.append("date")

    df_silver = df_filtered.select(*select_cols)

    # Conversiones de tipo finales (sobre strings limpios, sin riesgo de pushdown)
    if "value" in df_silver.columns:
        df_silver = df_silver.withColumn("value", F.col("value").cast(DoubleType()))
    if "timestamp" in df_silver.columns:
        df_silver = df_silver.withColumn("timestamp", F.to_timestamp("timestamp"))

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

        if df_silver is None:
            print(
                "[silver_influx_electricity] No se encontraron particiones Bronze "
                "para el período indicado. Saltando escritura."
            )
            return

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

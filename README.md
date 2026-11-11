# etl-jobs

Paquete Python compartido con jobs de Spark (compatibles con Spark Connect) para pipelines ETL. Se utiliza en:

- **Dagster**: Orquestador que ejecuta los jobs mediante Spark Connect.
- **Spark-ELT**: Cluster Spark standalone que ejecuta los jobs.

## Requisitos para Spark Connect

- Spark 4.0.1 o superior.
- `boto3` para operaciones de limpieza de staging en R2/S3.
- `pyspark[connect]==4.0.1` instalado en el entorno de ejecución.

## Configuración de SparkSession

Se utiliza `etl_jobs.common.spark_session.build_spark_session` para inicializar la sesión. Soporta:

- **Modo Remoto (Spark Connect):** Configurar `SPARK_REMOTE` o `SPARK_REMOTE_URL` (ej. `sc://spark-master:15002`).
- **Modo Cluster (Tradicional):** Configurar `SPARK_MASTER_URL` (ej. `spark://spark-master:7077`).
- **Cloudflare R2 / S3A:** Configuración automática mediante `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `R2_BUCKET`.

## Limpieza de Staging

Las operaciones de limpieza ahora utilizan `boto3` para interactuar con R2, evitando dependencias directas de la JVM (`_jsc`, `_jvm`) incompatibles con Spark Connect.

## Instalación

```bash
# Desde repositorio (release)
pip install git+https://github.com/JEEF2601/etl-jobs.git@v0.3.0
```

## Versionado

Bump `version` en `pyproject.toml` y taguea en git:
```bash
git tag -a v0.3.0 -m "Release 0.3.0: Spark Connect compatibility"
git push origin v0.3.0
```

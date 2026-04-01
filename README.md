# etl-jobs

Paquete Python compartido con jobs de Spark para pipelines ETL. Se utiliza en:
- **Dagster**: Orquestador que ejecuta los jobs via `spark-submit` en modo local
- **Spark-ELT**: Cluster Spark standalone que ejecuta los jobs directamente

## Instalación

```bash
# Desde archivo local en desarrollo
pip install -e .

# Desde repositorio (release)
pip install git+https://github.com/JEEF2601/etl-jobs.git@v0.1.0
```

## Estructura

```
etl_jobs/
  common/
    spark_session.py  – build_spark_session() con config S3A/R2
    utils.py          – get_env(), load_yaml()
  jobs/
    influx_to_r2.py   – InfluxDB → Parquet en R2
    cryptocompare_to_r2.py  – API CryptoCompare → Bronze/Silver Parquet
    daily_aggregation.py    – Placeholder
```

## Dependencias

- `pyspark>=4.0.1`
- `influxdb`
- `requests`
- `PyYAML`

## Versionado

Bump `version` en `pyproject.toml` y taguea en git:
```bash
git tag -a v0.2.0 -m "Release 0.2.0"
git push origin v0.2.0
```

Luego referencia en `requirements.txt` o Dockerfile:
```dockerfile
RUN pip install git+https://github.com/JEEF2601/etl-jobs.git@v0.2.0
```

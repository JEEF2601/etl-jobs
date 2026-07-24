from __future__ import annotations

from urllib.parse import parse_qsl, urlparse

from pyspark.sql import DataFrame


def parse_base_env_to_jdbc(base_env: str) -> tuple[str, str, str]:
    """Convierte BASE_ENV (postgres://...) a JDBC URL y credenciales."""
    parsed = urlparse(base_env)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("BASE_ENV debe iniciar con postgres:// o postgresql://")

    if not parsed.hostname:
        raise ValueError("BASE_ENV inválido: falta host")
    if not parsed.path or parsed.path == "/":
        raise ValueError("BASE_ENV inválido: falta database en la URL")

    db_name = parsed.path.lstrip("/")
    port = parsed.port or 5432
    query = dict(parse_qsl(parsed.query))
    query_str = f"?{'&'.join([f'{k}={v}' for k, v in query.items()])}" if query else ""

    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{port}/{db_name}{query_str}"
    user = parsed.username or ""
    password = parsed.password or ""
    return jdbc_url, user, password


def write_dataframe_to_postgres(
    df: DataFrame,
    *,
    base_env: str,
    table: str,
    mode: str = "overwrite",
    truncate: bool = True,
    create_table_column_types: str | None = None,
) -> None:
    """Escribe un DataFrame en PostgreSQL usando JDBC."""
    jdbc_url, user, password = parse_base_env_to_jdbc(base_env)

    writer = (
        df.write.format("jdbc")
        .mode(mode)
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
    )

    if truncate:
        writer = writer.option("truncate", "true")

    if create_table_column_types:
        writer = writer.option("createTableColumnTypes", create_table_column_types)

    writer.save()

from __future__ import annotations

import argparse

from etl_jobs.common.spark_session import build_spark_session


def run(date: str | None = None) -> None:
    spark = build_spark_session("daily_aggregation")
    print(f"daily_aggregation base lista. date={date}")
    spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=False)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(date=args.date)

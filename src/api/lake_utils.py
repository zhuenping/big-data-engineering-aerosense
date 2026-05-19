#!/usr/bin/env python3
"""
Data Lake Utilities - AeroSense REST API

Provides helper functions to read from the local data lake (Parquet files)
for the Flask API endpoints.

Author: Exam Candidate
Date: 2024-2025
"""

import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    max as spark_max,
    min as spark_min,
    avg,
    sum as spark_sum,
    to_timestamp,
    lit,
    desc as spark_desc,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

CURATED_PATH: str = "/tmp/datalake/curated/domain=iot"
CONSUMPTION_PATH: str = "/tmp/datalake/consumption/use_case=sensor_averages"

VALID_SENSORS: set[str] = {"temperature", "humidity", "pressure"}

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lake_utils")

# --------------------------------------------------------------------------- #
# Spark session (lazy, module-level singleton)
# --------------------------------------------------------------------------- #

_spark = None  # type: SparkSession | None


def _get_spark() -> SparkSession:
    """Return a module-level singleton SparkSession."""
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("AeroSenseAPILakeReader")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
    return _spark


# --------------------------------------------------------------------------- #
# Public API helpers
# --------------------------------------------------------------------------- #


def list_sensors() -> list[str]:
    """Return the list of sensor types found in the curated zone."""
    spark = _get_spark()
    try:
        df = spark.read.parquet(CURATED_PATH)
        rows = df.select("sensor_type").distinct().collect()
        return sorted({r["sensor_type"] for r in rows if r["sensor_type"]})
    except Exception:
        logger.exception("Failed to list sensors from data lake.")
        return sorted(list(VALID_SENSORS))


def get_latest_reading(curated_path: str, sensor_type: str):  # type: ignore
    """
    Return the most recent reading for ``sensor_type`` from the curated zone.

    Returns:
        A dict with keys: sensor_type, value, unit, timestamp, source, is_anomaly
        or ``None`` if no data is available.
    """
    spark = _get_spark()
    try:
        df = (
            spark.read.parquet(curated_path)
            .filter(col("sensor_type") == sensor_type)
            .filter(col("event_time").isNotNull())
            .orderBy(spark_desc("event_time"))
            .limit(1)
        )
        row = df.collect()
        if not row:
            return None
        r = row[0]
        return {
            "sensor_type": r["sensor_type"],
            "value": float(r["value"]),
            "unit": r["unit"],
            "timestamp": int(r["event_time"].timestamp() * 1000),
            "event_time": r["event_time"].isoformat(),
            "source": r["source"],
            "is_anomaly": bool(r["is_anomaly"]),
        }
    except Exception:
        logger.exception("Failed to fetch latest reading for %s", sensor_type)
        return None


def get_sensor_stats(curated_path: str, sensor_type: str, days: int) -> list[dict]:
    """
    Compute daily statistics for ``sensor_type`` over the last ``days`` days.

    Returns:
        A list of dicts, one per day, sorted by date ascending:
            [ {date, mean_value, min_value, max_value, observation_count, anomaly_count}, ... ]
    """
    spark = _get_spark()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        df = (
            spark.read.parquet(curated_path)
            .filter(col("sensor_type") == sensor_type)
            .filter(col("event_time") >= cutoff)
            .withColumn("event_date", col("event_time").cast("date"))
            .groupBy("event_date")
            .agg(
                avg("value").alias("mean_value"),
                spark_min("value").alias("min_value"),
                spark_max("value").alias("max_value"),
                count("*").alias("observation_count"),
                spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            )
            .orderBy("event_date")
        )
        rows = df.collect()
        return [
            {
                "date": r["event_date"].isoformat(),
                "mean_value": round(float(r["mean_value"]), 2),
                "min_value": round(float(r["min_value"]), 2),
                "max_value": round(float(r["max_value"]), 2),
                "observation_count": r["observation_count"],
                "anomaly_count": r["anomaly_count"],
            }
            for r in rows
        ]
    except Exception:
        logger.exception("Failed to compute stats for %s", sensor_type)
        return []


def get_recent_anomalies(curated_path: str, sensor_type: str, limit: int) -> list[dict]:
    """
    Return the ``limit`` most recent anomaly records for ``sensor_type``.

    Returns:
        A list of dicts sorted by event_time descending.
    """
    spark = _get_spark()
    try:
        df = (
            spark.read.parquet(curated_path)
            .filter(col("sensor_type") == sensor_type)
            .filter(col("is_anomaly") == True)
            .orderBy(spark_desc("event_time"))
            .limit(limit)
        )
        rows = df.collect()
        return [
            {
                "sensor_type": r["sensor_type"],
                "value": float(r["value"]),
                "unit": r["unit"],
                "timestamp": int(r["event_time"].timestamp() * 1000),
                "event_time": r["event_time"].isoformat(),
                "source": r["source"],
            }
            for r in rows
        ]
    except Exception:
        logger.exception("Failed to fetch anomalies for %s", sensor_type)
        return []

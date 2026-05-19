#!/usr/bin/env python3
"""
Analytics Module - Spark SQL Queries on the Data Lake

This script executes analytical queries against the three-zone data lake
and demonstrates the impact of partition pruning.

Author: Exam Candidate
Date: 2024-2025
"""

import logging
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    stddev,
    expr,
    to_timestamp,
    year as spark_year,
    month as spark_month,
    dayofmonth,
    hour,
    when,
    desc,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

DATA_LAKE_ROOT: str = "/tmp/datalake"

RAW_ZONE: str = f"{DATA_LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_ZONE: str = f"{DATA_LAKE_ROOT}/curated/domain=iot"
CONSUMPTION_ZONE: str = f"{DATA_LAKE_ROOT}/consumption/use_case=sensor_averages"

OUTPUT_DIR: str = "outputs/analytics"

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("analytics")


# --------------------------------------------------------------------------- #
# Spark session
# --------------------------------------------------------------------------- #

def create_spark_session(app_name: str = "AeroSenseAnalytics") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #

def query_top5_anomaly_hours(spark: SparkSession):
    """
    Query 1: Top 5 hours with the highest number of anomalies
    (all sensors combined).
    """
    logger.info("=== Query 1: Top 5 hours with highest anomaly count ===")

    df = (
        spark.read.parquet(CURATED_ZONE)
        .filter(col("is_anomaly") == True)
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("event_date", col("event_time").cast("date"))
        .groupBy("event_date", "event_hour")
        .agg(count("*").alias("anomaly_count"))
        .orderBy(desc("anomaly_count"))
        .limit(5)
    )

    logger.info("Top 5 anomaly hours:")
    df.show(truncate=False)

    df.write.mode("overwrite").option("header", "true").csv(
        f"{OUTPUT_DIR}/query1_top5_anomaly_hours"
    )
    logger.info("Saved to %s/query1_top5_anomaly_hours", OUTPUT_DIR)

    return df


def query_sensor_stats(spark: SparkSession):
    """
    Query 2: For each sensor type, compute:
      - global mean, min, max, stddev, anomaly rate (%)
    """
    logger.info("=== Query 2: Per-sensor statistics ===")

    df = (
        spark.read.parquet(CURATED_ZONE)
        .groupBy("sensor_type")
        .agg(
            avg("value").alias("global_mean"),
            spark_min("value").alias("global_min"),
            spark_max("value").alias("global_max"),
            stddev("value").alias("global_stddev"),
            (count(when(col("is_anomaly") == True, 1)) / count("*") * 100).alias("anomaly_rate_pct"),
        )
        .orderBy("sensor_type")
    )

    logger.info("Per-sensor statistics:")
    df.show(truncate=False)

    df.write.mode("overwrite").option("header", "true").csv(
        f"{OUTPUT_DIR}/query2_sensor_stats"
    )
    logger.info("Saved to %s/query2_sensor_stats", OUTPUT_DIR)

    return df


def query_temperature_daily(spark: SparkSession):
    """
    Query 3: Daily evolution of mean and anomaly count for temperature sensor.
    """
    logger.info("=== Query 3: Daily temperature evolution ===")

    df = (
        spark.read.parquet(CURATED_ZONE)
        .filter(col("sensor_type") == "temperature")
        .withColumn("event_date", col("event_time").cast("date"))
        .groupBy("event_date")
        .agg(
            avg("value").alias("daily_mean_temperature"),
            count(when(col("is_anomaly") == True, 1)).alias("daily_anomaly_count"),
            count("*").alias("daily_observation_count"),
        )
        .orderBy("event_date")
    )

    logger.info("Daily temperature evolution:")
    df.show(truncate=False)

    df.write.mode("overwrite").option("header", "true").csv(
        f"{OUTPUT_DIR}/query3_temperature_daily"
    )
    logger.info("Saved to %s/query3_temperature_daily", OUTPUT_DIR)

    return df


def demonstrate_partition_pruning(spark: SparkSession):
    """
    Query 4: Partition pruning demonstration.

    Runs the same COUNT(*) query:
      (a) without partition filters (full scan)
      (b) with partition filters (pruning)

    Measures execution time for both and computes the speedup factor.
    """
    logger.info("=== Query 4: Partition pruning demonstration ===")

    # (a) Full scan - no partition filter
    logger.info("Query 4a: COUNT(*) without partition filter (full scan)...")
    start_full = time.perf_counter()
    full_count = spark.read.parquet(CURATED_ZONE).count()
    elapsed_full = time.perf_counter() - start_full
    logger.info("  Full scan count = %d, elapsed = %.3f s", full_count, elapsed_full)

    # (b) With partition filter on sensor_type (triggers partition pruning)
    logger.info("Query 4b: COUNT(*) with partition filter (pruning)...")
    start_pruned = time.perf_counter()
    pruned_count = (
        spark.read.parquet(CURATED_ZONE)
        .filter(col("sensor_type") == "temperature")
        .count()
    )
    elapsed_pruned = time.perf_counter() - start_pruned
    logger.info("  Pruned scan count = %d, elapsed = %.3f s", pruned_count, elapsed_pruned)

    speedup = elapsed_full / elapsed_pruned if elapsed_pruned > 0 else float("inf")

    logger.info("--- Partition Pruning Results ---")
    logger.info("  Full scan time:   %.3f s", elapsed_full)
    logger.info("  Pruned scan time:  %.3f s", elapsed_pruned)
    logger.info("  Speedup factor:   %.2f x", speedup)
    logger.info("  (Partition pruning avoids reading unnecessary partitions.)")

    # Save results
    results = spark.createDataFrame(
        [
            ("full_scan", full_count, round(elapsed_full, 3)),
            ("pruned_scan", pruned_count, round(elapsed_pruned, 3)),
            ("speedup_factor", None, round(speedup, 2)),
        ],
        ["scan_type", "record_count", "elapsed_seconds"],
    )

    results.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{OUTPUT_DIR}/query4_partition_pruning"
    )
    logger.info("Saved to %s/query4_partition_pruning", OUTPUT_DIR)

    return elapsed_full, elapsed_pruned, speedup


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    logger.info("Starting AeroSense Analytics...")
    logger.info("Data lake root: %s", DATA_LAKE_ROOT)

    spark = create_spark_session()

    try:
        # Run all 4 required queries
        query_top5_anomaly_hours(spark)
        query_sensor_stats(spark)
        query_temperature_daily(spark)
        demonstrate_partition_pruning(spark)

        logger.info("All analytics queries completed successfully.")
        logger.info("CSV outputs written to: %s", OUTPUT_DIR)

    except Exception as exc:
        logger.error("Analytics failed: %s", exc)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()

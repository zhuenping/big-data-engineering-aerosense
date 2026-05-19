#!/usr/bin/env python3
"""
Spark Structured Streaming Pipeline - AeroSense IoT Sensor Data

This module consumes the ``sensor-events`` topic from Kafka, validates and
enriches the events, detects anomalies, computes windowed aggregates, and
writes the results to the three-zone data lake (raw / curated / consumption).

Author: Exam Candidate
Date: 2024-2025
"""

import logging
import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    from_json,
    lit,
    minute,
    hour,
    dayofmonth,
    month,
    year,
    window,
    avg,
    min as spark_min,
    max as spark_max,
    count as spark_count,
    sum as spark_sum,
    to_utc_timestamp,
    current_timestamp,
    unix_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

KAFKA_BOOTSTRAP_SERVERS: str = "127.0.0.1:19092,127.0.0.1:19094,127.0.0.1:19096"
TOPIC: str = "sensor-events"

# Data lake base path (no absolute user paths)
DATA_LAKE_ROOT: str = "/tmp/datalake"

RAW_ZONE: str = f"{DATA_LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_ZONE: str = f"{DATA_LAKE_ROOT}/curated/domain=iot"
CONSUMPTION_ZONE: str = f"{DATA_LAKE_ROOT}/consumption/use_case=sensor_averages"

CHECKPOINT_ROOT: str = f"{DATA_LAKE_ROOT}/_checkpoints"

# Anomaly detection thresholds (independent of producer's self-declared flag)
# Defined as function to avoid SparkContext requirement at module import
def _anomaly_condition():
    return (
        (col("sensor_type") == "temperature") & (col("value") > 35.0)
        | (col("sensor_type") == "humidity") & (col("value") > 90.0)
        | (col("sensor_type") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0))
    )

# Physical plausibility ranges (used for validation / outlier filter)
SENSOR_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (15.0, 45.0),
    "humidity": (30.0, 95.0),
    "pressure": (980.0, 1040.0),
}

# Kafka value schema
VALUE_SCHEMA: StructType = StructType([
    StructField("sensor", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True),
])

# Window duration and watermark
WINDOW_DURATION: str = "5 minutes"
WATERMARK_DELAY: str = "2 minutes"

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("spark_pipeline")


# --------------------------------------------------------------------------- #
# Spark session builder
# --------------------------------------------------------------------------- #

def create_spark_session(app_name: str = "AeroSenseSparkPipeline") -> SparkSession:
    """Create and return a configured SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Pipeline stages
# --------------------------------------------------------------------------- #

def read_from_kafka(spark: SparkSession):
    """Subscribe to ``sensor-events`` and return the raw Kafka stream."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_validate(df):
    """
    Parse the Kafka ``value`` column as JSON, cast types, and add
    ``event_time`` (TimestampType). Invalid records are filtered out.

    Returns:
        Parsed and validated DataFrame.
    """
    # Decode bytes -> string, then parse JSON
    parsed = (
        df
        .selectExpr("key as kafka_key", "value as kafka_value", "timestamp as kafka_ingestion_time")
        .select(
            col("kafka_key").cast("string").alias("sensor_type_key"),
            from_json(col("kafka_value").cast("string"), VALUE_SCHEMA).alias("data"),
            col("kafka_ingestion_time").alias("ingestion_time"),
        )
        .select(
            col("sensor_type_key"),
            col("data.sensor").alias("sensor_type"),
            col("data.value").alias("value"),
            col("data.unit").alias("unit"),
            col("data.timestamp").alias("ts_ms"),
            col("data.source").alias("source"),
            col("data.anomaly").alias("producer_anomaly_flag"),
            col("ingestion_time"),
        )
    )

    # Convert epoch-ms to Timestamp and add ingestion date columns
    with_event_time = (
        parsed
        .withColumn("event_time", (col("ts_ms") / 1000.0).cast("timestamp"))
        .withColumn("ingestion_time", col("ingestion_time").cast("timestamp"))
        .withColumn("event_year", year(col("event_time")))
        .withColumn("event_month", month(col("event_time")))
        .withColumn("event_day", dayofmonth(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("ingestion_year", year(col("ingestion_time")))
        .withColumn("ingestion_month", month(col("ingestion_time")))
        .withColumn("ingestion_day", dayofmonth(col("ingestion_time")))
        .withColumn("ingestion_hour", hour(col("ingestion_time")))
    )

    # Validate: drop records with null sensor_type or value, or out-of-range values
    valid = with_event_time.filter(
        col("sensor_type").isNotNull()
        & col("value").isNotNull()
        & (
            ((col("sensor_type") == "temperature") & col("value").between(15.0, 45.0))
            | ((col("sensor_type") == "humidity") & col("value").between(30.0, 95.0))
            | ((col("sensor_type") == "pressure") & col("value").between(980.0, 1040.0))
        )
    )

    return valid


def add_anomaly_flag(df):
    """Add ``is_anomaly`` column based on the threshold rules (independent)."""
    return df.withColumn("is_anomaly", _anomaly_condition())


def write_raw_zone(df, query_name: str = "raw_zone_writer"):
    """
    Write the raw JSON payloads to the raw zone in JSON format,
    partitioned by ingestion year / month / day / hour.
    """
    return (
        df
        .writeStream
        .format("json")
        .option("path", RAW_ZONE)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day", "ingestion_hour")
        .outputMode("append")
        .queryName(query_name)
        .start()
    )


def build_windowed_aggregates(df):
    """
    Compute 5-minute windowed aggregates per sensor_type:
      - mean_value, min_value, max_value
      - observation_count
      - anomaly_count

    Returns:
        DataFrame with columns:
            sensor_type, window_start, window_end,
            mean_value, min_value, max_value,
            observation_count, anomaly_count
    """
    return (
        df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            col("sensor_type"),
            window(col("event_time"), WINDOW_DURATION),
        )
        .agg(
            avg("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            spark_count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .select(
            col("sensor_type"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("mean_value"),
            col("min_value"),
            col("max_value"),
            col("observation_count"),
            col("anomaly_count"),
        )
    )


def write_curated_zone(df, query_name: str = "curated_zone_writer"):
    """
    Write validated, anomaly-tagged events to the curated zone.
    Partitioned by sensor_type / event_year / event_month / event_day.
    """
    curated = (
        df
        .withColumn("partition_date", col("event_time").cast("date"))
        .withColumn("event_year", year(col("event_time")))
        .withColumn("event_month", month(col("event_time")))
        .withColumn("event_day", dayofmonth(col("event_time")))
    )

    return (
        curated
        .writeStream
        .format("parquet")
        .option("path", CURATED_ZONE)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/curated")
        .partitionBy("sensor_type", "event_year", "event_month", "event_day")
        .outputMode("append")
        .queryName(query_name)
        .start()
    )


def write_consumption_zone(agg_df, query_name: str = "consumption_zone_writer"):
    """
    Write windowed aggregates to the consumption zone.
    Partitioned by sensor_type / window_start year / month.
    """
    with_partition_cols = (
        agg_df
        .withColumn("window_year", year(col("window_start")))
        .withColumn("window_month", month(col("window_start")))
    )

    return (
        with_partition_cols
        .writeStream
        .format("parquet")
        .option("path", CONSUMPTION_ZONE)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/consumption")
        .partitionBy("sensor_type", "window_year", "window_month")
        .outputMode("append")
        .queryName(query_name)
        .start()
    )


# --------------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------------- #

def run_pipeline() -> None:
    """Assemble and start the end-to-end Spark Structured Streaming pipeline."""
    logger.info("Starting AeroSense Spark Structured Streaming Pipeline...")
    logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", TOPIC)
    logger.info("Data lake root: %s", DATA_LAKE_ROOT)

    spark = create_spark_session()

    # Reduce shuffle partitions for local development
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    # Stage 1: Read from Kafka
    logger.info("Stage 1: Reading from Kafka topic '%s'...", TOPIC)
    kafka_df = read_from_kafka(spark)

    # Stage 2: Parse JSON and validate
    logger.info("Stage 2: Parsing JSON and validating records...")
    parsed_df = parse_and_validate(kafka_df)

    # Stage 3: Anomaly detection
    logger.info("Stage 3: Adding anomaly detection column...")
    anomaly_df = add_anomaly_flag(parsed_df)

    # Stage 4a: Write to raw zone
    logger.info("Stage 4a: Writing to raw zone -> %s", RAW_ZONE)
    raw_query = write_raw_zone(anomaly_df)

    # Stage 4b: Write to curated zone
    logger.info("Stage 4b: Writing to curated zone -> %s", CURATED_ZONE)
    curated_query = write_curated_zone(anomaly_df)

    # Stage 5: Windowed aggregation
    logger.info("Stage 5: Computing 5-minute windowed aggregates...")
    agg_df = build_windowed_aggregates(anomaly_df)

    # Stage 6: Write to consumption zone
    logger.info("Stage 6: Writing to consumption zone -> %s", CONSUMPTION_ZONE)
    consumption_query = write_consumption_zone(agg_df)

    logger.info("All streaming queries started. Awaiting termination...")
    logger.info(
        "Active queries: raw=%s, curated=%s, consumption=%s",
        raw_query.name,
        curated_query.name,
        consumption_query.name,
    )

    # Await termination with timeout (for testing)
    logger.info("Waiting up to 120 seconds for data processing...")
    spark.streams.awaitAnyTermination(timeout=120)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    try:
        run_pipeline()
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
    except Exception as exc:
        logger.error("Pipeline crashed: %s", exc)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

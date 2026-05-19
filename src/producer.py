#!/usr/bin/env python3
"""
Producer Module - AeroSense IoT Sensor Data Generator

This module simulates IoT sensor readings and publishes them to the
`sensor-events` Kafka topic. It supports parameterized generation
with configurable event count, rate, and source identifier.

Author: Exam Candidate
Date: 2024-2025
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

BOOTSTRAP_SERVERS: list[str] = ["127.0.0.1:19092", "127.0.0.1:19094", "127.0.0.1:19096"]
TOPIC_NAME: str = "sensor-events"

# Sensor configuration: (sensor_name, unit, normal_range_low, normal_range_high)
SENSOR_CONFIG: dict[str, tuple[str, float, float]] = {
    "temperature": ("C", 15.0, 45.0),
    "humidity": ("%", 30.0, 95.0),
    "pressure": ("hPa", 980.0, 1040.0),
}

# Anomaly thresholds (used by the producer to self-declare anomalies)
ANOMALY_THRESHOLDS: dict[str, tuple[float, float]] = {
    "temperature": (5.0, 55.0),   # anomaly if outside this wider range
    "humidity": (15.0, 105.0),
    "pressure": (960.0, 1060.0),
}

# Target anomaly rate (producer self-declared)
TARGET_ANOMALY_RATE: float = 0.12   # 12% to guarantee >= 10%

# Kafka producer configuration
PRODUCER_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "max_in_flight_requests_per_connection": 1,
    "linger_ms": 20,
    "batch_size": 32768,
    "compression_type": "gzip",
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    "key_serializer": lambda k: k.encode("utf-8") if k else None,
}


# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("producer")


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #

def generate_sensor_reading(sensor_type: str, source: str) -> dict:
    """
    Generate a single sensor reading.

    Args:
        sensor_type: One of 'temperature', 'humidity', 'pressure'.
        source: Identifier of the site / rack (e.g. 'site-A-rack-12').

    Returns:
        A dictionary conforming to the sensor event schema.
    """
    unit, low, high = SENSOR_CONFIG[sensor_type]

    # Decide whether this reading is an anomaly
    is_anomaly: bool = random.random() < TARGET_ANOMALY_RATE

    if is_anomaly:
        # Generate an out-of-threshold value
        anomaly_low, anomaly_high = ANOMALY_THRESHOLDS[sensor_type]
        # Pick a value outside the *normal* range but within the *anomaly* range
        if random.choice([True, False]):
            # Below normal range
            value = round(random.uniform(anomaly_low, low - 0.01), 2)
        else:
            # Above normal range
            value = round(random.uniform(high + 0.01, anomaly_high), 2)
    else:
        # Normal range
        value = round(random.uniform(low, high), 2)

    timestamp_ms: int = int(datetime.now(timezone.utc).timestamp() * 1000)

    return {
        "sensor": sensor_type,
        "value": value,
        "unit": unit,
        "timestamp": timestamp_ms,
        "source": source,
        "anomaly": is_anomaly,
    }


def create_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer instance."""
    return KafkaProducer(**PRODUCER_CONFIG)


def produce_events(
    producer: KafkaProducer,
    count: int,
    rate: float,
    source: str,
) -> None:
    """
    Produce ``count`` sensor events to the ``sensor-events`` topic.

    The events are distributed round-robin across the three sensor types.
    The publish rate is throttled to approximately ``rate`` events / second.

    Args:
        producer: An initialized KafkaProducer.
        count: Total number of events to produce.
        rate: Target publish rate (events per second).
        source: Site / rack identifier written into each event.
    """
    sensor_types: list[str] = list(SENSOR_CONFIG.keys())
    interval: float = 1.0 / rate if rate > 0 else 0.0

    logger.info(
        "Starting production: count=%d, rate=%.1f evt/s, source=%s",
        count,
        rate,
        source,
    )

    produced: int = 0
    start_time: float = time.perf_counter()

    for i in range(count):
        sensor_type = sensor_types[i % len(sensor_types)]
        event = generate_sensor_reading(sensor_type, source)
        key = sensor_type   # key-based partitioning

        future = producer.send(TOPIC_NAME, key=key, value=event)
        produced += 1

        if (i + 1) % 100 == 0:
            logger.info("Produced %d / %d events ...", i + 1, count)

        # Throttle to respect the target rate
        if rate > 0:
            time.sleep(interval)

    # Block until all buffered records have been sent
    producer.flush()
    elapsed: float = time.perf_counter() - start_time

    logger.info(
        "Production complete: %d events sent in %.2f s (%.1f evt/s)",
        produced,
        elapsed,
        produced / elapsed if elapsed > 0 else 0,
    )


# --------------------------------------------------------------------------- #
# Argument parser
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
    """Parse and return CLI arguments."""
    parser = argparse.ArgumentParser(
        description="AeroSense IoT Sensor Event Producer",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of sensor events to produce (default: 100)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Target publish rate in events per second (default: 10.0)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="site-A-rack-12",
        help="Source identifier written into each event (default: site-A-rack-12)",
    )
    return parser.parse_args()


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    args = parse_args()

    if args.count <= 0:
        logger.error("Argument --count must be a positive integer.")
        sys.exit(1)

    producer = create_producer()
    try:
        produce_events(
            producer=producer,
            count=args.count,
            rate=args.rate,
            source=args.source,
        )
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as exc:
        logger.exception("Producer crashed: %s", exc)
        sys.exit(1)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()

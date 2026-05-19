#!/usr/bin/env python3
"""
Kafka Utilities - AeroSense REST API

Provides helper functions to publish sensor readings to the Kafka
``sensor-events`` topic from the Flask API.

Author: Exam Candidate
Date: 2024-2025
"""

import json
import logging

from kafka import KafkaProducer

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

BOOTSTRAP_SERVERS: list[str] = ["127.0.0.1:19092", "127.0.0.1:19094", "127.0.0.1:19096"]
TOPIC_NAME: str = "sensor-events"

PRODUCER_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "max_in_flight_requests_per_connection": 1,
    "linger_ms": 5,
    "batch_size": 16384,
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    "key_serializer": lambda k: k.encode("utf-8") if k else None,
}

# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kafka_utils")

# --------------------------------------------------------------------------- #


def get_producer() -> KafkaProducer:
    """Create and return a KafkaProducer instance."""
    return KafkaProducer(**PRODUCER_CONFIG)


def publish_reading(reading: dict) -> None:
    """
    Publish a single sensor reading to the ``sensor-events`` topic.

    Args:
        reading: A dictionary conforming to the sensor event schema
                 (keys: sensor, value, unit, timestamp, source, anomaly).

    Raises:
        Exception: If publishing fails after the configured retries.
    """
    producer = get_producer()
    try:
        key = reading.get("sensor", "unknown")
        future = producer.send(TOPIC_NAME, key=key, value=reading)
        # Block until the record has been acknowledged by all in-sync replicas
        record_metadata = future.get(timeout=30)
        logger.info(
            "Published to topic=%s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
        producer.flush(timeout=30)
    except Exception as exc:
        logger.error("Failed to publish reading: %s", exc)
        raise
    finally:
        producer.close()

#!/usr/bin/env python3
"""
Kafka Consumer (Optional) - AeroSense IoT Sensor Platform

This consumer is provided for debugging and manual verification.
It prints messages from the ``sensor-events`` topic to stdout.

Author: Exam Candidate
Date: 2024-2025
"""

import json
import logging
import sys

from kafka import KafkaConsumer

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

BOOTSTRAP_SERVERS: list[str] = ["127.0.0.1:19092", "127.0.0.1:19094", "127.0.0.1:19096"]
TOPIC: str = "sensor-events"
GROUP_ID: str = "aerosense-debug-consumer"

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("consumer")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    logger.info("Subscribed to topic '%s' (group=%s)", TOPIC, GROUP_ID)
    logger.info("Waiting for messages... (Ctrl+C to stop)")

    try:
        for msg in consumer:
            print(
                f"[partition={msg.partition}, offset={msg.offset}, "
                f"key={msg.key}] {json.dumps(msg.value)}"
            )
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    except Exception as exc:
        logger.exception("Consumer crashed: %s", exc)
        sys.exit(1)
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()

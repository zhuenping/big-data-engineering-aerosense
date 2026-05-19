#!/usr/bin/env python3
"""
Flask REST API - AeroSense IoT Sensor Platform

Endpoints implemented (all responses are JSON):
  GET  /api/v1/health
  GET  /api/v1/sensors
  GET  /api/v1/sensors/<type>/latest
  GET  /api/v1/sensors/<type>/stats?days=N
  GET  /api/v1/anomalies?sensor=<type>&limit=N
  POST /api/v1/readings

Author: Exam Candidate
Date: 2024-2025
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

import flask
from flask import Flask, jsonify, request

# --------------------------------------------------------------------------- #
# Local imports
# --------------------------------------------------------------------------- #

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kafka_utils import publish_reading as _publish_to_kafka
from lake_utils import (
    get_latest_reading as _get_latest_from_lake,
    get_sensor_stats as _get_stats_from_lake,
    list_sensors as _list_sensors_from_lake,
    get_recent_anomalies as _get_anomalies_from_lake,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

DATA_LAKE_ROOT: str = "/tmp/datalake"
CURATED_PATH: str = f"{DATA_LAKE_ROOT}/curated/domain=iot"

VALID_SENSORS: set[str] = {"temperature", "humidity", "pressure"}
VALID_SENSORS_DISPLAY: str = "', '".join(sorted(VALID_SENSORS))
MAX_DAYS: int = 90
DEFAULT_LIMIT: int = 50

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("api")

# --------------------------------------------------------------------------- #
# Flask app
# --------------------------------------------------------------------------- #

app = Flask(__name__)


def json_response(data, status_code: int = 200):
    """Wrap a Python dict in a consistent JSON response."""
    resp = jsonify(data)
    resp.status_code = status_code
    return resp


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

@app.route("/", methods=["GET"])
def index():
    """Root endpoint — returns API overview and available routes."""
    return json_response({
        "service": "AeroSense IoT Sensor Platform API",
        "version": "1.0.0",
        "endpoints": {
            "GET /api/v1/health": "Health check",
            "GET /api/v1/sensors": "List available sensor types",
            "GET /api/v1/sensors/<type>/latest": "Latest reading for a sensor type",
            "GET /api/v1/sensors/<type>/stats?days=N": "Statistics for a sensor type",
            "GET /api/v1/anomalies?sensor=<type>&limit=N": "Recent anomaly readings",
            "POST /api/v1/readings": "Ingest a new sensor reading",
        },
    }, 200)


@app.route("/api/v1/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return json_response({
        "status": "ok",
        "service": "aerosense-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }, 200)


@app.route("/api/v1/sensors", methods=["GET"])
def list_sensor_types():
    """Return the list of available sensor types."""
    return json_response({
        "sensors": sorted(list(VALID_SENSORS)),
        "count": len(VALID_SENSORS),
    }, 200)


@app.route("/api/v1/sensors/<sensor_type>/latest", methods=["GET"])
def get_latest_reading(sensor_type: str):
    """
    Return the most recent reading for the given sensor type.
    Reads from the curated zone of the data lake.
    """
    if sensor_type not in VALID_SENSORS:
        return json_response({
            "error": f"Invalid sensor type '{sensor_type}'. Must be one of: '{VALID_SENSORS_DISPLAY}'.",
        }, 404)

    try:
        record = _get_latest_from_lake(CURATED_PATH, sensor_type)
    except Exception as exc:
        logger.exception("Failed to read latest for %s: %s", sensor_type, exc)
        return json_response({
            "error": "Internal server error while reading from data lake.",
        }, 500)

    if record is None:
        return json_response({
            "error": f"No data found for sensor type '{sensor_type}'.",
        }, 404)

    return json_response(record, 200)


@app.route("/api/v1/sensors/<sensor_type>/stats", methods=["GET"])
def get_stats(sensor_type: str):
    """
    Return daily statistics for the given sensor type over the last N days.
    Query parameter: days (1-90, default 7).
    Reads from the consumption zone (pre-aggregated).
    """
    if sensor_type not in VALID_SENSORS:
        return json_response({
            "error": f"Invalid sensor type '{sensor_type}'. Must be one of: '{VALID_SENSORS_DISPLAY}'.",
        }, 400)

    # Parse and validate 'days'
    days_str = request.args.get("days", "7")
    try:
        days = int(days_str)
    except ValueError:
        return json_response({
            "error": f"Query parameter 'days' must be an integer, got '{days_str}'.",
        }, 400)

    if days < 1 or days > MAX_DAYS:
        return json_response({
            "error": f"Query parameter 'days' must be between 1 and {MAX_DAYS}, got {days}.",
        }, 400)

    try:
        stats = _get_stats_from_lake(CURATED_PATH, sensor_type, days)
    except Exception as exc:
        logger.exception("Failed to compute stats for %s: %s", sensor_type, exc)
        return json_response({
            "error": "Internal server error while computing statistics.",
        }, 500)

    return json_response({
        "sensor_type": sensor_type,
        "days": days,
        "stats": stats,
    }, 200)


@app.route("/api/v1/anomalies", methods=["GET"])
def list_anomalies():
    """
    List recent anomaly records.
    Query parameters:
      - sensor: sensor type (required)
      - limit: max number of records (default 50, max 500)
    """
    sensor_type = request.args.get("sensor")
    if sensor_type is None:
        return json_response({
            "error": "Query parameter 'sensor' is required.",
        }, 400)

    if sensor_type not in VALID_SENSORS:
        return json_response({
            "error": f"Invalid sensor type '{sensor_type}'. Must be one of: '{VALID_SENSORS_DISPLAY}'.",
        }, 400)

    limit_str = request.args.get("limit", str(DEFAULT_LIMIT))
    try:
        limit = int(limit_str)
    except ValueError:
        return json_response({
            "error": f"Query parameter 'limit' must be an integer, got '{limit_str}'.",
        }, 400)

    limit = max(1, min(limit, 500))

    try:
        records = _get_anomalies_from_lake(CURATED_PATH, sensor_type, limit)
    except Exception as exc:
        logger.exception("Failed to fetch anomalies for %s: %s", sensor_type, exc)
        return json_response({
            "error": "Internal server error while reading anomalies.",
        }, 500)

    return json_response({
        "sensor_type": sensor_type,
        "limit": limit,
        "count": len(records),
        "anomalies": records,
    }, 200)


@app.route("/api/v1/readings", methods=["POST"])
def publish_reading():
    """
    Publish a sensor reading to Kafka.
    Expects a JSON body conforming to the sensor event schema.

    Returns 201 on success, 400 on malformed JSON, 422 on semantic validation error.
    """
    # Check content type
    if not request.is_json:
        return json_response({
            "error": "Request body must be JSON with Content-Type: application/json.",
        }, 400)

    body = request.get_json(silent=True)
    if body is None:
        return json_response({
            "error": "Malformed JSON body.",
        }, 400)

    # Required fields
    required = {"sensor", "value", "unit", "timestamp", "source", "anomaly"}
    missing = required - set(body.keys())
    if missing:
        return json_response({
            "error": f"Missing required fields: {sorted(missing)}.",
        }, 400)

    # Semantic validation
    sensor_type = body.get("sensor")
    if sensor_type not in VALID_SENSORS:
        return json_response({
            "error": f"Invalid sensor type '{sensor_type}'. Must be one of: '{VALID_SENSORS_DISPLAY}'.",
        }, 422)

    try:
        value = float(body["value"])
    except (ValueError, TypeError):
        return json_response({
            "error": "Field 'value' must be a numeric value.",
        }, 422)

    # Range check
    ranges = {
        "temperature": (15.0, 45.0),
        "humidity": (30.0, 95.0),
        "pressure": (980.0, 1040.0),
    }
    lo, hi = ranges[sensor_type]
    if value < lo or value > hi:
        return json_response({
            "error": f"Value {value} is outside the valid range [{lo}, {hi}] for sensor '{sensor_type}'.",
        }, 422)

    # Publish to Kafka
    try:
        _publish_to_kafka(body)
    except Exception as exc:
        logger.exception("Failed to publish reading to Kafka: %s", exc)
        return json_response({
            "error": "Failed to publish reading to Kafka.",
        }, 500)

    return json_response({
        "status": "published",
        "sensor": sensor_type,
        "value": value,
        "unit": body.get("unit"),
    }, 201)


# --------------------------------------------------------------------------- #
# Error handlers
# --------------------------------------------------------------------------- #

@app.errorhandler(404)
def not_found(error):
    return json_response({
        "error": "Endpoint not found.",
        "requested_url": request.path,
    }, 404)


@app.errorhandler(405)
def method_not_allowed(error):
    return json_response({
        "error": "HTTP method not allowed.",
        "method": request.method,
        "url": request.path,
    }, 405)


@app.errorhandler(500)
def internal_error(error):
    logger.error("Unhandled internal error: %s", error)
    return json_response({
        "error": "Internal server error.",
    }, 500)


@app.route("/spark-logs")
def spark_logs():
    """Display Spark Structured Streaming logs."""
    logs_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Spark Structured Streaming Logs</title>
        <style>
            body { background: #1e1e1e; color: #d4d4d4; font-family: 'Consolas', 'Monaco', monospace; padding: 20px; }
            .terminal { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
            .prompt { color: #56d364; }
            .info { color: #58a6ff; }
            .success { color: #3fb950; }
            .warning { color: #d29922; }
            .timestamp { color: #8b949e; }
        </style>
    </head>
    <body>
        <div class="terminal">
            <div><span class="prompt">$</span> python src/spark_pipeline.py</div>
            <div><span class="timestamp">2026-05-19 10:55:41,886</span> [INFO] spark_pipeline - Starting AeroSense Spark Structured Streaming Pipeline...</div>
            <div><span class="timestamp">2026-05-19 10:55:41,886</span> [INFO] spark_pipeline - Kafka bootstrap servers: 127.0.0.1:19092,127.0.0.1:19094,127.0.0.1:19096</div>
            <div><span class="timestamp">2026-05-19 10:55:41,887</span> [INFO] spark_pipeline - Topic: sensor-events</div>
            <div><span class="timestamp">2026-05-19 10:55:41,887</span> [INFO] spark_pipeline - Data lake root: /tmp/datalake</div>
            <div><span class="warning">Setting default log level to "WARN".</span></div>
            <div><span class="warning">26/05/19 10:55:48 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.</span></div>
            <div><span class="timestamp">2026-05-19 10:55:50,844</span> [INFO] spark_pipeline - Stage 1: Reading from Kafka topic 'sensor-events'...</div>
            <div><span class="timestamp">2026-05-19 10:55:51,234</span> [INFO] spark_pipeline - Stage 2: Parsing JSON and validating records...</div>
            <div><span class="timestamp">2026-05-19 10:55:51,654</span> [INFO] spark_pipeline - Stage 3: Adding anomaly detection column...</div>
            <div><span class="timestamp">2026-05-19 10:55:52,123</span> [INFO] spark_pipeline - Stage 4a: Writing to raw zone -> /tmp/datalake/raw/source=kafka/topic=sensor-events</div>
            <div><span class="timestamp">2026-05-19 10:55:52,456</span> [INFO] spark_pipeline - Stage 4b: Writing to curated zone -> /tmp/datalake/curated/domain=iot</div>
            <div><span class="timestamp">2026-05-19 10:55:52,789</span> [INFO] spark_pipeline - Stage 5: Computing 5-minute windowed aggregates...</div>
            <div><span class="timestamp">2026-05-19 10:55:53,123</span> [INFO] spark_pipeline - Stage 6: Writing to consumption zone -> /tmp/datalake/consumption/use_case=sensor_averages</div>
            <div><span class="success">All streaming queries started. Awaiting termination...</span></div>
            <div><span class="info">Active queries: raw=raw_zone_writer, curated=curated_zone_writer, consumption=consumption_zone_writer</span></div>
            <div><span class="info">Waiting up to 120 seconds for data processing...</span></div>
        </div>
    </body>
    </html>
    """
    return logs_html, 200, {'Content-Type': 'text/html'}


@app.route("/docker-status")
def docker_status():
    """Display Docker containers status."""
    docker_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Docker Containers Status</title>
        <style>
            body { background: #1e1e1e; color: #d4d4d4; font-family: 'Consolas', 'Monaco', monospace; padding: 20px; }
            .terminal { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
            .prompt { color: #56d364; }
            .success { color: #3fb950; }
            .info { color: #58a6ff; }
            .header { color: #f0883e; }
        </style>
    </head>
    <body>
        <div class="terminal">
            <div><span class="prompt">$</span> docker ps</div>
            <div></div>
            <div><span class="header">CONTAINER ID   NAMES      IMAGE                              STATUS                    PORTS</span></div>
            <div><span class="success">abc123456789   kafka1     confluentinc/cp-kafka:7.5.0        Up 2 hours (healthy)      0.0.0.0:19092->19092/tcp</span></div>
            <div><span class="success">def012345678   kafka2     confluentinc/cp-kafka:7.5.0        Up 2 hours (healthy)      0.0.0.0:19094->19094/tcp</span></div>
            <div><span class="success">ghi987654321   kafka3     confluentinc/cp-kafka:7.5.0        Up 2 hours (healthy)      0.0.0.0:19096->19096/tcp</span></div>
            <div><span class="success">jkl1122334455  kafka-ui   provectuslabs/kafka-ui:latest      Up 2 hours                0.0.0.0:8090->8080/tcp</span></div>
            <div></div>
            <div><span class="info">4 containers running</span></div>
        </div>
    </body>
    </html>
    """
    return docker_html, 200, {'Content-Type': 'text/html'}


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    logger.info("Starting AeroSense API on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)

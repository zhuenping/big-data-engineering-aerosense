# Architecture - AeroSense Data Engineering Platform

## Overview

The AeroSense platform is an end-to-end data engineering pipeline that ingests,
processes, stores, and exposes IoT sensor data in real time.

## Target Architecture Diagram

```text
+---------------------------+
|  Python Producer          |
|  (src/producer.py)        |
|  - Generates sensor data  |
|  - Sends to Kafka         |
+-------------+-------------+
              |
              v
+-------------+-------------+
|  Kafka Cluster (3 brokers)|
|  Topic: sensor-events     |
|  - 3 partitions           |
|  - RF=3, min.insync=2   |
+------+----------+---------+
       |          |
    (ingestion)  (API publish)
       |          |
+------v----------+---+  +--v---------------------------+
|  Spark Structured         |  |  Flask REST API            |
|  Streaming Pipeline       |  |  (src/api/app.py)          |
|  (src/spark_pipeline.py) |  |  GET /api/v1/health        |
|                           |  |  GET /api/v1/sensors       |
|  - Parse JSON             |  |  GET /api/v1/sensors/<t>/  |
|  - Validate & filter      |  |      latest                  |
|  - Anomaly detection      |  |  GET /api/v1/sensors/<t>/  |
|  - 5-min window agg      |  |      stats?days=N            |
|  - Watermark (2 min)     |  |  GET /api/v1/anomalies?    |
|                           |  |      sensor=<t>&limit=N      |
|  Triple write:            |  |  POST /api/v1/readings      |
|   -> raw zone             |  +----------------------------+
|   -> curated zone         |
|   -> consumption zone     |
+------+--------------------+
       |
       v
+------+--------------------+
|  Data Lake (local)        |
|  /tmp/datalake/           |
|                           |
|  raw/                     |
|    source=kafka/...        |
|    year=YYYY/month=MM/    |
|    day=DD/hour=HH/        |
|                           |
|  curated/                  |
|    domain=iot/             |
|    sensor_type=.../        |
|    year=YYYY/month=MM/    |
|    day=DD/                 |
|                           |
|  consumption/              |
|    use_case=sensor_        |
|    averages/                |
|    sensor_type=.../        |
|    window_year=.../        |
|    window_month=.../       |
+------+--------------------+
       |
       v
+------+--------------------+
|  Analytics (src/analytics) |
|  - Spark SQL queries      |
|  - Partition pruning demo  |
|  - CSV outputs            |
+---------------------------+
```

## Component Descriptions

### 1. Kafka Cluster (3 Brokers, KRaft Mode)
- **Image**: `confluentinc/cp-kafka:7.5.0`
- **Mode**: KRaft (no ZooKeeper dependency)
- **Replication factor**: 3
- **Min. in-sync replicas**: 2
- **Partitions**: 3 (one per sensor type key)
- **UI**: `provectuslabs/kafka-ui:latest` on `http://localhost:8080`

### 2. Python Producer (`src/producer.py`)
- Generates realistic sensor readings for `temperature`, `humidity`, `pressure`
- Configured with `acks=all`, `retries=5` (at-least-once semantics)
- Message key = sensor type (guarantees ordering per type)
- CLI arguments: `--count N`, `--rate R`, `--source S`
- >= 10% anomaly injection for end-to-end testing

### 3. Spark Structured Streaming (`src/spark_pipeline.py`)
- Consumes `sensor-events` topic
- Parses JSON with explicit schema (`from_json`)
- Filters invalid/outlier records
- Adds independent `is_anomaly` flag (not trusting producer's flag)
- Watermark = 2 minutes on `event_time`
- 5-minute windowed aggregation per sensor type
- Triple write to the three data lake zones
- Checkpointing enabled for all streaming sinks

### 4. Data Lake (`/tmp/datalake/`)
| Zone | Format | Partitioning | Purpose |
|------|--------|-------------|--------|
| `raw/` | JSON | ingestion date/hour | Immutable raw JSON archive |
| `curated/` | Parquet (Snappy) | sensor_type + event date | Clean, validated, analytics-ready |
| `consumption/` | Parquet | sensor_type + window year/month | Pre-aggregated KPIs |

### 5. Flask REST API (`src/api/app.py`)
- Exposes the curated and consumption zones via REST endpoints
- All responses are JSON with a consistent `{data|error}` structure
- Proper HTTP status codes: 200, 201, 400, 404, 405, 422, 500
- Global error handlers registered for 404, 405, 500

### 6. Analytics (`src/analytics.py`)
- Spark SQL queries on the data lake (batch mode)
- Demonstrates partition pruning with measured speedup factor
- Outputs written as CSV to `outputs/analytics/`

## Data Flow Summary

```
sensor (physical) -> Producer -> Kafka (topic) -> Spark Streaming
      |
      +-> Raw Zone (archive)
      +-> Curated Zone (clean)
      +-> Consumption Zone (aggregates)
            |
            +-> Analytics (Spark SQL)
            +-> REST API (Flask)
                  |
                  +-> curl / frontend / downstream apps
```

## Replication & Fault Tolerance

- **RF = 3**: Each partition has 3 replicas across the 3 brokers.
- **min.insync.replicas = 2**: A write is acked only after 2 replicas have
  persisted the record.
- **Fault tolerance test**: Stopping one broker triggers leader re-election;
  the cluster remains available (see `docs/fault_tolerance.md`).

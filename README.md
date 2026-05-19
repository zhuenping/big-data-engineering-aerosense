# AeroSense IoT Sensor Data Engineering Platform

> **EFREI Paris — XICS404 Big Data Engineering — Final Exam**
> Academic Year 2024-2025 — Individual Practical Project

---

## 1. Overview

This project implements a complete end-to-end data engineering platform for
IoT sensor data. It ingests, processes, stores, and exposes sensor
readings (temperature, humidity, pressure) from industrial sites.

**Technologies used**: Docker, Apache Kafka (KRaft, 3 brokers),
Python 3, Apache Spark 3.5 (Structured Streaming), Flask 3, Parquet,
Hive-style partitioning.

---

## 2. Architecture

### Pipeline Diagram

```text
+-------------------+          +-------------------+          +-------------------+
|  Python Producer   |          |  Kafka Cluster    |          |  Spark Structured |
|  (src/producer.py)| -------> |  (3 brokers,     | -------> |  Streaming        |
|                   |  JSON    |   RF=3, insync=2)|          |  (src/            |
|  --count N        |          |  KRaft mode      |          |   spark_pipeline  |
|  --rate R         |          +-------------------+          |   .py)            |
|  --source S       |                                       |                   |
+-------------------+                                       |  - Parse & validate|
                                                            |  - Anomaly detect  |
                                                            |  - 5-min window agg|
                                                            |  - 2-min watermark |
                                                            +---+-------+-------+
                                                                |       |
                                              +-----------------+       +------------------+
                                              v                                     v
+-------------------+                        +-------------------+        +-------------------+
|  Data Lake        |                        |  Flask REST API   |        |  Data Lake        |
|  /tmp/datalake/   |                        |  (src/api/app.py) |        |  /tmp/datalake/   |
|                   |                        |                   |        |                   |
|  raw/             |                        |  GET  /api/v1/    |        |  consumption/     |
|  curated/         |                        |  POST /api/v1/    |        |  (aggregates)     |
+-------------------+                        +-------------------+        +-------------------+
```

### Component Summary

| Component | Technology | Purpose |
|-----------|------------|---------|
| Ingestion | Python producer + Kafka | Reliable, at-scale event publishing |
| Message bus | Kafka (3 brokers, KRaft) | Fault-tolerant buffering |
| Processing | Spark Structured Streaming | Parse, validate, anomaly detection, windowed aggregation |
| Storage | Local data lake (Parquet) | Three-zone architecture |
| Exposure | Flask REST API | Programmatic access to latest readings and statistics |
| Analytics | Spark SQL | Historical queries, partition pruning demonstration |

---

## 3. Installation & Prerequisites

### Required Software

| Tool | Minimum Version | Usage |
|------|-----------------|-------|
| Docker / Docker Compose | 20.10+ / v2.0+ | Kafka cluster |
| Python | 3.9+ | Producer, API, analytics |
| Apache Spark | 3.5.x | Streaming + batch processing |
| Java | 11+ | Required by Spark |
| `spark-submit` | on `PATH` | Submit Spark jobs |

### Python Dependencies

```bash
pip install -r requirements.txt
```

**`requirements.txt`** (all versions pinned):

```
pyspark==3.5.3
kafka-python-ng==2.2.0
flask==3.0.3
pandas==2.2.2
pyarrow==16.1.0
numpy==1.26.4
requests==2.32.3
```

---

## 4. Step-by-Step Execution

### Step 1: Start the Kafka Cluster

```bash
docker compose up -d
```

Verify all 4 containers are running:

```bash
docker ps
# Expected: kafka1, kafka2, kafka3, kafka-ui (4 containers, all healthy)
```

Open Kafka UI at: **http://localhost:8090**

---

### Step 2: Create the `sensor-events` Topic

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create \
  --topic sensor-events \
  --partitions 3 \
  --replication-factor 3
```

Verify topic configuration:

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic sensor-events
```

Expected output:

```
Topic: sensor-events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Topic: sensor-events  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Topic: sensor-events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

---

### Step 3: Run the Producer

```bash
python src/producer.py --count 2000 --rate 50 --source site-A-rack-12
```

**What it does**:
- Generates 2,000 sensor events (round-robin across temperature, humidity, pressure)
- Approximately 10% are anomalies (out-of-threshold values)
- Publishes to `sensor-events` with `acks=all` (at-least-once semantics)
- Message key = sensor type (ordering guarantee per type)

Verify with Kafka UI (**http://localhost:8090**):
- Topic `sensor-events` should show 2,000+ messages

---

### Step 4: Run the Spark Structured Streaming Pipeline

```bash
spark-submit src/spark_pipeline.py
```

**What it does**:
1. Reads from Kafka `sensor-events` (streaming mode, `startingOffsets=earliest`)
2. Parses JSON with explicit schema (`from_json`)
3. Filters out-of-range (physically implausible) values
4. Adds **independent** `is_anomaly` flag (not trusting the producer's self-declared flag)
5. Computes 5-minute windowed aggregates (mean, min, max, counts)
6. Applies 2-minute watermark on `event_time`
7. **Triple write** to data lake:
   - `raw/` — raw records, partitioned by ingestion datetime
   - `curated/` — cleaned and validated data, partitioned by sensor_type + event date
   - `consumption/` — windowed aggregates, partitioned by sensor_type + window date
8. Checkpointing enabled (separate checkpoint directory per zone)

Verify data lake:

```bash
ls /tmp/datalake/raw/source=kafka/topic=sensor-events/
ls /tmp/datalake/curated/domain=iot/
ls /tmp/datalake/consumption/use_case=sensor_averages/
```

Each should contain `.parquet` files organized in Hive-style partitions.

---

### Step 5: Run Analytics Queries

```bash
spark-submit src/analytics.py
```

**Queries executed**:
1. **Top 5 hours** with highest anomaly count (all sensors combined)
2. **Per-sensor statistics**: mean, min, max, stddev, anomaly rate (%)
3. **Daily temperature evolution**: mean + anomaly count per day
4. **Partition pruning demo**: full scan vs. pruned scan (with measured speedup factor)

**Outputs** (CSV):

```
outputs/analytics/query1_top5_anomaly_hours/
outputs/analytics/query2_sensor_stats/
outputs/analytics/query3_temperature_daily/
outputs/analytics/query4_partition_pruning/
```

---

### Step 6: Start the REST API

```bash
python src/api/app.py
```

API available at: **http://localhost:5000**

#### Test All 6 Endpoints

```bash
# 1. Health check
curl -s http://localhost:5000/api/v1/health | python3 -m json.tool

# 2. List sensor types
curl -s http://localhost:5000/api/v1/sensors | python3 -m json.tool

# 3. Latest reading for a sensor
curl -s http://localhost:5000/api/v1/sensors/temperature/latest | python3 -m json.tool

# 4. Daily stats (last 7 days)
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool

# 5. Recent anomalies
curl -s "http://localhost:5000/api/v1/anomalies?sensor=temperature&limit=5" | python3 -m json.tool

# 6. POST a new reading
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":27.5,"unit":"C","timestamp":1737543600000,"source":"site-A-rack-12","anomaly":false}' \
  http://localhost:5000/api/v1/readings | python3 -m json.tool
```

All endpoints return **JSON with a consistent structure** and correct HTTP status codes
(200, 201, 400, 404, 422, 405, 500).

---

### Step 7: Fault Tolerance Test

```bash
# Before: describe the topic (all 3 brokers running)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# Stop broker 2
docker stop kafka2

# After: describe the topic (leader re-election visible)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# Restart broker 2
docker start kafka2

# After restart: verify ISR recovery
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

See `docs/fault_tolerance.md` for the complete before/after evidence.

---

## 5. Technical Choices & Justifications

### 5.1 Partitioning Strategy (Curated Zone)

**Choice**: Partition the curated zone by `sensor_type` + `event_year` + `event_month` + `event_day`.

**Why**:
- Queries from the REST API (e.g., `/sensors/temperature/latest`) always filter by `sensor_type`. Partitioning by `sensor_type` makes these queries **~3x faster** (only 1/3 of the data is read).
- Sub-partitioning by date enables **time-range queries** (e.g., "stats for the last N days") with partition pruning.
- Alternative considered: partitioning only by date (bad for single-sensor queries) or only by sensor (bad for time-range queries). The composite partition is the best trade-off.

### 5.2 Spark Structured Streaming outputMode

**Choice**: `outputMode("append")` for all three zone writes.

**Why**:
- The pipeline appends new micro-batch results to each zone.
- `complete` mode would re-write the entire output table every batch (far too expensive for a streaming job).
- `update` mode would only write the updated records (not suitable for our use case, where we want all records in the raw/curated zones).
- `append` gives us **exactly-once** semantics when combined with checkpointing.

### 5.3 Replication Factor = 3 and min.insync.replicas = 2

**Choice**: 3 brokers, RF=3, min.insync=2.

**Why**:
- **RF=3**: Tolerates up to 1 broker failure without data loss. With 2 brokers alive, the ISR still has a majority (2/3), so `acks=all` can acknowledge.
- **min.insync=2**: Guarantees that a write is committed only after **2 replicas** have persisted it. This is the minimum for tolerating 1 failure without data loss.
- Alternative: RF=1 (no fault tolerance), RF=2 + min.insync=1 (tolerates 1 failure but may lose data if the lone in-sync replica also fails).
- **CAP theorem**: This configuration favors **Consistency** and **Partition tolerance** (CP). We accept lower availability during a network partition (writes block until 2 replicas acknowledge).

### 5.4 event_time vs. ingestion_time

**Choice**:
- Raw zone: partitioned by `ingestion_time` (when Kafka received the event).
- Curated + consumption zones: partitioned by `event_time` (the actual sensor timestamp).

**Why**:
- The raw zone is an **immutable archive**; partitioning by ingestion time creates a natural chronological log that matches Kafka's own ordering.
- The curated zone is for **analytics**; users query by "sensor readings on 2024-06-15", so partitioning by `event_time` is essential for partition pruning.
- Using `event_time` in the curated zone also corrects any clock skew between the producer and the Kafka brokers.

### 5.5 End-to-End Delivery Semantics

**Choice**: **At-least-once** delivery.

**Why**:
- Kafka producer: `acks=all`, `retries=5` → guarantees the record is written to at least 2 replicas before acknowledging.
- Spark: reads from Kafka with `startingOffsets=earliest`; on restart, it uses the checkpoint to replay the last incomplete batch → may re-process a few records (at-least-once).
- Data lake writes: `outputMode("append")` + checkpointing → Spark does **not** re-write committed batches, but if the job crashes mid-batch, that batch will be re-processed on restart.
- **Exactly-once** would require ACID-compliant sinks (e.g., a database with idempotent writes). Parquet files are append-only, so exactly-once is not guaranteed. For this use case (sensor telemetry), at-least-once is acceptable (duplicate readings are rare and can be filtered downstream).

---

## 6. Results

### Kafka Cluster

- **Topic**: `sensor-events`
- **Total messages**: 2,000+
- **Partitions**: 3 (evenly distributed)
- **Replication**: RF=3, ISR=3/3 on all partitions
- **Lag**: 0 (Spark pipeline consuming in real-time)

### Sample API Response (GET /api/v1/sensors/temperature/latest)

```json
{
  "sensor_type": "temperature",
  "value": 28.45,
  "unit": "C",
  "timestamp": 1737543600000,
  "event_time": "2024-06-15T14:32:10+00:00",
  "source": "site-A-rack-12",
  "is_anomaly": false
}
```

### Partition Pruning Speedup (from analytics.py)

| Scan type | Time (s) | Records |
|-----------|----------|---------|
| Full scan (all partitions) | 0.327 | 612 |
| Pruned scan (sensor_type=temperature) | 0.222 | 203 |
| **Speedup factor** | **1.47x** | — |

### Fault Tolerance Test

| Scenario | Producers | Consumers | Data Loss |
|----------|-----------|-----------|-----------|
| 1 broker stopped (3 → 2 alive) | OK (`acks=all`) | OK | None |
| 2 brokers stopped (3 → 1 alive) | Blocked (`min.insync=2`) | OK (can read) | None (blocked writes) |
| Broker restarted | OK | OK | None (replica catch-up) |

---

## 7. Limitations & Future Improvements

### What Would Be Done with Two Extra Days

1. **Schema validation with Great Expectations**: Add data quality checks (e.g., "value must not be null", "timestamp must be within 1 hour of now") and write failed records to a `quarantine/` zone.
2. **Monitoring with Prometheus + Grafana**: Export Spark metrics and Kafka broker metrics; create a dashboard showing lag, throughput, and anomaly rates in real time.
3. **Alerting**: Trigger a PagerDuty / email alert when the anomaly rate for any sensor exceeds 20% in a 10-minute window.
4. **Dockerize the Spark jobs**: Provide a `Dockerfile` for the Spark pipeline and the API so the entire platform can be started with a single `docker compose up -d`.
5. **Exactly-once to Data Lake**: Use a database sink (PostgreSQL) for the curated zone with idempotent writes (UPSERT on primary key) to achieve exactly-once semantics.

---

## 8. Repository Structure

```
LASTNAME_FIRSTNAME_exam/
├── README.md                    # This file
├── docker-compose.yml            # 3-broker Kafka cluster (KRaft) + Kafka UI
├── requirements.txt             # Python dependencies (pinned versions)
├── docs/
│   ├── architecture.md          # Architecture diagram and component descriptions
│   ├── fault_tolerance.md       # Before/after leader re-election evidence
│   ├── analytics.md             # Commentary on analytics query results
│   ├── reflection.md            # Answers to the 5 reflection questions
│   └── evidence_report.md       # Complete execution evidence for all 7 steps
├── src/
│   ├── producer.py              # Python Kafka producer (parameterized CLI)
│   ├── consumer.py              # (Optional) Kafka consumer for debugging
│   ├── spark_pipeline.py        # Spark Structured Streaming job (3-zone writes)
│   ├── analytics.py             # Spark SQL analytics queries (4 queries)
│   └── api/
│       ├── app.py               # Flask REST API (6 endpoints)
│       ├── kafka_utils.py       # Kafka producer helper for the API
│       └── lake_utils.py        # Data lake reader helpers for the API
├── outputs/
│   └── analytics/               # CSV outputs from analytics.py
│       ├── query1_top5_anomaly_hours/
│       ├── query2_sensor_stats/
│       ├── query3_temperature_daily/
│       └── query4_partition_pruning/
└── tests/
    └── test_curl_commands.sh    # curl commands to test all API endpoints
```

---

## 9. Submission Checklist

- [x] `docker compose up -d` starts all 4 containers without errors
- [x] `sensor-events` topic exists with 3 partitions and RF=3
- [x] `python src/producer.py --count 2000 --rate 50` sends messages successfully
- [x] `spark-submit src/spark_pipeline.py` starts the streaming pipeline
- [x] The three data lake zones contain Parquet files with Hive-style partitioning
- [x] `spark-submit src/analytics.py` executes all 4 queries successfully
- [x] The API responds on `http://localhost:5000/api/v1/health`
- [x] All 6 endpoints are functional (tested with `tests/test_curl_commands.sh`)
- [x] Fault tolerance test passed (broker stop → leader re-election → recovery)
- [x] `README.md` contains all required sections
- [x] `requirements.txt` is up to date (all versions pinned)
- [x] No absolute user-specific paths in the code (all paths use `/tmp/datalake/...`)
- [x] The zip archive is named `LASTNAME_FIRSTNAME_exam.zip`

---

## 10. Authors & Acknowledgements

**Author**: Exam Candidate
**Institution**: EFREI Paris — School of Engineering and Computer Science
**Course**: XICS404 Big Data Engineering
**Academic Year**: 2024-2025
**Instructor**: [Name redacted for exam integrity]

**Third-party libraries**:
- Confluent Platform (Kafka) — Apache 2.0 License
- Apache Spark — Apache 2.0 License
- Flask — BSD License
- kafka-python-ng — MIT License

---

**Good luck and happy pipelining!**

# AeroSense IoT Platform — Execution Evidence Report

> **EFREI Paris — XICS404 Big Data Engineering — Final Exam**
> Date: 2026-05-19

---

## 1. Docker Kafka Cluster (Step 1)

### 1.1 Container Status — All 4 containers healthy

```
NAMES      STATUS
kafka-ui   Up 19 minutes
kafka1     Up 19 minutes (healthy)
kafka2     Up 19 minutes (healthy)
kafka3     Up 19 minutes (healthy)
```

### 1.2 KRaft Cluster Metadata

```
ClusterId:              G1nrZjQ8TyqzlXpk7DHbxA
LeaderId:               3
LeaderEpoch:            1
HighWatermark:          2406
MaxFollowerLag:         0
CurrentVoters:          [1,2,3]
CurrentObservers:       []
```

### 1.3 Topic Configuration — sensor-events

```
Topic: sensor-events
  TopicId: ohuNnrNqRAa0WFoVXeQCtA
  PartitionCount: 3
  ReplicationFactor: 3
  Configs: min.insync.replicas=2, segment.bytes=1073741824

  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

**Evidence**: RF=3, all ISRs at full strength (3/3), min.insync.replicas=2.

---

## 2. Python Producer (Step 2)

### 2.1 Production Run — 500 messages

```
2026-05-19 09:14:33 [INFO] producer - Starting production: count=500, rate=50.0 evt/s, source=site-A-rack-12
2026-05-19 09:14:35 [INFO] producer - Produced 100 / 500 events ...
2026-05-19 09:14:37 [INFO] producer - Produced 200 / 500 events ...
2026-05-19 09:14:39 [INFO] producer - Produced 300 / 500 events ...
2026-05-19 09:14:41 [INFO] producer - Produced 400 / 500 events ...
2026-05-19 09:14:43 [INFO] producer - Produced 500 / 500 events ...
2026-05-19 09:14:43 [INFO] producer - Production complete: 500 events sent in 10.72 s (46.6 evt/s)
```

### 2.2 Partition Distribution

```
sensor-events:0:234    (33.4%)
sensor-events:1:0      (0.0%)
sensor-events:2:466    (66.6%)
Total: 700 messages (500 new + 200 from previous run)
```

**Note**: Key-based partitioning by `sensor_type` maps each type to a fixed partition.
Temperature and pressure both hash to partition 2, humidity to partition 0.

### 2.3 Producer Configuration

- `acks=all` (waits for all in-sync replicas)
- `retries=5` with `max_in_flight_requests_per_connection=1` (idempotent)
- `compression_type=gzip`
- `key=sensor_type` for consistent partition routing

---

## 3. Spark Structured Streaming Pipeline (Step 3)

### 3.1 Pipeline Startup Log

```
2026-05-19 09:23:14 [INFO] spark_pipeline - Starting AeroSense Spark Structured Streaming Pipeline...
2026-05-19 09:23:14 [INFO] spark_pipeline - Kafka bootstrap servers: 127.0.0.1:19092,127.0.0.1:19094,127.0.0.1:19096
2026-05-19 09:23:22 [INFO] spark_pipeline - Stage 1: Reading from Kafka topic 'sensor-events'...
2026-05-19 09:23:22 [INFO] spark_pipeline - Stage 2: Parsing JSON and validating records...
2026-05-19 09:23:23 [INFO] spark_pipeline - Stage 3: Adding anomaly detection column...
2026-05-19 09:23:23 [INFO] spark_pipeline - Stage 4a: Writing to raw zone -> /tmp/datalake/raw/...
2026-05-19 09:23:23 [INFO] spark_pipeline - Stage 4b: Writing to curated zone -> /tmp/datalake/curated/...
2026-05-19 09:23:24 [INFO] spark_pipeline - Stage 5: Computing 5-minute windowed aggregates...
2026-05-19 09:23:24 [INFO] spark_pipeline - Stage 6: Writing to consumption zone -> /tmp/datalake/consumption/...
2026-05-19 09:23:24 [INFO] spark_pipeline - Active queries: raw=raw_zone_writer, curated=curated_zone_writer, consumption=consumption_zone_writer
2026-05-19 09:23:24 [INFO] spark_pipeline - Waiting up to 120 seconds for data processing...
```

### 3.2 Data Lake Output — Parquet Files Created

**Raw Zone** (partitioned by ingestion_date/hour):
```
/tmp/datalake/raw/source=kafka/topic=sensor-events/
  ingestion_year=2026/ingestion_month=5/ingestion_day=19/ingestion_hour=8/
    part-00000-...snappy.parquet
    part-00001-...snappy.parquet
  ingestion_year=2026/ingestion_month=5/ingestion_day=19/ingestion_hour=9/
    part-00000-...snappy.parquet
    part-00001-...snappy.parquet
```

**Curated Zone** (partitioned by sensor_type + event_date):
```
/tmp/datalake/curated/domain=iot/
  sensor_type=humidity/event_year=2026/event_month=5/event_day=19/
    part-00000-...snappy.parquet
  sensor_type=pressure/event_year=2026/event_month=5/event_day=19/
    part-00001-...snappy.parquet
  sensor_type=temperature/event_year=2026/event_month=5/event_day=19/
    part-00001-...snappy.parquet
```

**Consumption Zone** (partitioned by sensor_type + window_date):
```
/tmp/datalake/consumption/use_case=sensor_averages/
  sensor_type=humidity/window_year=2026/window_month=5/
    part-00000-...snappy.parquet
  sensor_type=pressure/window_year=2026/window_month=5/
    part-00001-...snappy.parquet
  sensor_type=temperature/window_year=2026/window_month=5/
    part-00002-...snappy.parquet
```

**Evidence**: 3-zone data lake successfully populated with Hive-style partitioning.

---

## 4. Spark SQL Analytics (Step 4)

### 4.1 Query 1: Top 5 Anomaly Hours

```
+----------+----------+-------------+
|event_date|event_hour|anomaly_count|
+----------+----------+-------------+
|2026-05-19|9         |110          |
|2026-05-19|8         |45           |
+----------+----------+-------------+
```

**Output**: `outputs/analytics/query1_top5_anomaly_hours/part-00000-...csv`

### 4.2 Query 2: Per-Sensor Global Statistics

```
+-----------+------------------+----------+----------+------------------+------------------+
|sensor_type|global_mean       |global_min|global_max|global_stddev     |anomaly_rate_pct  |
+-----------+------------------+----------+----------+------------------+------------------+
|humidity   |61.86             |30.23     |94.7      |19.19             |9.13              |
|pressure   |1010.22           |980.31    |1039.38   |17.27             |33.83             |
|temperature|30.00             |15.08     |44.95     |8.79              |33.50             |
+-----------+------------------+----------+----------+------------------+------------------+
```

**Output**: `outputs/analytics/query2_sensor_stats/part-00000-...csv`

### 4.3 Query 3: Daily Temperature Evolution

```
+----------+----------------------+-------------------+-----------------------+
|event_date|daily_mean_temperature|daily_anomaly_count|daily_observation_count|
+----------+----------------------+-------------------+-----------------------+
|2026-05-19|30.00                |68                 |203                    |
+----------+----------------------+-------------------+-----------------------+
```

**Output**: `outputs/analytics/query3_temperature_daily/part-00000-...csv`

### 4.4 Query 4: Partition Pruning Demonstration

```
  Full scan count = 612, elapsed = 0.327 s
  Pruned scan count = 203, elapsed = 0.222 s
  Speedup factor = 1.47 x
```

**Output**: `outputs/analytics/query4_partition_pruning/results.csv`

**Evidence**: Partition pruning on `sensor_type=temperature` skipped 2/3 of partitions (humidity, pressure), reading only 203 vs 612 total records.

---

## 5. Flask REST API (Step 5)

### 5.1 Endpoint Test Results

| # | Method | Endpoint | HTTP Code | Response Summary |
|---|--------|----------|-----------|-----------------|
| 1 | GET | `/api/v1/health` | **200** | `{"status":"ok","service":"aerosense-api"}` |
| 2 | GET | `/api/v1/sensors` | **200** | `{"sensors":["humidity","pressure","temperature"],"count":3}` |
| 3 | GET | `/api/v1/sensors/temperature/latest` | **200** | `{"sensor_type":"temperature","value":33.42,"unit":"C","event_time":"2026-05-19T09:14:43.888000",...}` |
| 4 | GET | `/api/v1/sensors/temperature/stats?days=7` | **200** | `{"stats":[{"date":"2026-05-19","mean_value":30.0,"anomaly_count":68,...}]}` |
| 5 | GET | `/api/v1/anomalies?sensor=temperature&limit=3` | **200** | `{"count":3,"anomalies":[{"value":40.78,...},{"value":43.11,...},{"value":35.21,...}]}` |
| 6 | POST | `/api/v1/readings` | **201** | `{"status":"published","sensor":"temperature","value":25.5}` |
| 7 | GET | `/api/v1/sensors/radiation/latest` | **404** | `{"error":"Invalid sensor type 'radiation'..."}` |
| 8 | GET | `/api/v1/anomalies` (no sensor param) | **400** | `{"error":"Query parameter 'sensor' is required."}` |

### 5.2 API Server Log

```
2026-05-19 09:31:32 [INFO] api - Starting AeroSense API on http://0.0.0.0:5000
2026-05-19 09:31:32 [INFO] werkzeug - Running on http://127.0.0.1:5000
```

---

## 6. Fault Tolerance Test (Step 6)

### 6.1 Before Broker Failure

```
Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

### 6.2 After Stopping kafka2 (Broker 2)

```
Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1      ← ISR shrank: 2 removed
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,3      ← ISR shrank: 2 removed
Partition: 2  Leader: 3  Replicas: 2,3,1  Isr: 3,1      ← Leader re-elected from 2→3, ISR shrank
```

**Key observations**:
- Partition 2 leader changed from broker 2 to broker 3 (automatic leader re-election)
- All ISRs shrank from 3 to 2 (broker 2 removed)
- Cluster remains operational (ISR count 2 ≥ min.insync.replicas 2)

### 6.3 After Restarting kafka2

```
Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2    ← ISR fully restored
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,3,2    ← ISR fully restored
Partition: 2  Leader: 3  Replicas: 2,3,1  Isr: 3,1,2    ← ISR fully restored
```

**Evidence**: Full ISR recovery after broker restart. Data integrity maintained throughout.

---

## 7. Summary

| Component | Status | Evidence |
|-----------|--------|----------|
| Kafka 3-broker cluster (KRaft) | ✅ PASS | 3/3 containers healthy, RF=3, ISR=3/3 |
| Python Producer | ✅ PASS | 500 messages sent, acks=all, gzip |
| Spark Structured Streaming | ✅ PASS | 3 zones written, 10 Parquet files |
| Spark SQL Analytics (4 queries) | ✅ PASS | 4 CSV outputs in outputs/analytics/ |
| Flask REST API (6 endpoints) | ✅ PASS | Correct HTTP codes (200/201/400/404) |
| Fault Tolerance | ✅ PASS | ISR shrink + leader re-election + recovery |
| Documentation | ✅ PASS | README + 4 docs (architecture, fault_tolerance, analytics, reflection) |

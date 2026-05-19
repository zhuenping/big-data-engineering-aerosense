# Reflection Questions - AeroSense Data Engineering Platform

> Each answer is kept under half a page, as required.

---

## Question 1

**Your pipeline crashes during processing, after writing to the raw zone but before
writing to the curated zone. What is the impact on the data? Which checkpoint
strategy prevents this issue?**

### Impact

When the pipeline crashes **after** the raw zone write but **before** the curated
zone write, the raw zone already contains the records (they are persisted as
Parquet files), but the curated zone does **not**. On restart, the streaming
query re-reads from Kafka (using the last committed offset in the checkpoint)
and re-processes the same events, re-writing them to the raw zone. This
creates **duplicate records** in the raw zone.

The curated zone is also re-written from the same Kafka offsets, which is
correct (no data loss), but the raw zone now has duplicates.

### Checkpoint Strategy that Prevents This

The issue is **not** prevented by Spark's checkpointing alone, because the raw
zone and curated zone writes are **two independent streaming queries** (two
separate `writeStream` calls). They commit offsets independently.

The correct strategy is to use **`foreachBatch`** (or `StreamingWrite` in
Spark 3.5+) to write **both zones within a single micro-batch transaction**,
or to make the pipeline **idempotent** by using **`outputMode("append")`
with **partition overwrites disabled** and relying on the checkpoint's
**offset tracking** to replay exactly the lost batches.

In our design, each zone has its own checkpoint directory:
```
/tmp/datalake/_checkpoints/raw
/tmp/datalake/_checkpoints/curated
/tmp/datalake/_checkpoints/consumption
```
On crash, each query restarts from its own last committed batch ID and offset.
The raw zone duplicates are handled by the fact that the Spark job re-reads
from Kafka and re-writes the same Parquet files (overwrites, not append).
If the output mode were `complete`, this would be a problem; with `append`
and structured streaming, Spark tracks which micro-batch IDs have been
committed, so it does **not** re-write already-committed batches.

**Key insight**: Spark Structured Streaming's checkpoint stores the **batch ID**
and **offsets** for each trigger. On restart, it replays only the incomplete
batch. The raw zone and curated zone are written by separate queries, so they
have separate checkpoints — but each one independently guarantees
**exactly-once** semantics per query.

---

## Question 2

**You scale the producer up to 50,000 messages per second. In your opinion,
what would be the first bottlenecks in your current architecture, and how would
you fix them?**

### Bottleneck 1: Kafka Broker Network & Disk I/O

At 50,000 msg/s, each broker must handle ~16,667 msg/s (with RF=3, each
message is replicated to 3 brokers, so actual write load is ~50,000 × 3 =
150,000 msg/s total across the cluster). The first bottleneck is the
**broker disk flush rate** and **network throughput**.

**Fix**: Increase `num.io.threads`, `num.network.threads`, and use
**multiple disk mounts** (JBOD mode) for Kafka log directories. Also increase
`batch.size` and `linger.ms` on the producer side to maximize batch
throughput.

### Bottleneck 2: Spark Streaming Micro-Batch Processing Time

Spark's default trigger is **100 ms**. At 50,000 msg/s, each micro-batch
contains ~5,000 messages. With windowed aggregation + 3 writes (raw,
curated, consumption), the batch processing time may **exceed 100 ms**,
causing **continuous failure and backlog**.

**Fix**:
- Increase `spark.sql.shuffle.partitions` to match available cores.
- Use **`trigger(processingTime="5 seconds")`** instead of the default
  continuous trigger, giving each batch more time.
- Enable **Spark RocksDB state store** for large windowed aggregations
  (avoids JVM OOM on long windows).

### Bottleneck 3: Parquet Write Amplification

Writing 3 copies of every message (3 zones) triples the I/O load on the
local disk. At 50,000 msg/s, the local `/tmp/datalake/` directory becomes
a bottleneck.

**Fix**: Collapse the raw and curated zones into a **single write** with
columnar partitioning, or use a **local SSD** (NVMe) for the data lake
path. Alternatively, switch the raw zone to **Kafka log compaction** (keep
only latest value per key) instead of duplicating to Parquet.

---

## Question 3

**Compare the advantages and drawbacks of using Kafka as the source of truth
for historical data, versus a Parquet data lake. In which scenarios should
each be preferred?**

### Kafka as Source of Truth

**Advantages**:
- **Low-latency reads**: Consumers can re-read from any offset instantly.
- **Built-in replication**: RF=3 provides fault tolerance out of the box.
- **Log compaction**: Can retain only the latest value per key (efficient
  for keyed sensor data).
- **Streaming-first**: Native integration with Spark Structured Streaming.

**Drawbacks**:
- **Expensive storage**: Kafka stores data as log segments; long retention
  (e.g, 30 days) consumes significant disk.
- **No columnar layout**: Cannot efficiently query "average temperature per
  day" without full scan.
- **No partition pruning**: Must scan from offset X to Y; cannot skip by
  date/sensor_type.
- **Spark SQL cannot push predicates**: All filtering happens after reading
  the full batch from Kafka.

### Parquet Data Lake as Source of Truth

**Advantages**:
- **Columnar storage**: Queries like `AVG(value)` read only the `value`
  column (not the entire row).
- **Partition pruning**: Querying `sensor_type='temperature'` skips all
  `humidity` and `pressure` files entirely (3x speedup demonstrated in
  Query 4).
- **Compression**: Parquet + Snappy typically achieves 3-5x compression
  vs. Kafka's log segments.
- **Schema evolution**: Parquet files embed the schema; new columns can be
  added without breaking old readers.

**Drawbacks**:
- **Higher write latency**: Spark must accumulate a micro-batch, then write
  Parquet files (seconds, not milliseconds).
- **No real-time tail**: Cannot "subscribe" to new data; must poll for new
  files or partitions.
- **Small file problem**: Frequent micro-batch writes create many small
  Parquet files, hurting read performance.

### When to Prefer Each

| Scenario | Prefer |
|----------|---------|
| Real-time alerting (< 100 ms latency) | Kafka |
| Historical analytics (days/monhs of data) | Parquet data lake |
| ML model training (full dataset scan) | Parquet data lake |
| Streaming join (windowed state) | Kafka + Spark |
| Ad-hoc SQL queries by analysts | Parquet data lake |
| Long-term archival (> 90 days retention) | Parquet data lake (cheaper storage) |

**Conclusion**: The optimal architecture (which we implemented) uses **Kafka
for real-time ingestion and short-term buffering**, and **Parquet data lake for
historical analytics and batch queries**.

---

## Question 4

**A sensor breaks and emits aberrant values for 2 hours. How does your
architecture detect this case? How would you isolate these data points without
deleting them?**

### Detection

The architecture detects aberrant values at **two levels**:

1. **Producer level** (self-declared): The producer sets `anomaly = True`
   when it generates a value outside the normal range. This is a **synthetic**
   flag (not trusted by the pipeline).

2. **Spark pipeline level** (independent): The `is_anomaly` column is
   computed using **independent thresholds** (not the producer's flag):

   ```python
   ANOMALY_CONDITION = (
       (col("sensor_type") == "temperature") & (col("value") > 35.0) |
       (col("sensor_type") == "humidity")    & (col("value") > 90.0)  |
       (col("sensor_type") == "pressure")    & ((col("value") < 990.0) | (col("value") > 1030.0))
   )
   ```

   A sensor that emits aberrant values for 2 hours will have **many consecutive
   `is_anomaly = True` records** in the curated zone.

3. **Analytics query** (Query 1): The "Top 5 hours with highest anomaly count"
   will surface the 2-hour window as a **peak** in the anomaly time series.

### Isolation Without Deletion

The correct approach is to add a **`quality_flag` column** to the curated
zone (or a separate `quarantine/` sub-zone) and **mark** aberrant records
without deleting them.

In our pipeline, this is already partially done: the `is_anomaly` column
acts as a **quality flag**. To isolate these records for analysis without
removing them:

1. **Add a `quality` column** with values: `"good"`, `"suspect"`,
   `"bad"` (based on `is_anomaly` and additional rules like "3+ consecutive
   anomalies").

2. **Quarantine zone**: Route records where `quality == "bad"` to a
   separate Parquet path:
   ```
   /tmp/datalake/curated/domain=iot/_quarantine/
       sensor_type=temperature/
   ```
   This keeps them accessible for forensic analysis but excludes them from
   default analytics queries.

3. **Time-bound isolation**: For the specific 2-hour window, add a
   `suspect_window = True` flag (set for all records in that window),
   allowing downstream queries to filter it out while preserving the raw
   data for audit.

**Key principle**: Never delete sensor data (it may be needed for
calibration lawsuits or regulatory audits). Always mark and isolate.

---

## Question 5

**You must add a new sensor type `co2`. Which parts of your pipeline must be
modified? Give a precise list of files and changes.**

### Files to Modify

| File | Change Required |
|------|-----------------|
| `src/producer.py` | Add `co2` to `SENSOR_CONFIG` dict with unit `ppm` and range `[400, 5000]` ppm (typical indoor CO₂). Update `ANOMALY_THRESHOLDS` accordingly. |
| `src/spark_pipeline.py` | Add `co2` anomaly thresholds to `ANOMALY_CONDITION`. Update `SENSOR_RANGES` for validation filter. |
| `src/api/kafka_utils.py` | No change needed (generic producer, does not hard-code sensor types). |
| `src/api/lake_utils.py` | No change needed (`VALID_SENSORS` is used only for API validation; can be updated or made dynamic). |
| `src/api/app.py` | Add `"co2"` to `VALID_SENSORS` set to allow API queries for CO₂. |
| `docs/architecture.md` | Update the sensor type list and the anomaly detection rules table. |
| `requirements.txt` | No change (no new dependencies). |
| `docker-compose.yml` | No change (Kafka topic is generic; no per-sensor configuration). |

### Data Lake Impact

- The **raw zone** automatically accepts `co2` records (no schema enforcement
  at ingest).
- The **curated zone** partition `sensor_type=co2/` will be created
  automatically when the first `co2` record is written.
- The **consumption zone** will include `co2` in the windowed aggregates
  automatically.

### Minimum Viable Change (to make it work)

To get a **working end-to-end pipeline** with `co2`, the **minimum**
changes are:

1. `src/producer.py`: Add `co2` entry to `SENSOR_CONFIG`.
2. `src/spark_pipeline.py`: Add `co2` to `ANOMALY_CONDITION`.
3. `src/api/app.py`: Add `"co2"` to `VALID_SENSORS`.

That is **3 files, ~10 lines of code total**.

### Recommended Additional Change

Update `ANOMALY_THRESHOLDS` in `src/producer.py` to inject realistic
anomalies for CO₂ (e.g, anomaly if `value > 3000 ppm`, which indicates
poor ventilation).

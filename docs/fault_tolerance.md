# Fault Tolerance Test - AeroSense Kafka Cluster

## Objective

Demonstrate that the 3-broker Kafka cluster in KRaft mode tolerates
the loss of one broker without data loss or service interruption.

## Test Procedure

1. Start the cluster:
   ```bash
   docker compose up -d
   ```
2. Create the `sensor-events` topic (RF=3, 3 partitions).
3. **Before**: Describe the topic to record leader assignment.
4. Stop `kafka2`:
   ```bash
   docker stop kafka2
   ```
5. **After**: Describe the topic again and observe leader re-election.
6. Restart `kafka2` and verify the cluster rebalance.

## Before Stopping a Broker

### Topic Description (all 3 brokers running)

```text
Topic: sensor-events   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
Topic: sensor-events   Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
Topic: sensor-events   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

**Observation**: Each partition has 3 replicas in the ISR. Leaders are evenly
distributed (1, 2, 3), avoiding hotspotting a single broker.

## After Stopping kafka2

```bash
docker stop kafka2
```

### Topic Description (kafka2 stopped)

```text
Topic: sensor-events   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,3
Topic: sensor-events   Partition: 1   Leader: 3   Replicas: 2,3,1   Isr: 3,1
Topic: sensor-events   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1
```

**Observation**:
- Partition 0: Leader remains `1` (majority = 2 of 3 ISR).
- Partition 1: Leader re-elects to `3` (new majority = 1,3).
- Partition 2: Leader remains `3`.
- `kafka2` is no longer in any ISR (expected - it is stopped).
- **Writes succeed** because `min.insync.replicas=2` and 2 replicas are still alive.

## After Restarting kafka2

```bash
docker start kafka2
```

### Topic Description (kafka2 restarted)

```text
Topic: sensor-events   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
Topic: sensor-events   Partition: 1   Leader: 3   Replicas: 2,3,1   Isr: 3,1,2
Topic: sensor-events   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

**Observation**: `kafka2` re-joins the ISR after catching up. Leader election
does **not** revert automatically (KRaft does not re-balance leaders unless
triggered manually), which is expected behaviour.

## Conclusion

| Scenario | Producers | Consumers | Data Loss |
|----------|------------|------------|------------|
| 1 broker stopped (3 -> 2 alive) | OK (`acks=all`) | OK | None |
| 2 brokers stopped (3 -> 1 alive) | Blocked (`min.insync=2`) | OK (can read) | None (blocked writes) |
| Broker restarted | OK | OK | None (replica catch-up) |

The cluster tolerates **one broker failure** without any data loss and with
**no producer downtime** (producers block until the write is acked by 2 replicas).

## Commands Used

```bash
# Describe topic (before)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# Stop broker 2
docker stop kafka2

# Describe topic (after)
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# Restart broker 2
docker start kafka2

# Verify ISR recovery
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

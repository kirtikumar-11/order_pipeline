# 5 · Design scenarios, worked end to end

How to use: read the question, **cover the answer**, answer out loud with the 6 steps (Clarify, Quantify, Constrain, Design, Trade-offs, Operate), then compare. Each answer shows the questions to ask first, because in the interview that is half the score.

---

## S1. Real-time fraud detection on card payments
> "Block suspicious payments before they're approved."

**Ask first:** How fast must the decision be (approval path is usually < 100–300 ms)? What signals: rules or an ML model? Volume (say 5,000 payments/s peak)? Cost of a false positive (blocked good customer) vs false negative?

**Key insight:** a decision *in the approval path* is a **request/response** problem, not a pipeline problem. Streaming computes the *features*; the payment service makes the decision synchronously.

**Design:**
```text
Payments → Kafka (key = card_id) → Flink: features per card (txn count last 1/10/60 min,
   amount vs avg, new country) → online feature store (Redis/DynamoDB)
Payment API → reads features (ms) → model/rules → approve/decline → event to Kafka
Kafka → Spark → Delta Bronze/Silver → model training, investigations, dashboards
```
**Trade-offs:** Flink over Spark for millisecond, per-event updates with big keyed state and timers. Spark is fine for the offline/training side. Rules first (explainable), ML later.
**Operate:** feature freshness SLA, fallback if feature store is down (approve with a lower limit, or rules only), feedback loop with chargebacks (labels arrive weeks later), audit trail in Delta.

---

## S2. Executive sales dashboard refreshed "in real time"
**Ask first:** Is "real time" seconds or 15 minutes? (Usually 5–15 min is fine for executives.) What metrics? Who else uses the data?
**Design:** exactly your project. `availableNow` every 10–15 min, Gold tables, Databricks SQL dashboard.
**Trade-off line:** "Going from 15 minutes to 30 seconds roughly multiplies compute cost (always-on cluster) for a dashboard people look at a few times a day. I'd confirm the need before paying for it."
**Operate:** freshness shown on the dashboard ("data as of 10:45"), reconciliation with finance daily.

---

## S3. IoT: 1 million devices sending temperature every 10 s
**Quantify:** 1M / 10 s = 100,000 events/s. At 200 bytes = 20 MB/s ≈ 1.7 TB/day raw.
**Ask first:** What do we do with it: alerts when a device overheats (seconds), dashboards per factory (minutes), long-term analytics? Can devices send late (offline buffering)? Duplicates on reconnect?
**Design:**
```text
Devices → MQTT/IoT Hub → Kafka (key = device_id, ~100+ partitions)
  ├─ Flink or Spark (always on): threshold & anomaly alerts per device → alert service
  └─ Spark → Bronze (raw) → Silver (dedup on device_id + reading_ts, event-time)
            → Gold: 1-min / 1-hour aggregates per device & site (watermark e.g. 1 hour)
```
**Trade-offs:** downsample for long-term storage (keep raw 30 days, aggregates forever). Partition Delta by date, cluster by device_id. Watermark = trade-off between waiting for offline devices and state size; very late data goes to a correction batch job.
**Operate:** "device silent" alerts (absence of data is a signal), clock skew on devices (use server receive time as fallback), small-file compaction.

---

## S4. Replicate a Postgres orders database into the lakehouse (CDC)
**Ask first:** Freshness? Which tables? Deletes need to propagate? Schema changes frequency? Can we touch the source DB (load)?
**Design:**
```text
Postgres WAL → Debezium (Kafka Connect) → Kafka topic per table (key = primary key)
  → Spark: Bronze (raw change events: op c/u/d, before/after, LSN/ts)
  → Silver: MERGE into current-state table by primary key, ordered by LSN
       (WHEN MATCHED AND op='d' DELETE / WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT)
  → optional SCD2 history table
```
**Why CDC instead of `SELECT * WHERE updated_at > X`:** catches deletes, no load from polling, no missed updates when `updated_at` isn't maintained.
**Trade-offs:** managed CDC (Fivetran, DMS, Lakeflow Connect) = less ops, more cost. Initial snapshot + then stream changes.
**Gotchas:** within one micro-batch the same key may change several times → keep only the latest by LSN before MERGE, otherwise MERGE fails ("multiple source rows matched").

---

## S5. Late events: hourly revenue must be correct even if events arrive 2 days late
**Ask first:** How often is data late, and how late at most? Does the report need to be final at some point (finance close)?
**Options:**
1. Streaming aggregation with a 2-day watermark: correct, but 2 days of state and results finalised only after 2 days (append mode) or constantly updated (update mode).
2. **Silver by event time + Gold recomputed for affected hours** (preferred): each run, find hours touched by new events and recompute/MERGE only those hours.
3. Lambda-style: fast streaming numbers + nightly batch correction.
**Say:** "Late data is a business decision: either results change after publishing, or we wait. I'd show 'provisional' vs 'final' numbers."

---

## S6. Join orders with payments (two streams)
**Ask first:** How far apart can order and payment be? What if payment never comes?
**Design:** both streams with watermarks; join condition with time range:
```python
orders.withWatermark("order_ts", "2 hours").join(
  payments.withWatermark("pay_ts", "3 hours"),
  expr("o_id = p_order_id AND pay_ts BETWEEN order_ts AND order_ts + interval 1 hour"),
  "leftOuter")
```
Left outer gives "order without payment within 1 hour" rows (useful: abandoned checkouts) once the watermark passes.
**Trade-off:** bigger window = more state and later results. Alternative: land both in Silver and join in batch every few minutes (simpler, no streaming state).

---

## S7. A producer team added a field and changed `amount` from number to string
**Ask first:** Who owns the contract? Was this announced? Old and new producers running at the same time?
**Answer:**
- In my pipeline, the new field is ignored (explicit schema), and string amounts become `invalid_amount` in quarantine. The pipeline keeps running and the validation task alerts.
- Fix forward: agree a contract version. Parse both forms (`coalesce(cast(amount as decimal), ...)`), add the field with `mergeSchema`, then replay quarantined rows.
- Prevent: Avro/Protobuf + Schema Registry with BACKWARD compatibility rules blocks the breaking change at produce time. Data contracts + CI checks.

---

## S8. GDPR: "delete all data for customer 370"
**Ask first:** Which systems hold PII (Kafka, Bronze, Silver, Gold, backups)? Deadline (usually 30 days)?
**Answer:**
- Delta: `DELETE FROM silver WHERE customer_id = 370` (and Bronze, Gold), then `VACUUM` after the retention period so old files with the data are physically removed. Time travel to before the delete disappears after VACUUM: that's required here.
- Kafka: retention handles it (data expires); for compacted topics publish a tombstone (null value) for the key.
- Better design: keep PII in a separate table keyed by a pseudonymous ID; events carry only the ID. Delete one row = forget the person (crypto-shredding: encrypt PII per customer and delete the key).
- Use Deletion Vectors so deletes don't rewrite huge files.

---

## S9. Small files problem
> "Queries on the Silver table got slow over months."
**Cause:** frequent micro-batches write many small files; metadata and file-open overhead dominate.
**Fix:** `OPTIMIZE` (scheduled), optimized writes + auto-compaction, liquid clustering, fewer/larger batches (longer trigger), `VACUUM` old files. Check with `DESCRIBE DETAIL` (numFiles, sizeInBytes). Target ~100 MB–1 GB per file.

---

## S10. Data skew / hot partition
> "One Kafka partition has 10× the data; one Spark task takes forever."
**Ask first:** Is it one key (a mega-customer), or a bad partitioner?
**Fix:** key on a finer field; **salting** (key = customer_id + random 0..9, then aggregate twice); more partitions doesn't help one hot key. In Spark joins/aggregations: AQE skew join handling, salting, broadcast the small side.

---

## S11. Backfill 1 year of historical orders into the same tables
**Ask first:** Where is the history (files, DB dump)? Same schema? Can it run while streaming continues?
**Answer:** load history with a **batch** job into Bronze (same columns, `source = 'backfill'`), then run Silver MERGE in batch over it (idempotent, so overlaps with streamed data are harmless), then recompute Gold. Don't push a year through Kafka unless retention and consumers can handle it. Run backfill in date chunks, monitor, and keep the streaming job running (Delta handles concurrent appends; avoid concurrent MERGE on the same partitions or catch conflicts).

---

## S12. Real-time inventory: "never sell an item we don't have"
**Key insight:** this is **transactional consistency**, not analytics. The source of truth must be an OLTP database with row-level locking or conditional writes (Postgres, DynamoDB conditional update). Streaming + lakehouse is for analytics *about* inventory (forecasting, dashboards), not for reserving stock.
Say this: "I wouldn't use Kafka + Spark to decide stock reservations; eventual consistency would oversell."

---

## S13. Clickstream sessionisation (30 minutes of inactivity ends a session)
**Design:** Kafka (key = user_id) → Spark `session_window("event_ts", "30 minutes")` with watermark, or Flink session windows → Gold sessions table (duration, pages, conversion).
**Trade-off:** watermark vs very late mobile events (apps send batched events when back online) → nightly batch recompute for yesterday's sessions.

---

## S14. Exactly-once write to an external database (e.g. Postgres for an app)
**Answer:** Spark can't do a distributed transaction with Postgres. Make the write idempotent: `foreachBatch` → `INSERT ... ON CONFLICT (event_id) DO UPDATE/NOTHING` (upsert). Or write `(batch_id)` into a progress table in the same DB transaction and skip batches already applied. Use JDBC batch writes, limit connections (`numPartitions`).

---

## S15. Multi-region / disaster recovery
**Ask first:** RPO (how much data can we lose?) and RTO (how fast must we recover?)?
**Answer:** Kafka replication across regions (MirrorMaker 2, Confluent Cluster Linking, managed options); Delta tables in storage with cross-region replication or `DEEP CLONE` on a schedule; checkpoints are region-specific (offsets differ across mirrored clusters), so plan to restart consumers with offset translation or reprocess from a known point with idempotent sinks. Practise failover.

---

## S16. Reduce cost of the pipeline by 50%
**Look at:** always-on clusters that could be `availableNow` on a schedule; oversized clusters (check utilisation); serverless vs classic; photon on/off per workload; small files causing slow reads; full Gold recomputes → incremental; retention of raw data (cheaper storage tier); spot instances for batch; turn off unused dashboards' auto-refresh.
**Say:** "First measure: cost per job and per table. Then fix the biggest one."

---

## S17. Reprocess after a bug in Silver logic (wrong amount conversion for 3 days)
**Answer:** Fix code → rebuild Silver from Bronze (that's why Bronze keeps raw): either full rebuild (new table + new checkpoint, then swap) or targeted: delete affected date range from Silver, re-run batch Silver on Bronze for those dates (MERGE is idempotent), recompute Gold. Use time travel to compare before/after. Communicate the correction to dashboard users.

---

## S18. Choose between Spark and Flink for a new team
Ask: latency target, state size, team skills, existing platform, batch needs.
- Seconds-to-minutes + Databricks already + also batch ETL → Spark.
- Milliseconds, CEP, event-by-event timers, huge keyed state → Flink.
- "I'd avoid running both unless one can't meet a hard requirement; two engines = double the operations."

---

## Practice prompts (no answers: use the framework)
1. Ride-sharing: show drivers' live positions on a map and compute surge pricing per area.
2. Bank: daily regulatory report that must be reproducible exactly a year later.
3. Food delivery: ETA predictions using live restaurant prep times.
4. Ad platform: count unique users per campaign per hour (hint: approximate counting, HyperLogLog).
5. Stock market ticks: 1-minute candles and alerts on 5% moves.
6. Log analytics: 2 TB/day of app logs; engineers search errors within a minute.

For each, first write the 5 questions you'd ask.

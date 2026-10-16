# 3 · Core concepts in plain English

Each concept: what it is, the one thing interviewers want to hear, and a common trap.

---

## Kafka

**Topic, partition, offset.** A topic is a named log. It's split into partitions (separate append-only files on brokers). Each record in a partition gets an increasing number, the **offset**.
- *Say:* "Kafka guarantees order only within a partition."
- *Trap:* saying Kafka is globally ordered.

**Key → partition.** The producer hashes the key (murmur2) and takes it modulo the partition count. Same key → same partition → ordered. No key → spread across partitions.
- *Trap:* adding partitions later changes `hash % N`, so a key can move to a new partition and per-key ordering breaks at that moment.

**Replication.** Each partition has a leader and followers (replication factor, usually 3). **ISR** = in-sync replicas.
- `acks=0`: fire and forget. `acks=1`: leader wrote it. `acks=all`: all ISRs have it.
- With `acks=all`, set topic `min.insync.replicas=2` (RF 3) so a write fails rather than being stored on only one broker.

**Idempotent producer.** The broker tracks a producer ID + sequence number per partition and throws away retried duplicates. On by default in modern clients.
- *Say:* "It prevents duplicates from retries, not from my application re-sending after a crash. For that I need transactions or an event ID that I dedupe on downstream."

**Consumer groups.** Consumers with the same `group.id` share partitions; each partition goes to one consumer in the group. Max useful consumers = number of partitions. Different groups each get all the data.
- *Spark detail:* Spark's Kafka source doesn't use consumer-group commits for progress. It stores offsets in its **checkpoint**. So Kafka's "consumer lag" tools don't show Spark lag by default; monitor through Spark's query progress (`numInputRows`, `inputRowsPerSecond` vs `processedRowsPerSecond`, and the offsets-behind-latest metrics).

**Retention vs compaction.** Delete policy keeps data for time/size (e.g. 7 days). Compact policy keeps the latest value per key forever (good for "current state" topics, CDC).
- *Say:* "Kafka is the buffer, not the long-term store. Long-term history lives in Bronze."

**Delivery semantics (producer side):** `acks=all` + idempotence + retries ≈ no loss, no retry dups. **Transactions** (`transactional.id`) give atomic writes across partitions and exactly-once read-process-write *inside Kafka*.

---

## Spark Structured Streaming

**Model.** A stream is an unbounded table. Every micro-batch, Spark reads new rows, runs your query incrementally, and writes results.

**Checkpoint.** A folder storing: the offsets planned for each batch (**offsets log**), which batches finished (**commits log**), and **state** (for aggregations/dedup/joins).
- On restart Spark reads the checkpoint, re-runs any batch that started but didn't commit, then continues.
- One checkpoint per query. Never share. Changing the query a lot (new aggregation, different source) needs a new checkpoint.
- *Trap:* "the checkpoint stores the data". It stores progress and state, not your output.

**`startingOffsets`** is only used on the **first** run (no checkpoint). After that the checkpoint wins.

**Exactly-once in Spark.** Source must be replayable (Kafka, Delta), progress tracked in checkpoint, sink idempotent. The Delta sink is exactly-once (it records the batch ID in the transaction log). `foreachBatch` is **at-least-once** unless you make the write idempotent (MERGE on key, or Delta's `txnAppId` + `txnVersion` options).

**Event time vs processing time.** Event time = when it happened (in the record). Processing time = when Spark saw it. Business metrics use event time.

**Watermark.** `withWatermark("event_ts", "10 minutes")` = "I'll wait for data up to 10 minutes late". Watermark = max event time seen − 10 min. State older than the watermark is dropped; for aggregations, rows later than that are ignored.
- *Say:* "The watermark is a trade-off between completeness and state size/latency."

**Output modes.** `append` (only final rows; with aggregations needs a watermark), `update` (changed rows), `complete` (whole result each time; small aggregations only).

**Stateful operations:** aggregations, `dropDuplicates`, stream-stream joins, `flatMapGroupsWithState`/`transformWithState`. State lives in the state store (RocksDB recommended for big state).

**`foreachBatch`.** Gives you each micro-batch as a normal DataFrame, so you can MERGE, write to several tables, or call batch-only APIs.

**`maxOffsetsPerTrigger`.** Caps records per batch. Protects against huge first batches and lets a backlog drain in steady steps.

**Backpressure / lag.** If `processedRowsPerSecond` < `inputRowsPerSecond` for long, you're falling behind: scale out, increase partitions, simplify the query, or fix skew.

---

## Delta Lake

**What it is.** Parquet files + a **transaction log** (`_delta_log/`) of JSON commits (plus periodic Parquet checkpoints). The log says which files make up each table version.

**ACID.** Writers use optimistic concurrency: write files, then try to commit the next log version; conflicts are detected and retried or failed. Readers always see a complete version.

**MERGE.** Upsert/delete by key. The workhorse for dedup, CDC and SCD.

**Time travel.** `SELECT * FROM t VERSION AS OF 12` or `TIMESTAMP AS OF ...`. Great for debugging and audits; limited by `VACUUM` retention (default 7 days).

**OPTIMIZE / Z-ORDER / liquid clustering.** Streaming creates many small files. `OPTIMIZE` compacts them. Z-ORDER (older) or **liquid clustering** (`CLUSTER BY`, newer and preferred) co-locates data for faster filters.

**VACUUM.** Deletes files no longer referenced, older than retention. Too short a retention breaks time travel and long-running readers.

**Schema enforcement and evolution.** Writes with wrong columns fail by default (enforcement). `mergeSchema` adds new columns (evolution).

**Change Data Feed (CDF).** Records row-level inserts/updates/deletes per version so downstream jobs can read only changes. Useful for incremental Gold.

**Delta as a streaming source.** `spark.readStream.table("bronze")` reads only new commits. Updates/deletes in the source break append-only streaming unless you use `skipChangeCommits` or CDF.

---

## Medallion architecture

| Layer | Contains | Rules |
|---|---|---|
| **Bronze** | Raw data as received + metadata (source, offset, ingest time) | Append only, no business logic, keep long. The replay point. |
| **Silver** | Cleaned, typed, validated, deduplicated, conformed | One row per real event/entity; bad rows quarantined |
| **Gold** | Business aggregates and models for consumers | Shaped for a specific use (dashboard, ML, API) |

*Say:* "If Silver logic has a bug, I fix it and rebuild Silver and Gold from Bronze. Without Bronze, I'd need Kafka to still have the data."

---

## Databricks specifics

- **Unity Catalog:** `catalog.schema.table` naming, permissions, lineage; **Volumes** for files (checkpoints, certificates).
- **Secrets:** `dbutils.secrets.get(scope, key)`; values are redacted in notebook output.
- **Serverless vs classic compute:** serverless starts fast and needs no tuning, but (on many workspaces) only allows `availableNow` for streaming. Classic job clusters support always-on streams.
- **Jobs / Workflows:** tasks with dependencies, retries, schedules, `max_concurrent_runs`.
- **Auto Loader (`cloudFiles`):** incremental file ingestion from cloud storage with schema inference/evolution and a rescued-data column.
- **Lakeflow Declarative Pipelines (formerly Delta Live Tables):** declare tables, add `expectations` (data-quality rules), the platform runs and retries.
- **Shaded Kafka classes:** on Databricks the JAAS config uses `kafkashaded.org.apache.kafka...`.

---

## Data quality vocabulary
- **Completeness** (no missing rows/fields), **validity** (values allowed), **uniqueness** (no dups), **timeliness/freshness**, **consistency** (matches the source / other systems), **accuracy**.
- **Quarantine / dead-letter queue (DLQ):** where bad records go with the reason.
- **Reconciliation:** compare counts or sums with the source of truth (e.g. Gold revenue vs payments DB daily).

## SCD (slowly changing dimensions)
- **Type 1:** overwrite (no history).
- **Type 2:** new row per change with `valid_from`, `valid_to`, `is_current` (full history). Built with MERGE: close the old row, insert the new one.

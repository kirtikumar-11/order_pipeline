# 2 · Choosing technology: decision tables

Rule: **requirements first, tool second.** Each table lists the deciding questions and when each option wins. "Wins when" is what you say after "because".

---

## 2.1 Batch vs streaming: the first decision

| How fresh must the result be? | Pattern | Typical tools | Cost |
|---|---|---|---|
| Milliseconds to < 1 s (fraud block, ad bidding) | Event-at-a-time streaming | Flink, Kafka Streams | High, complex |
| 1 s – few minutes (live dashboards, alerts) | Micro-batch streaming, always-on | Spark Structured Streaming (`processingTime`) | Medium–high (24/7 cluster) |
| 5 min – 1 hour (near-real-time reports) | Scheduled incremental ("streaming as batch") | Spark `availableNow`, Auto Loader, Lakeflow Declarative Pipelines (DLT) triggered | Low |
| Hours – daily | Batch | Spark batch, dbt, SQL warehouse | Lowest |

**Interview tip:** many "real-time" asks are actually "every 15 minutes is fine". Ask. The cheapest design that meets the requirement is the right one.

**Spark triggers explained:**
- `processingTime="30 seconds"`: runs forever, one micro-batch every 30 s.
- `availableNow=True`: processes everything available now (in several batches if big), then stops. Schedule it. Same checkpoints and guarantees as an always-on stream.
- `once=True`: old version of availableNow (one big batch); deprecated, prefer availableNow.
- `continuous`: experimental ~1 ms mode with limits (at-least-once, few operations). Rarely used. Databricks also offers newer low-latency "real-time mode" for Structured Streaming; check current docs if asked.

---

## 2.2 Message transport: Kafka vs others

| Need | Pick | Why |
|---|---|---|
| Many independent consumers, replay last N days, high throughput, ordering per key | **Kafka** (Aiven, Confluent, MSK, self-hosted) | Durable log; consumers keep their own position; partitions scale out |
| All-in on AWS, want zero ops, moderate scale | **Kinesis Data Streams** | Managed, IAM auth; shards similar to partitions (≈1 MB/s or 1,000 records/s write per shard) |
| All-in on GCP | **Pub/Sub** | Serverless, auto-scaling; ordering via ordering keys |
| All-in on Azure | **Event Hubs** | Has a Kafka-compatible endpoint, so Kafka clients work |
| Task queue: each message processed once by one worker, no replay | **RabbitMQ, SQS** | Per-message ack, routing; messages are deleted after processing |
| Files land every hour, no streaming source | **No broker**: cloud storage + Auto Loader | Don't add Kafka just to look modern |
| Database changes (CDC) | **Debezium → Kafka**, or managed CDC (DMS, Fivetran, Lakeflow Connect) | Reads the DB's transaction log, not polling |

**Managed vs self-hosted Kafka:** managed (Aiven/Confluent/MSK) when the team is small and you want upgrades, security patches, and monitoring handled; self-hosted only with a platform team and a cost or control reason.

---

## 2.3 Stream processing engine

| Need | Pick | Why |
|---|---|---|
| Seconds–minutes latency, also batch jobs, team knows Spark/SQL, lakehouse sink | **Spark Structured Streaming** | Same API for batch and stream; native Delta; huge ecosystem |
| Sub-second latency, complex event-time logic, large state, CEP (patterns like "3 failed logins in 1 min") | **Apache Flink** | True event-at-a-time; strong state and timers |
| Logic lives inside a Java/Kotlin microservice, input and output are Kafka | **Kafka Streams** | A library, no cluster; scales with app instances |
| Simple filters/aggregations in SQL on Kafka | **ksqlDB / Flink SQL** | No code |
| Declarative pipelines with built-in data-quality rules on Databricks | **Lakeflow Declarative Pipelines (DLT)** | Expectations, lineage, auto-retries; less control |
| Very small volume (a few events/s) | **A plain consumer app** (Python `confluent-kafka`) or serverless functions (Lambda) | A Spark cluster is overkill |

---

## 2.4 Table format / storage

| Need | Pick | Why |
|---|---|---|
| Databricks-centric lakehouse, MERGE, streaming in & out | **Delta Lake** | ACID, MERGE, time travel, Change Data Feed, deep Spark integration |
| Many engines (Snowflake, Trino, Flink, Spark) on the same tables, vendor-neutral | **Apache Iceberg** | Open spec, broad engine support, hidden partitioning |
| Heavy upserts/CDC with record-level index, incremental pulls | **Apache Hudi** | Built for upserts (copy-on-write / merge-on-read) |
| Write-once analytical files, no updates | Plain **Parquet** | Simplest; but no ACID, no MERGE, no time travel |
| SQL analysts, mostly BI, structured data | **Cloud warehouse** (Snowflake, BigQuery, Databricks SQL) | Fast SQL, managed, concurrency |

**Delta UniForm** can expose Delta tables as Iceberg too. Good answer if asked "what if another team uses Snowflake?"

---

## 2.5 Serving layer (where Gold is read from)

| Who reads, how | Pick | Why |
|---|---|---|
| BI dashboards, ad-hoc SQL, seconds per query OK | **Delta Gold + Databricks SQL / warehouse** | Cheap, scalable, already there |
| User-facing analytics, sub-second, high concurrency (e.g. "seller dashboard" for 100k sellers) | **ClickHouse, Apache Pinot, Druid** | Real-time OLAP, very fast aggregations |
| App needs one record by key in ms ("current status of order 123") | **Cassandra, DynamoDB, Redis, Postgres** | Key-value / point lookups |
| Search ("orders containing 'keyboard'") | **Elasticsearch / OpenSearch** | Full-text search |
| ML features online | **Feature store** (online store backed by key-value DB) | Low-latency feature reads |

---

## 2.6 Delivery guarantees: what to build

| Requirement | Design |
|---|---|
| Losing data is OK, duplicates are not (rare: metrics sampling) | At-most-once: commit before processing |
| No loss; duplicates OK or handled later | At-least-once + **idempotent consumer** (dedup by ID, MERGE/upsert) ← most common real answer |
| No loss, no duplicates, end to end | Replayable source (Kafka) + checkpointed engine (Spark/Flink) + **idempotent or transactional sink** (Delta, MERGE on key, Kafka transactions) |

Say this: *"Exactly-once is not a property of one tool; it's the combination of a replayable source, tracked progress, and an idempotent sink. And since producers can also re-send, I always add an event ID and dedupe."*

---

## 2.7 Deduplication options

| Option | Scope | State | Use when |
|---|---|---|---|
| Idempotent producer (`enable.idempotence`) | Retries inside one producer session | None for you | Always on; not enough alone |
| `dropDuplicates(["id"])` in a stream without watermark | Forever | Grows forever ⚠ | Almost never |
| `withWatermark(...).dropDuplicatesWithinWatermark(["id"])` (Spark 3.5+) | Within the watermark window | Bounded | High volume, duplicates arrive close together |
| `MERGE ... WHEN NOT MATCHED INSERT` in `foreachBatch` | All history in the table | None (reads target) | Correctness matters most; replays possible (this project) |
| Dedup in Gold/SQL with `ROW_NUMBER()` | Query time | None | Batch / small data |

---

## 2.8 Joins in streaming

| Join | How | Watch out |
|---|---|---|
| Stream + small static table (e.g. product catalog) | `stream.join(static_df, "product_id")` | Static side is re-read each batch for Delta; fine for small tables |
| Stream + stream (orders + payments) | Watermarks on both + time-range condition (`payment_ts BETWEEN order_ts AND order_ts + interval 1 hour`) | Without both, state grows forever |
| Stream + slowly changing dimension | Join with the Delta table version, or SCD2 table with `valid_from/valid_to` | Point-in-time correctness |
| Enrichment via lookup service | `foreachBatch` + batched lookups / cache | Don't call an API per row |

---

## 2.9 Message format

| Need | Pick |
|---|---|
| Small project, few producers, human-readable | **JSON** + explicit schema in consumer (this project) |
| Many teams, schema changes, compact, enforced contracts | **Avro or Protobuf + Schema Registry** with compatibility rules (BACKWARD is the usual default) |
| Columnar storage at rest | **Parquet** (inside Delta/Iceberg) |

---

## 2.10 Kafka security options

| Option | Use when |
|---|---|
| **SASL_SSL + SCRAM-SHA-256/512** (this project) | Username/password auth over TLS; simple, widely supported |
| **mTLS (SSL client certificates)** | Strong machine identity, no passwords; certificate rotation needed |
| **SASL/OAUTHBEARER, AWS IAM (MSK)** | Central identity provider, short-lived tokens |
| **PLAINTEXT** | Never in production |

Always: TLS for encryption in transit, verify the broker certificate (CA), ACLs so each app can only read/write its topics, secrets in a vault (Databricks Secrets, AWS Secrets Manager, Azure Key Vault).

---

## 2.11 Orchestration and compute

| Need | Pick |
|---|---|
| Databricks-only pipeline | **Databricks Jobs / Workflows** (task dependencies, retries, schedules) |
| Many systems (APIs, DBs, Databricks, dbt) | **Airflow / Dagster / Prefect** |
| Serverless compute (no cluster management, fast start) | Short `availableNow` jobs, SQL; **not** always-on streams (on many workspaces) |
| Classic job clusters | Always-on streaming, custom libraries, specific Spark configs |
| Interactive (all-purpose) clusters | Development only; too expensive for scheduled jobs |

---

## 2.12 Partition key for Kafka (and the lakehouse)

**Kafka key** decides partition and ordering:
- Need order per entity → key = that entity (`customer_id`, `order_id`, `device_id`).
- Watch for **hot keys** (one huge customer) → key by a finer ID, or add a salt and re-aggregate.
- **Number of partitions** ≥ the max parallel consumers you'll want. More partitions = more parallelism but more overhead. Increasing later breaks the key→partition mapping for new data, so plan ahead.

**Delta table layout:**
- Small tables (< 1 TB): don't partition; use **liquid clustering** (`CLUSTER BY`) or nothing.
- Large tables: partition only by low-cardinality columns that filters always use (usually date). Never by high-cardinality columns (`customer_id`) → millions of tiny files.
- Run `OPTIMIZE` (compaction) on streaming tables; enable auto-compaction / optimized writes.

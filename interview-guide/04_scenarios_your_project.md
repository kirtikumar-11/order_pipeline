# 4 · Questions about your project (with model answers)

Interviewers always dig into your own project. They test whether you **built** it or **copied** it. Answer with what you did, what you saw, and what you'd change. Run `../docs/failure_tests.md` first so these answers are first-hand.

---

### Q1. "Walk me through your project." (2-minute version)
**Model answer:**
> "It's a real-time order pipeline. A Python producer simulates e-commerce orders going through CREATED, PAID and SHIPPED and publishes JSON events to a 3-partition Kafka topic on Aiven, over TLS with SCRAM authentication. The key is customer ID so a customer's events stay in order.
>
> On Databricks, a Structured Streaming job reads the topic into a Bronze Delta table that keeps the raw JSON plus partition and offset. A second stream reads new Bronze rows, parses them with an explicit schema, validates them, sends bad rows to a quarantine table with the reason, and MERGEs good rows into Silver on the event ID, so duplicates and replays are ignored. Gold rebuilds business tables: current status per order, revenue per product, and hourly sales.
>
> It runs as a Databricks job every 15 minutes with the `availableNow` trigger, because serverless doesn't allow always-on streams and 15-minute freshness was enough. A validation task fails the job if there are duplicates or too many bad rows. I tested restart, duplicates, malformed data, a lost checkpoint and wrong certificates: for example, after deleting the Bronze checkpoint Kafka was replayed, Bronze doubled, and Silver stayed exactly the same."

Why it works: architecture → a reason for each choice → failure testing with a concrete number.

---

### Q2. "Why Kafka? Why not write directly from the producer to Delta?"
**Clarify-style answer:** "It depends on how many consumers and how much decoupling we need."
- Kafka **decouples** producers from consumers: if Databricks is down, orders still land in Kafka and are processed later.
- **Replay**: I can reprocess the last N days (retention).
- **Multiple consumers**: fraud, analytics and email services can each read the same events independently.
- **Ordering per key** and horizontal scale with partitions.
- **When I wouldn't:** one consumer, files arriving hourly, low volume → write files to cloud storage and use Auto Loader. Kafka adds cost and operations.

---

### Q3. "Why is the key `customer_id`? What happens if you change the number of partitions?"
- Same key → same partition → events of one customer (so of each of their orders) are processed in order.
- Trade-off: a huge customer can create a hot partition. If only per-order order matters, `order_id` spreads load better.
- Adding partitions changes `hash(key) % N`, so new events for a customer may go to a different partition than old ones. During the switch, ordering across old and new partition isn't guaranteed. Plan partitions up front, or switch at a quiet time.
- My Silver/Gold logic doesn't depend on arrival order anyway: current status is ranked by lifecycle (SHIPPED > PAID > CREATED), not by arrival.

---

### Q4. "How do you guarantee no data loss and no duplicates?"
Walk the pipeline left to right:
1. **Producer → Kafka:** `acks=all` + idempotence, so no loss and no duplicates from retries. But an app re-send after a crash is a real duplicate, so every event has an `event_id`.
2. **Kafka → Bronze:** Spark tracks offsets in the checkpoint; Delta sink is exactly-once per batch. A lost checkpoint replays Kafka → Bronze may contain duplicates. That's acceptable in Bronze.
3. **Bronze → Silver:** `foreachBatch` + `MERGE ... WHEN NOT MATCHED INSERT` on `event_key`. Retries, replays and producer duplicates all collapse to one row. Quarantine MERGEs on topic/partition/offset.
4. **Silver → Gold:** recomputed from Silver, so it's always consistent.
5. **Check:** validation task asserts zero duplicate keys in Silver.
> "So end to end it's effectively exactly-once, built from at-least-once delivery plus idempotent writes."

---

### Q5. "Your compute rejected the infinite trigger. What did you do and what's the trade-off?"
- Error `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`: serverless only supports `availableNow`.
- I used `availableNow` on a 15-minute schedule. Each run reads everything new since the checkpoint and stops. Same checkpointing and exactly-once guarantees.
- Trade-off: latency = schedule + run time (~15–20 min) vs seconds; cost is much lower (no 24/7 cluster).
- If the business needs seconds: classic job compute with `processingTime="30 seconds"`, Bronze and Silver as always-on jobs, Gold on its own schedule.

---

### Q6. "What happens if the job doesn't run for 3 days?"
- If Kafka retention > 3 days: the next run starts from the checkpoint offsets and catches up. `maxOffsetsPerTrigger` splits the backlog into steady batches so the first run doesn't explode.
- If retention < 3 days: the oldest data is gone. With `failOnDataLoss=true` the job fails loudly. I chose that on purpose: silent gaps in revenue are worse.
- Prevention: alert on job failures and freshness; set retention comfortably longer than the worst outage (e.g. 7 days); Bronze keeps the long-term history.

---

### Q7. "How do you handle malformed messages?"
- Parse with an explicit schema (`from_json`). Validate: missing order ID, invalid amount, unknown status, bad JSON.
- Bad rows go to `silver_quarantine` with an array of reasons plus the raw value and offset. The stream never fails because of one bad message, and nothing is silently dropped.
- Validation fails the job if bad rows exceed 10%, because that signals a broken producer, not random noise.
- Fix path: fix the producer, then re-send or repair the quarantined rows and MERGE them into Silver.

---

### Q8. "Why MERGE for dedup and not `dropDuplicates`?"
- `dropDuplicates` on a stream keeps state; without a watermark the state grows forever, with a watermark it only catches duplicates inside the window. A lost checkpoint loses the state.
- MERGE checks against the whole Silver table, so it also catches duplicates from a replay days later.
- Cost: MERGE reads the target. At large scale I'd cluster Silver by `event_key`/date and limit the match condition to recent dates, or switch to `dropDuplicatesWithinWatermark` if duplicates always arrive close together.

---

### Q9. "Why is Gold a full overwrite? Won't that be slow?"
- Today Silver is small; recompute takes seconds and is always correct even with late or replayed data.
- When it grows: incremental Gold. Read only changes (Silver's Change Data Feed or the batch itself), find the affected `order_id`s / hours, and MERGE only those rows. Or streaming aggregations with a watermark if late data has a known bound.

---

### Q10. "How do you secure it?"
- Transport: TLS (`SASL_SSL`), broker certificate verified with Aiven's CA. Never disable verification.
- Auth: SCRAM-SHA-256 username/password.
- Secrets: `.env` locally (git-ignored), Databricks Secrets in the cloud; values never in notebooks or logs.
- Next steps for production: per-application Kafka users with ACLs (producer can only write `orders`, Databricks can only read), Unity Catalog grants per layer (analysts read Gold only), PII masking for customer fields, rotate passwords.

---

### Q11. "How would you monitor this in production?"
| What | How | Alert when |
|---|---|---|
| Job health | Databricks job runs, retries | Any failed run |
| Freshness | `now − max(ingested_at)` in Bronze; `now − max(event_ts)` in Silver | > 30 min |
| Lag | Spark progress: input vs processed rows/sec; offsets behind latest | Growing for 3 runs |
| Data quality | Quarantine rate per run; validation task | > 1–10% |
| Correctness | Daily reconciliation: Gold revenue vs source system | Mismatch > 0.1% |
| Cost | DBUs per run | Sudden jump |

---

### Q12. "How would you scale it 100×?" (≈ 50,000 events/s)
- **Kafka:** more partitions (e.g. 48–96) so consumers can parallelise; check broker capacity; maybe Avro to shrink messages.
- **Spark:** always-on classic cluster with autoscaling; executors ≈ partitions; RocksDB state store if stateful.
- **Delta:** liquid clustering on Silver; optimized writes + auto-compaction; scheduled `OPTIMIZE`.
- **Silver MERGE:** restrict to recent dates, or switch to watermark dedup.
- **Gold:** incremental instead of full recompute.
- Measure first: find the bottleneck from the Spark UI and query progress before changing things.

---

### Q13. "What would you do differently next time?"
Good honest answers:
- "Use Avro + Schema Registry instead of JSON, so schema changes are checked at the producer."
- "Key by `order_id` if only per-order ordering matters, to avoid hot partitions."
- "Make Gold incremental from the start using Change Data Feed."
- "Add a small reconciliation job against a 'source of truth' table."

---

### Q14. "What was the hardest problem?"
Pick something real. Examples from this project:
- Getting TLS trust working in Databricks (PEM truststore vs JKS, keeping line breaks in the secret).
- The `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` error and redesigning around `availableNow`.
- Understanding that the Spark Kafka source doesn't commit offsets to Kafka, so "consumer lag" isn't where you'd expect.

Structure: **situation → what I tried → what I learned → what I'd do now** (STAR).

---

### Q15. "Why `decimal` for amount? Why is timestamp a double in the source?"
- Floating point can't represent many decimals exactly (0.1 + 0.2 ≠ 0.3), so money sums drift. `decimal(12,2)` is exact.
- Python `time.time()` gives epoch seconds as a float; I convert with `timestamp_seconds()` in Silver. Better contract: send ISO-8601 with timezone or epoch milliseconds as an integer.

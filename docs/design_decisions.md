# Design decisions (and how to defend them in an interview)

Each decision: what I chose, why, what I gave up, and when I'd choose differently.

### 1. Kafka key = `customer_id`
- **Why:** Kafka keeps order only inside a partition. Same key → same partition, so all events of one customer (and so of each of their orders) stay in order.
- **Gave up:** a very busy customer could make one partition "hot".
- **Different when:** only per-order ordering matters → key by `order_id` (spreads load better). No ordering need → no key (round robin, best spread).

### 2. `acks=all` + idempotent producer
- **Why:** the broker confirms only after all in-sync replicas have the record, and producer retries can't create duplicates in Kafka.
- **Gave up:** a few ms of latency per batch.
- **Note:** idempotence only covers retries inside one producer session. If the app crashes and re-sends, Kafka gets a real duplicate. That's why every event also has an `event_id` and Silver deduplicates on it.

### 3. Bronze stores the raw JSON string, not parsed columns
- **Why:** if the parser or schema is wrong, the original bytes are still there to replay. Bronze is the "receipt".
- **Gave up:** Bronze is not nice to query. That's Silver's job.

### 4. Explicit schema with `from_json` (no inference)
- **Why:** inference on a stream is impossible to do safely; a wrong type would silently change the table. With a fixed schema, a bad message becomes a quarantined row with a reason.
- **Different when:** many producers and frequent changes → Avro/Protobuf + Schema Registry, so the contract is checked when producing.

### 5. Quarantine table instead of dropping bad rows
- **Why:** "never silently lose data". You can count, alert on, fix and replay bad rows.

### 6. Silver dedup with `MERGE ... WHEN NOT MATCHED INSERT` inside `foreachBatch`
- **Why:** it removes duplicates across *all* history, survives replays and lost checkpoints, and needs no streaming state in memory. Works with `availableNow`.
- **Gave up:** a MERGE reads the target table, so it gets slower as Silver grows. Mitigate by clustering Silver on `event_key`/`event_date` and limiting the match to recent dates (`t.event_date >= current_date() - 7`).
- **Alternative:** `dropDuplicatesWithinWatermark("event_id")` with a watermark: faster, but only dedups inside the watermark window and needs state; a lost checkpoint loses the state.

### 7. `availableNow` trigger on a schedule
- **Why:** serverless compute only allows it; it's cheap (compute only while running) and still "streaming" (checkpoints, exactly-once, incremental).
- **Gave up:** latency is the schedule interval (minutes), not seconds.
- **Different when:** the business needs seconds → always-on `processingTime` trigger on classic compute, or Flink for sub-second.

### 8. Gold = full recompute + overwrite
- **Why:** tiny tables, simple code, always correct even with late or replayed data.
- **Different when:** Silver is billions of rows → incremental Gold: find changed `order_id`s / hours from the batch and MERGE only those, or use a streaming aggregation with a watermark.

### 9. Order status ranked by lifecycle, not by latest timestamp
- **Why:** clocks and arrival order lie. A SHIPPED order can't become PAID again.

### 10. Money as `decimal(12,2)`
- **Why:** floating point can't represent 0.1 exactly; sums drift. Decimal is exact.

### 11. Secrets in a Databricks secret scope; CA passed as PEM text
- **Why:** no secrets in code or notebook output (Databricks redacts secret values). PEM trust store avoids converting to a Java keystore and works on serverless.

### 12. Checkpoints in a Unity Catalog Volume, one per query
- **Why:** durable, governed storage. A checkpoint belongs to exactly one query; sharing one breaks both.

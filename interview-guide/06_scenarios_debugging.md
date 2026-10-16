# 6 · Production debugging scenarios

Method for any "it's broken" question:
```text
1. Impact      Who is affected? Since when? Data wrong, late, or missing?
2. Observe     Look before touching: job runs, Spark UI, query progress, table history
3. Hypothesis  List 2–3 likely causes, check the cheapest first
4. Mitigate    Stop the bleeding (pause, scale up, roll back) without losing data
5. Fix         Root cause
6. Prevent     Test, alert, or guardrail so it can't silently happen again
```
Interviewers love it when you say "first I'd check X **before** changing anything".

---

### D1. Consumer lag keeps growing
**Check:** query progress, `inputRowsPerSecond` vs `processedRowsPerSecond`; batch duration vs trigger interval; Spark UI for one slow task (skew) vs all slow (under-sized).
**Causes & fixes:**
- Input grew → scale executors; Kafka partitions ≥ cores (`minPartitions` option can split Kafka partitions across more tasks).
- Skew (one hot partition/key) → re-key or salt.
- Slow sink: MERGE on a big table → cluster target on the merge key, restrict match to recent dates.
- State growing (no watermark) → add watermark; RocksDB state store.
- Small files making reads slow → OPTIMIZE.
**Mitigate:** raise `maxOffsetsPerTrigger` carefully or add compute temporarily. Check Kafka retention so data isn't deleted before you catch up.

### D2. Duplicate rows appeared in Gold
**Check:** are duplicates already in Silver? (`GROUP BY event_key HAVING count(*) > 1`) Which versions introduced them (`DESCRIBE HISTORY`)?
**Common causes:** `foreachBatch` with plain `append` (not idempotent) and a batch retried; checkpoint deleted/changed and Kafka replayed; producer re-sent after a crash with no event ID; two jobs writing the same table.
**Fix:** dedupe with MERGE/ROW_NUMBER, restore with `RESTORE TABLE ... TO VERSION AS OF n` if needed, make writes idempotent, set `max_concurrent_runs = 1`.

### D3. Rows are missing (Gold total lower than the source)
**Walk the layers:** source count → Kafka offsets → Bronze → Silver + quarantine → Gold. Where does the count drop?
- Missing in Bronze: `failOnDataLoss=false` hid a retention gap; wrong `startingOffsets` (`latest` on first run skips history); wrong topic.
- Bronze OK, Silver short: rows in quarantine (validation too strict?), or dropped by a watermark (late data in a streaming aggregation).
- Silver OK, Gold short: filter logic (status not included), timezone bucketing, inner join dropping unmatched rows.

### D4. Streaming job fails after a code change with a checkpoint error
**Cause:** the change isn't compatible with existing state/checkpoint (changed aggregation keys, added a stateful operator, changed source).
**Fix:** for non-stateful changes (new column, filter tweak) the checkpoint usually works. For incompatible changes: new checkpoint path + choose where to start (`startingOffsets` or a timestamp) + idempotent sink so overlap is harmless. Never edit checkpoint files by hand.

### D5. Driver/executor out of memory in a streaming aggregation
**Cause:** unbounded state (no watermark, or a huge one), high-cardinality keys, `collect()` on the driver, `complete` output mode on a big result.
**Fix:** watermark, RocksDB state store, reduce key cardinality, `update`/`append` mode, bigger executors as a stopgap.

### D6. `SSLHandshakeException: PKIX path building failed`
**Cause:** client doesn't trust the broker certificate: wrong/missing CA, CA file lost line breaks, connecting to a different Kafka service.
**Fix:** re-upload the correct CA; check the truststore type (PEM vs JKS) matches the file. **Never** set `ssl.endpoint.identification.algorithm=` empty or disable verification as a "fix" in production.

### D7. `SaslAuthenticationException`
Wrong user/password, wrong mechanism (SCRAM-SHA-256 vs 512), SASL not enabled on the service, or using the client-cert (SSL) port instead of the SASL port. On Databricks: `ClassNotFoundException` for the login module means the `kafkashaded.` prefix is missing.

### D8. MERGE fails: "multiple source rows matched the same target row"
**Cause:** duplicates of the same key *inside one batch*.
**Fix:** dedupe the batch first (`row_number()` over key ordered by time/offset, keep 1), as `split_good_bad` does in this project.

### D9. Concurrent write conflict (`ConcurrentAppendException`, `ConcurrentDeleteReadException`)
**Cause:** two jobs MERGE/DELETE overlapping files of the same Delta table (e.g. backfill + streaming).
**Fix:** run them sequentially, make them touch disjoint partitions and say so in the MERGE condition, enable row-level concurrency where available, retry with backoff.

### D10. Dashboard numbers changed for last week
**Possible reasons:** late data recomputed Gold (expected; communicate it), a bug fix reprocessed Silver, a GDPR delete, someone ran a manual update.
**Investigate:** `DESCRIBE HISTORY gold_table` shows who/what changed it and when; time travel to compare versions.

### D11. Job is green but data is stale
**Cause:** `availableNow` run finds nothing new because the producer stopped, topic changed, or credentials for the producer expired.
**Prevent:** freshness alerts (`now − max(event_ts)`), not just job-status alerts. "Green job, no data" is a classic silent failure.

### D12. Kafka broker disk is full
**Cause:** retention too long for volume, a consumer team asked for long retention, compaction stuck.
**Fix:** increase disk / tiered storage, reduce retention for that topic (only after checking all consumers are caught up), compress (lz4/zstd) on the producer.

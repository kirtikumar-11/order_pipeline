# Failure tests

Each test says what it proves, how to run it, and what you should see. Run `04_validate` (or `python local/run_local_pipeline.py --stage validate`) after each one. Results marked **sandbox** were run against a local SASL_SSL Kafka; repeat them on Databricks to see them yourself.

## 1. Restart picks up where it stopped (checkpointing)
**Proves:** each run reads only new Kafka records, nothing twice, nothing skipped.
1. Run the job once. Note the Bronze row count.
2. `python producer/producer.py --count 100 --rate 0`
3. Run the job again.

**Expect:** Bronze grows by exactly 100. `bronze_duplicate_offsets = 0`. **Sandbox:** 300 → 400.

## 2. Producer duplicates (same event sent twice)
**Proves:** Silver deduplicates on `event_id`, across batches too.
```bash
python producer/producer.py --count 300 --rate 0 --dup-ratio 0.1
```
**Expect:** Bronze has every copy (it is raw). Silver has one row per `event_key`; `duplicates_removed` equals the producer's `dups` count. **Sandbox:** 24 dups sent, 24 removed.

## 3. Malformed messages
**Proves:** bad data never breaks the stream and is never silently dropped.
```bash
python producer/producer.py --count 300 --rate 0 --bad-ratio 0.05
```
**Expect:** quarantine rows = producer's `bad` count, each with reasons such as `unparseable_json`, `missing_order_id`, `invalid_amount`, `invalid_status`. The 02_silver notebook shows a count per reason. **Sandbox:** 15 sent, 15 quarantined.

To fix and replay a quarantined row later: correct the source, re-send it (it gets a new offset), or write a small notebook that reads `raw_value` from quarantine, fixes it, and MERGEs it into Silver.

## 4. Lost checkpoint (worst case: Kafka is replayed)
**Proves:** the pipeline is idempotent. Replaying old data does not create duplicates downstream.
- Databricks: delete `/Volumes/<catalog>/orders_pipeline/checkpoints/bronze_orders` (only in your test schema!), set `starting_offsets = earliest`, run the job.
- Local: `python local/run_local_pipeline.py --drop-checkpoint bronze`

**Expect:** Bronze doubles (`bronze_duplicate_offsets` > 0, that is allowed in raw). Silver, quarantine and Gold do not change. **Sandbox:** Bronze 400 → 800, Silver stayed 340, quarantine stayed 20.

## 5. Crash in the middle of a batch
**Proves:** if Spark retries a micro-batch, nothing is written twice.
- Start the job and cancel it while the silver task runs, then run it again.

**Expect:** validation passes. Why: Spark only marks a batch done in the checkpoint after the sink succeeds, so it re-runs the same batch; both Silver writes are insert-if-new MERGEs. Covered by unit test `test_silver_writer_is_idempotent_across_batches_and_retries`.

## 6. Late and out-of-order events
**Proves:** Gold uses business logic, not arrival order.
```bash
python producer/producer.py --count 300 --rate 0 --late-ratio 0.1
```
**Expect:** late events land in the right hour of `gold_sales_by_hour` (Gold is recomputed). An order that is SHIPPED never goes back to PAID in `gold_orders_current`, because it is ranked by lifecycle first (unit test `test_current_status_ignores_late_lower_status`).

## 7. Wrong certificate / wrong password
**Proves:** security is enforced, and you can recognise the errors.
- Put a different PEM into the `kafka-ca-pem` secret → the bronze task fails with `SSLHandshakeException ... PKIX path building failed`. **Sandbox:** failed in 16 s.
- Wrong password in `.env` → producer prints `SASL authentication error` and exits with code 1. **Sandbox:** confirmed.

## 8. Kafka unavailable / job paused for days
- Pause the schedule for longer than the topic retention (Aiven default is often 1–7 days), produce data, wait, unpause.

**Expect:** the job fails with a data-loss error (`failOnDataLoss = true`). That is on purpose: a silent gap in orders is worse than a red job. Fix: run more often or raise retention, then decide whether to accept the gap.

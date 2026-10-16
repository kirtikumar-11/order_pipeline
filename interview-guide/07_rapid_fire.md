# 7 · Rapid-fire questions (flash cards)

Cover the answer, say yours, check. Aim for 1–3 sentences each.

## Kafka
1. **What guarantees ordering in Kafka?** Only within a partition. Same key → same partition.
2. **What's an offset?** Position of a record within a partition; consumers track it to know where they are.
3. **Max useful consumers in a group?** Number of partitions; extra consumers sit idle.
4. **`acks=all` means?** Leader waits for all in-sync replicas before confirming.
5. **Why `min.insync.replicas=2`?** With `acks=all`, a write fails instead of being stored on just one broker.
6. **What does the idempotent producer prevent?** Duplicates caused by producer retries within a session.
7. **What doesn't it prevent?** Your app re-sending after a crash. Use transactions or event IDs.
8. **Log compaction?** Keeps the latest record per key; tombstone (null value) deletes a key.
9. **How to increase throughput of a producer?** Batching (`linger.ms`, `batch.size`), compression (lz4/zstd), async sends.
10. **What happens when you add partitions?** Keys may map to new partitions; per-key ordering breaks across the change.
11. **Rebalance?** Partitions are reassigned among consumers when one joins/leaves; processing pauses briefly.
12. **SASL_SSL vs SSL?** SASL_SSL = TLS encryption + username/password-style auth; SSL = TLS (optionally with client certs for auth).
13. **Why Schema Registry?** Central, versioned schemas with compatibility checks so producers can't break consumers.
14. **Kafka vs RabbitMQ in one line?** Kafka is a replayable log for many consumers; RabbitMQ is a queue where messages are removed once processed.
15. **Where does Spark store Kafka offsets?** In its checkpoint, not in Kafka consumer-group commits.

## Spark Structured Streaming
16. **Micro-batch?** Spark processes new data in small batches on a trigger.
17. **What's in a checkpoint?** Offsets log, commits log, metadata, and state store files.
18. **`startingOffsets` after the first run?** Ignored; the checkpoint decides.
19. **`availableNow` vs `processingTime`?** availableNow processes everything currently available and stops; processingTime runs forever on an interval.
20. **Watermark?** Max event time seen minus allowed lateness; older state is dropped and later rows ignored in aggregations.
21. **Output modes?** Append (final rows), Update (changed rows), Complete (whole result).
22. **Why `foreachBatch`?** To use batch APIs (MERGE, multiple sinks, JDBC) on each micro-batch.
23. **Is `foreachBatch` exactly-once?** At-least-once; make the write idempotent (MERGE or txnAppId/txnVersion).
24. **How to dedupe a stream with bounded state?** `withWatermark` + `dropDuplicatesWithinWatermark`.
25. **Stream-stream join requirement?** Watermarks on both sides + a time-range condition to bound state.
26. **`maxOffsetsPerTrigger`?** Caps records per batch; controls batch size and backlog catch-up.
27. **Signs of falling behind?** Processed rate < input rate; batch duration > trigger interval; offsets behind growing.
28. **RocksDB state store, why?** Keeps large state off the JVM heap, avoids GC/OOM issues.
29. **Event time vs processing time?** When it happened vs when it was processed. Business metrics use event time.
30. **Can you change a streaming query and keep the checkpoint?** Stateless changes usually yes; changes to stateful operators/sources need a new checkpoint.

## Delta Lake & Databricks
31. **What makes Delta ACID?** The transaction log with optimistic concurrency.
32. **Time travel?** Query old versions via `VERSION AS OF` / `TIMESTAMP AS OF`, until VACUUM removes the files.
33. **VACUUM default retention?** 7 days.
34. **OPTIMIZE?** Compacts small files into larger ones.
35. **Liquid clustering vs partitioning?** Clustering adapts layout by chosen columns without fixed directories; avoids over-partitioning. Preferred for most new tables.
36. **When to partition a Delta table?** Very large tables, by a low-cardinality column always used in filters (date).
37. **Schema enforcement vs evolution?** Enforcement rejects mismatched writes; evolution (`mergeSchema`) adds new columns.
38. **Change Data Feed?** Row-level change log per version for incremental downstream processing.
39. **MERGE "multiple source rows matched"?** Dedupe the source by key first.
40. **Delta sink exactly-once how?** Stores the streaming query ID + batch ID in the log and skips a batch already committed.
41. **Auto Loader?** Incremental, scalable ingestion of new files from cloud storage with schema handling.
42. **Lakeflow Declarative Pipelines / DLT?** Declarative tables with expectations (data-quality rules), managed orchestration and retries.
43. **Unity Catalog?** Central governance: three-level names, permissions, lineage, volumes.
44. **Databricks Secrets?** Secure storage for credentials; values are redacted in notebook output.
45. **Why serverless might reject a streaming job?** Serverless (on many workspaces) only supports `availableNow` triggers.
46. **Deletion vectors?** Mark deleted rows without rewriting whole files; faster DELETE/UPDATE/MERGE.

## Design & concepts
47. **Bronze vs Silver vs Gold?** Raw as received / cleaned, typed, deduped / business-ready aggregates.
48. **Why keep Bronze raw?** To replay and rebuild downstream after bugs or schema changes.
49. **Lambda vs Kappa?** Lambda: separate batch + speed layers; Kappa: one streaming path, reprocess by replaying. Lakehouse with streaming+batch on the same tables is close to Kappa.
50. **At-least-once + idempotent sink equals?** Effectively exactly-once results.
51. **Dead-letter queue?** Where bad records go with the error, instead of failing or dropping.
52. **SCD Type 2?** History rows with `valid_from`, `valid_to`, `is_current`.
53. **Why decimal for money?** Floats can't represent decimals exactly; sums drift.
54. **Data contract?** An agreed schema + semantics + SLA between producer and consumer teams, enforced in CI or the registry.
55. **Backpressure?** When downstream can't keep up; handle by buffering (Kafka), rate limits, scaling.
56. **Hot key fix?** Finer key, salting with two-stage aggregation.
57. **RPO vs RTO?** Max acceptable data loss vs max acceptable downtime.
58. **How do you test a pipeline?** Unit tests on pure transform functions, integration tests with a local Kafka/Spark, data-quality checks in production, failure drills.
59. **Freshness metric?** `now − max(event_time)` in the output table.
60. **First question for any design?** "What decision or product does this data serve, and how fresh must it be?"

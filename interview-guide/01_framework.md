# 1 · The method: how to answer any scenario question

Scenario questions are open on purpose. The interviewer is checking whether you **gather requirements before designing**, **reason about trade-offs**, and **think about failure**. A great answer follows the same six steps every time.

```text
1. Clarify      What exactly is the problem? Who uses the output?
2. Quantify     How much data, how fast, how fresh, how long kept?
3. Constrain    Guarantees, budget, team, cloud, compliance
4. Design       Draw the boxes: source → transport → processing → storage → serving
5. Trade-offs   What I chose, what I gave up, when I'd choose differently
6. Operate      Failure modes, monitoring, data quality, backfill, cost
```

Spend roughly: 10–15% on steps 1–3, 40% on step 4, 20% on step 5, 25% on step 6. Most candidates skip 1–3 and 6. Those are exactly where seniors stand out.

---

## Step 1 · Clarify (what and for whom)
Ask these out loud. Even if the interviewer says "you decide", **state your assumption** and move on.

- What is the business goal? (A dashboard? An alert? A feature for an ML model? A report for finance?)
- Who or what consumes the output? (Humans looking at a dashboard, an app calling an API, another pipeline)
- What is the source? (App events, database tables, files, third-party API, IoT devices)
- What does one record look like? Is there a unique ID?
- Is this new, or replacing something? What's wrong with the current one?

## Step 2 · Quantify (the numbers that decide the technology)
| Ask | Why it matters | Rough thresholds |
|---|---|---|
| **Latency / freshness:** how old can the result be? | Picks batch vs streaming | < 1 s → true streaming (Flink, Kafka Streams). Seconds–minutes → Spark Structured Streaming. 15 min+ → scheduled incremental (`availableNow`). Hours → batch. |
| **Volume:** events per second, average and peak; record size | Sizing partitions, cluster, cost | 1 KB × 10,000/s = 10 MB/s ≈ 0.86 TB/day |
| **Growth:** 10× in a year? | Avoid redesign | Choose something that scales horizontally |
| **Retention / replay:** how far back must we reprocess? | Kafka retention, Bronze storage | Kafka for days; lakehouse (Bronze) for years |
| **History size** for backfills | Batch job sizing | |
| **Query pattern:** who queries Gold, how often, how fast? | Serving layer | BI dashboards → Delta/warehouse. Millisecond key lookups → Cassandra/Redis/Postgres |

**Do the maths out loud.** "10k events/s at 1 KB is 10 MB/s, about 860 GB a day raw, maybe 150 GB compressed in Parquet. One year is ~55 TB. That's lakehouse territory, not Postgres."

## Step 3 · Constrain (guarantees and real-world limits)
- **Correctness:** can we lose an event? Can we count one twice? (→ at-most / at-least / exactly-once)
- **Ordering:** does order matter? Globally, or per customer/device/order? (→ partition key)
- **Late / out-of-order data:** how late can events arrive? (→ watermarks, event time vs processing time)
- **Data quality:** what if a record is malformed? Drop, fix, or quarantine?
- **Schema changes:** will producers add fields? Who owns the schema?
- **Privacy / compliance:** PII? GDPR delete requests? Data residency?
- **Cost / team / cloud:** existing cloud, budget, what the team already knows, managed vs self-hosted
- **Availability:** what happens if the pipeline is down for an hour? Is that a disaster or fine?

## Step 4 · Design (draw it)
Always draw the same five layers, then fill them in:

```text
Source  →  Transport/Buffer  →  Processing  →  Storage  →  Serving
(app,DB)    (Kafka, Kinesis)     (Spark,Flink)  (Delta     (BI, API,
                                                Bronze/     Cassandra,
                                                Silver/     alerts)
                                                Gold)
```
For each box, say **one reason** linked to step 2 or 3.

## Step 5 · Trade-offs
Use this sentence shape:
> "I chose **X** because **requirement**. The cost is **downside**. If **condition changes**, I'd switch to **Y**."

Example: "I chose an `availableNow` job every 15 minutes because the dashboard only needs 15-minute freshness and it's 5–10× cheaper than an always-on cluster. The cost is latency. If the business later needs fraud alerts in seconds, that path would move to an always-on stream or Flink."

## Step 6 · Operate (what makes you sound senior)
Cover at least four of these:
- **Failure & recovery:** checkpoints, replay from Kafka, idempotent writes, retries
- **Duplicates:** where they can come from (producer retry, replay, at-least-once sinks) and where you remove them
- **Bad data:** validation rules, quarantine/dead-letter table, alerting on bad-row rate
- **Monitoring:** consumer lag (input rate vs processing rate), batch duration, freshness (now − max event time), row counts per layer, data-quality metrics
- **Backfill / reprocessing:** how to rebuild Silver/Gold after a bug fix (from Bronze)
- **Schema evolution:** what happens when a field is added or changes type
- **Cost:** autoscaling, triggers, file compaction, storage tiers
- **Security:** encryption in transit (TLS), authentication (SASL/SCRAM, mTLS, IAM), secrets manager, least-privilege access, PII masking

---

## The requirements checklist (print this)

```text
FUNCTIONAL                          NON-FUNCTIONAL
[ ] Business goal / consumer        [ ] Latency (p50 / p99) and freshness
[ ] Source system(s) and format     [ ] Throughput avg / peak, record size
[ ] Unique ID per record?           [ ] Growth over 1–3 years
[ ] Transformations / joins         [ ] Delivery guarantee (lost? duplicated?)
[ ] Aggregations / windows          [ ] Ordering (global / per key / none)
[ ] Output shape & serving          [ ] Late data tolerance
[ ] History / backfill needs        [ ] Retention & replay window
                                    [ ] Availability / recovery time
DATA                                [ ] Cost ceiling
[ ] Schema owner, evolution         [ ] Team skills, cloud, managed vs self-run
[ ] Data quality rules              [ ] Security, PII, compliance, residency
[ ] PII fields
```

---

## Worked example of the method (short)

> **"Design a pipeline that shows live revenue on a dashboard for an e-commerce site."**

1. **Clarify:** "Is 'live' seconds or minutes? Is the dashboard for executives or the ops team? Is revenue at order creation, payment or shipping? Do refunds count?"
2. **Quantify:** "Assume 500 orders/s at peak during sales, 2 KB each = 1 MB/s; 'live' means within 1 minute."
3. **Constrain:** "Revenue must not double count, so I need dedup. Refunds arrive late. We're on Databricks already."
4. **Design:** "Order service → Kafka `orders` (key = order_id, 6 partitions) → Spark Structured Streaming 30-second trigger → Bronze (raw) → Silver (parsed, MERGE dedup on event_id) → Gold revenue per minute (streaming aggregation with 10-minute watermark) → Databricks SQL dashboard refreshing every minute."
5. **Trade-offs:** "Spark over Flink because 30-second latency is enough and the team knows Spark; always-on trigger because 1-minute freshness rules out a 15-minute schedule; the cost is a cluster running 24/7."
6. **Operate:** "Alert if processing rate < input rate for 10 minutes or if freshness > 5 minutes; quarantine bad events; daily reconciliation of Gold revenue vs the payments database."

That's a strong answer in about 4 minutes.

## Phrases that help
- "Before I design, can I check a few requirements?"
- "I'll assume X; tell me if that's wrong."
- "The key question here is ordering / latency / correctness, so let me focus on that."
- "There are two reasonable options; here's how I'd pick between them."
- "What happens when this fails? Let me walk through it."

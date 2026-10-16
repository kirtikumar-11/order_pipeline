# Project Walkthrough: Real-Time Order Processing Pipeline

This walkthrough follows **one order** from the moment Python creates it until it shows up as revenue on a Gold table. At each stop you'll see which file does the work, the important lines of code, why it's written that way, and what an interviewer might ask about it.

Read it with the code open beside you. Allow about 90 minutes the first time.

**Contents**
1. [The big picture](#1-the-big-picture)
2. [Folder tour](#2-folder-tour)
3. [Stop 1: The producer creates an order](#3-stop-1-the-producer-creates-an-order)
4. [Stop 2: Kafka stores it](#4-stop-2-kafka-stores-it)
5. [Stop 3: Databricks connects securely](#5-stop-3-databricks-connects-securely)
6. [Stop 4: Bronze, the raw copy](#6-stop-4-bronze-the-raw-copy)
7. [Stop 5: Silver, clean and deduplicated](#7-stop-5-silver-clean-and-deduplicated)
8. [Stop 6: Gold, business answers](#8-stop-6-gold-business-answers)
9. [Stop 7: Validate, the safety net](#9-stop-7-validate-the-safety-net)
10. [Stop 8: The job that runs it all](#10-stop-8-the-job-that-runs-it-all)
11. [What happens when things go wrong](#11-what-happens-when-things-go-wrong)
12. [Testing: how we know it works](#12-testing-how-we-know-it-works)
13. [Your hands-on checklist](#13-your-hands-on-checklist)
14. [Explaining it in an interview](#14-explaining-it-in-an-interview)

---

## 1. The big picture

```text
 YOUR LAPTOP                AIVEN (cloud)                 DATABRICKS (cloud)
┌──────────────┐         ┌──────────────────┐   ┌─────────────────────────────────────────┐
│ producer.py  │ ──TLS──►│ Kafka topic      │──►│ 01_bronze  ► bronze_orders   (raw)      │
│ makes orders │  SCRAM  │ "orders"         │   │ 02_silver  ► silver_order_events (clean)│
│ as JSON      │         │ partitions 0,1,2 │   │            ► silver_quarantine  (bad)   │
└──────────────┘         └──────────────────┘   │ 03_gold    ► gold_* tables  (answers)   │
                                                │ 04_validate  checks, fails job if wrong │
                                                └─────────────────────────────────────────┘
```

Three ideas run through the whole project. If you remember only these, you can explain everything else:

1. **Never lose data.** Kafka buffers it, Bronze keeps the raw copy, and bad rows go to quarantine instead of being dropped.
2. **Never count anything twice.** Every event has an `event_id`, and every write is "insert only if new", so retries and replays are harmless. This is called being **idempotent**.
3. **Every run continues where the last one stopped.** Spark's **checkpoint** remembers which Kafka offsets and which Bronze rows were already processed.

---

## 2. Folder tour

```text
order-pipeline/
├── .env.example              ← template for your Aiven credentials (copy to .env)
├── producer/producer.py      ← Stop 1: makes order events and sends them to Kafka
├── databricks/
│   ├── databricks.yml        ← Stop 8: bundle config (deploy with one command)
│   ├── resources/order_pipeline_job.yml   ← the 4-task scheduled job
│   └── src/
│       ├── order_pipeline/   ← ALL the real logic, as plain Python functions
│       │   ├── config.py     ← Stop 3: Kafka connection options, trigger choice
│       │   ├── settings.py   ← reads widgets + Databricks Secrets → table names, paths
│       │   ├── transforms.py ← Stops 4–6: Bronze, Silver, Gold logic
│       │   └── checks.py     ← Stop 7: health checks
│       └── notebooks/        ← thin notebooks that just call the functions above
│           ├── 00_setup_and_connection_check.py
│           ├── 01_bronze.py  02_silver.py  03_gold.py  04_validate.py
├── local/run_local_pipeline.py  ← run the same pipeline on your laptop
├── tests/                    ← unit tests (pytest)
├── docs/                     ← failure tests + design decisions
└── interview-guide/          ← scenario interview prep
```

**Why are notebooks so thin?** All logic lives in `.py` modules. Notebooks only read settings and call functions. That gives you three things: the code can be **unit-tested** on a laptop without Databricks, the **same code** runs locally and in Databricks, and changes show up cleanly in **Git diffs** (notebook JSON diffs are messy). Interviewers like hearing this.

---

## 3. Stop 1: The producer creates an order

**File:** `producer/producer.py`

### 3.1 Reading credentials safely
```python
def build_config() -> dict:
    load_dotenv(find_dotenv(usecwd=True))   # loads .env from the folder you run in
    ...
    config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": protocol,             # SASL_SSL
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": ..., "sasl.password": ...,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
    }
    config["ssl.ca.location"] = ca                 # Aiven's CA certificate
```
What each setting does, in plain words:

| Setting | Meaning | Why |
|---|---|---|
| `SASL_SSL` | Encrypt the connection (TLS) and log in with a username and password (SASL) | Nobody can read or fake our traffic |
| `SCRAM-SHA-256` | The login method. The password is never sent in plain text | Aiven's standard SASL method |
| `ssl.ca.location` | "Trust only brokers whose certificate is signed by this CA" | Stops someone pretending to be our broker |
| `acks=all` | Kafka confirms only after **all in-sync copies** have saved the message | No data loss if one broker dies |
| `enable.idempotence` | Kafka drops duplicate copies caused by the producer retrying | No duplicates from network hiccups |
| `linger.ms=20` | Wait up to 20 ms to group messages into one batch | Far fewer network calls |
| `compression.type=lz4` | Compress each batch | Less network and disk |

The script **fails fast** with a clear message if `.env` is missing values or the CA file doesn't exist. That's better than a confusing Kafka error later.

### 3.2 Simulating a real order lifecycle
`OrderSimulator` keeps a pool of open orders. Each time it's asked for an event it either:
- creates a **new order** (`status = CREATED`), or
- **advances an existing order**: CREATED → PAID → SHIPPED.

Like real life, 15% of carts are abandoned (never paid) and some paid orders stop before shipping.

One event looks like this:
```json
{
  "event_id": "3f0c1d9e-...",      ← unique per event: the key to removing duplicates later
  "order_id": 1790664001,
  "customer_id": 370,
  "product": "Keyboard",
  "amount": 4508.36,
  "status": "PAID",
  "timestamp": 1790664086.123      ← epoch seconds (when it happened)
}
```
**Why add `event_id` when your original design didn't have one?** The same order produces several events (CREATED, PAID, SHIPPED), so `order_id` can't identify one event. Without a unique event ID, a re-sent message looks like a new one. Idempotence in the producer only protects against *its own retries*; if the app crashes and re-sends, Kafka gets a real duplicate. `event_id` lets Silver catch those.

### 3.3 Sending to Kafka
```python
key, value = str(event["customer_id"]).encode(), json.dumps(event).encode()
producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
producer.poll(0)
...
remaining = producer.flush(30)
```
- **Key = `customer_id`.** Kafka hashes the key to choose a partition. Same customer → same partition → that customer's events stay **in order**.
- `produce()` is **asynchronous**: it puts the message in a local buffer and returns immediately. The library sends batches in the background.
- `on_delivery` is called later with success or an error. `poll(0)` gives the library a chance to run those callbacks.
- `flush()` at the end waits until everything is delivered. If anything failed, the script exits with code 1, so a scheduler would notice.
- `BufferError` means the local buffer is full; we `poll` to let it drain and try again (simple **backpressure**).

### 3.4 Failure-test switches
```bash
python producer/producer.py --count 300 --dup-ratio 0.1 --bad-ratio 0.05 --late-ratio 0.05
```
- `--dup-ratio`: re-sends the **exact same bytes** (same `event_id`), like an app retry.
- `--bad-ratio`: sends broken messages: truncated JSON, missing `order_id`, negative amount, unknown status, amount as text.
- `--late-ratio`: event timestamp 2 hours in the past (late data).

At the end it prints a summary like `{'sent': 300, 'delivered': 300, 'failed': 0, 'dups': 24, 'bad': 15, 'late': 14}`. You'll use those numbers to check the pipeline later.

**Interview angle:** "What's the difference between idempotence and exactly-once?" Idempotence stops duplicates from retries within one producer session. It doesn't stop your application from sending the same business event twice. That's why the event carries its own ID.

---

## 4. Stop 2: Kafka stores it

Nothing in this repo runs here; Aiven runs Kafka for you. But you must understand what happens.

```text
Topic "orders"
 partition 0:  [off 0][off 1][off 2][off 3] ...  ← customer 370's events always land here
 partition 1:  [off 0][off 1][off 2] ...
 partition 2:  [off 0][off 1] ...
```
- Each partition is an **append-only log**. Each record gets the next **offset** (0, 1, 2…).
- **Order is guaranteed only inside a partition.** Across partitions there is no global order.
- Kafka **keeps** records for the retention period (e.g. 7 days) whether or not anyone read them. Reading doesn't delete. That's what makes **replay** possible.
- Many readers can read the same topic independently; each tracks its own position.

**Interview angle:** "If you add a 4th partition, what happens?" The key→partition mapping is `hash(key) % number_of_partitions`, so some customers move to a different partition. Their ordering isn't guaranteed during the change. Plan the partition count up front.

---

## 5. Stop 3: Databricks connects securely

**Files:** `databricks/src/order_pipeline/config.py`, `settings.py`, notebook `00_setup_and_connection_check.py`

### 5.1 Secrets, not passwords in code
You put four values into a Databricks **secret scope** called `order-pipeline` (README step 3). `settings.py` reads them:
```python
bootstrap_servers=dbutils.secrets.get(scope, "kafka-bootstrap-servers"),
username=dbutils.secrets.get(scope, "kafka-username"),
password=dbutils.secrets.get(scope, "kafka-password"),
ca_pem = dbutils.secrets.get(scope, "kafka-ca-pem")
```
If you try to `print()` a secret in a notebook, Databricks shows `[REDACTED]`. Nothing sensitive ends up in notebook output or Git.

### 5.2 Building the Kafka options
```python
opts = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": "orders",
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config":
        'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
        'username="..." password="...";',
    "kafka.ssl.truststore.type": "PEM",
    "kafka.ssl.truststore.certificates": ca_pem,
}
```
Line by line:
- Options starting with **`kafka.`** are passed straight to the Kafka client inside Spark. The others (`subscribe`, `startingOffsets`, …) are for Spark itself.
- **`startingOffsets=earliest`** is used **only on the very first run**, when there's no checkpoint. After that, the checkpoint decides where to start. People get this wrong in interviews.
- **`maxOffsetsPerTrigger`** caps how many records one micro-batch takes. If the job was paused for a day, the backlog is processed in steady chunks instead of one giant batch.
- **`failOnDataLoss=true`**: if Kafka already deleted records we never read (retention passed), stop with an error. A loud failure beats a silent hole in your revenue numbers.
- **`kafka.sasl.jaas.config`**: how Java Kafka clients receive login details. On Databricks the class name starts with **`kafkashaded.`** because Databricks bundles a renamed copy of the Kafka library. Open-source Spark uses the normal name (`shaded=False` in the local runner).
- **`truststore.type=PEM` + `certificates`**: give Spark the Aiven CA **as text**, the same PEM file your producer uses. No need to convert it into a Java keystore (`.jks`) and no file path to manage. Alternative: put `ca.pem` in a Unity Catalog Volume and use `ca_volume_path`.

### 5.3 Notebook 00: check before you build
`00_setup_and_connection_check.py` does four things, in order:
1. `CREATE SCHEMA IF NOT EXISTS workspace.orders_pipeline`
2. `CREATE VOLUME ... checkpoints`: a governed folder where checkpoints will live
3. Lists the secret scope and fails with a clear list of anything missing
4. Does a **batch** read of Kafka (`spark.read`, not `readStream`) and shows the latest 10 records and a count per partition

Why a batch read? It needs no checkpoint and finishes immediately, so it's the quickest way to prove "network + TLS + login + topic all work".

---

## 6. Stop 4: Bronze, the raw copy

**Files:** `transforms.py` (`bronze_from_kafka`, `start_bronze`), notebook `01_bronze.py`

### 6.1 What Spark gets from Kafka
Every Kafka record arrives in Spark as a row with these columns:

| key | value | topic | partition | offset | timestamp | … |
|---|---|---|---|---|---|---|
| binary | binary | string | int | long | timestamp | |

`key` and `value` are **bytes**. Spark doesn't know they're JSON.

### 6.2 What we keep
```python
def bronze_from_kafka(kafka_df):
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),   # the JSON, untouched
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )
```
We turn bytes into text but **don't parse the JSON** here. Bronze is a faithful receipt of what arrived. If our Silver parsing has a bug next month, we can rebuild Silver from Bronze even after Kafka has deleted the data.

`topic`, `partition` and `offset` together are the **unique physical address** of a record. They let you trace any row back to the exact Kafka message.

### 6.3 Writing the stream
```python
stream = spark.readStream.format("kafka").options(**kafka_opts).load()
writer = (bronze_from_kafka(stream).writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpoint)   # /Volumes/.../checkpoints/bronze_orders
          .queryName("bronze_orders"))
apply_trigger(writer, "available_now").toTable("workspace.orders_pipeline.bronze_orders")
```
- `readStream` instead of `read` makes it a **streaming** query: Spark remembers progress.
- **`checkpointLocation`** is where Spark saves which offsets each batch covered and whether it finished. Each query gets its **own** checkpoint folder. Never share one.
- **Delta sink + checkpoint = exactly-once into Bronze.** If a batch fails halfway, Spark re-runs it, and the Delta sink knows (from the batch ID it stored) not to write it twice.

### 6.4 The trigger
```python
def apply_trigger(writer, trigger):
    if trigger == "available_now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=trigger)   # e.g. "30 seconds"
```
Your Databricks compute rejected always-on streams with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. That's how **serverless** behaves. `availableNow` means: "read everything that's new right now (in several batches if needed), write it, then **stop**." Running that every 15 minutes gives you "near real-time" at a fraction of the cost of a cluster running 24/7. It's still real streaming, with checkpoints and exactly-once.

In the notebook, `query.awaitTermination()` waits until it stops, then prints `lastProgress` (rows read, durations, offsets). The last cell shows row counts and offset ranges per partition.

---

## 7. Stop 5: Silver, clean and deduplicated

**Files:** `transforms.py` (`parse_and_validate`, `split_good_bad`, `make_silver_batch_writer`, `start_silver`), notebook `02_silver.py`

This is the most important part of the project, and the one interviewers dig into most.

### 7.1 Reading Bronze as a stream
```python
spark.readStream.table(bronze_table)
```
A Delta table can be a **streaming source**. Spark reads only rows added since the last run, using Silver's **own** checkpoint. Bronze and Silver are two independent streaming queries.

### 7.2 Parsing with an explicit schema
```python
ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("product", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("timestamp", DoubleType()),
])
parsed = bronze_df.withColumn("o", F.from_json("raw_value", ORDER_SCHEMA))
```
`from_json` turns the JSON text into a struct column `o`. With a **fixed schema**, bad input can't silently change the table's types: broken JSON gives nulls instead of crashing, and a wrong type (like `"amount": "nine hundred"`) gives null for that field. Our checks then catch the nulls.

Schema **inference** (letting Spark guess) isn't safe on a stream: tomorrow's data might be guessed differently.

### 7.3 Data-quality checks
```python
checks = F.array(
    F.when(<all key fields null>,              F.lit("unparseable_json")),
    F.when(o("order_id").isNull(),             F.lit("missing_order_id")),
    F.when(o("timestamp").isNull(),            F.lit("missing_timestamp")),
    F.when(o("product").isNull(),              F.lit("missing_product")),
    F.when(o("amount").isNull() | (o("amount") <= 0), F.lit("invalid_amount")),
    F.when(status not in CREATED/PAID/SHIPPED, F.lit("invalid_status")),
)
... F.filter(checks, lambda c: c.isNotNull()).alias("dq_errors")
```
Each check gives a reason or null. After removing the nulls, every row has a list `dq_errors`:
- `[]` means a good row
- `["invalid_amount"]` or `["missing_order_id", "missing_product"]` means a bad row, and the list says exactly why

Collecting **all** reasons, not just the first, makes fixing the producer much faster.

### 7.4 Cleaning and typing
```python
F.initcap(F.trim(o("product")))            # " keyboard" → "Keyboard"
o("amount").cast("decimal(12,2)")          # money as exact decimal
F.upper(F.trim(o("status")))               # "paid " → "PAID"
F.timestamp_seconds(o("timestamp"))        # 1790664086.12 → 2026-09-29 06:41:26
```
**Why decimal for money?** Floating point numbers can't store most decimals exactly (0.1 + 0.2 = 0.30000000000000004). Over millions of orders, sums drift. `decimal(12,2)` is exact.

### 7.5 The dedup key
```python
F.coalesce(o("event_id"),
           F.sha2(F.concat_ws("|", o("order_id"), o("status"), o("timestamp")), 256)
).alias("event_key")
```
Use `event_id` if it's present. For older events without one (your original producer), build a fingerprint from the fields that identify "this order reached this status at this time".

### 7.6 Splitting good and bad, and in-batch duplicates
```python
good = checked.filter(F.size("dq_errors") == 0)
w = Window.partitionBy("event_key").orderBy("kafka_timestamp", "partition", "offset")
good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
bad  = checked.filter(F.size("dq_errors") > 0)
```
If the same event appears twice **in the same batch**, keep only the first copy Kafka received. This is necessary, not just tidy: a MERGE fails with *"multiple source rows matched the same target row"* if the batch itself has duplicates.

### 7.7 Writing with `foreachBatch` + MERGE
```python
def write_batch(batch_df, batch_id):
    good, bad = split_good_bad(batch_df)

    DeltaTable.forName(spark, silver_table).alias("t") \
        .merge(good.alias("s"), "t.event_key = s.event_key") \
        .whenNotMatchedInsertAll() \
        .execute()

    DeltaTable.forName(spark, quarantine_table).alias("t") \
        .merge(bad.alias("s"), "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset") \
        .whenNotMatchedInsertAll() \
        .execute()
```
- **`foreachBatch`** hands you each micro-batch as a normal DataFrame, so you can use batch-only operations like MERGE and write to **two** tables.
- **`whenNotMatchedInsertAll`** = "insert only if this key isn't already there". Nothing is ever updated or inserted twice.
- Silver is keyed by **`event_key`** (the *business* identity: "this event"). Quarantine is keyed by **topic/partition/offset** (the *physical* identity: "this Kafka record"), because bad rows may have no usable ID.

**Why this matters:** `foreachBatch` on its own is *at-least-once*. If Spark crashes after writing but before saving the checkpoint, it re-runs the same batch. Because both writes are insert-if-new MERGEs, the re-run changes nothing. That makes the result **effectively exactly-once**.

This one design handles **four** sources of duplicates:

| Duplicate source | Caught by |
|---|---|
| Producer retry (network) | Kafka idempotent producer |
| App re-sends the same event | MERGE on `event_key` |
| Spark retries a batch | MERGE on `event_key` / offset |
| Checkpoint lost → Kafka replayed | MERGE on `event_key` / offset |

`ensure_silver_tables()` creates both tables empty before the first run, because a MERGE needs an existing target.

### 7.8 Alternatives you should know
- `dropDuplicates(["event_id"])` on a stream **without a watermark**: state grows forever. Avoid.
- `withWatermark(...).dropDuplicatesWithinWatermark(["event_id"])`: bounded state and faster, but it only catches duplicates that arrive within the watermark window, and a lost checkpoint loses the state.
- MERGE (our choice): catches duplicates across all history and survives replays. The cost is that it reads the target table, which gets slower as Silver grows. Clustering Silver on `event_key` and limiting the match to recent dates fixes that later.

---

## 8. Stop 6: Gold, business answers

**Files:** `transforms.py` (`orders_current`, `revenue_by_product`, `orders_by_status`, `sales_by_hour`, `build_gold`), notebook `03_gold.py`

### 8.1 Current status per order
```python
def status_rank():
    return F.when(status == "SHIPPED", 3).when(status == "PAID", 2).otherwise(1)

w = Window.partitionBy("order_id").orderBy(status_rank().desc(), F.col("event_ts").desc())
... row_number() over w, keep row 1
```
Silver has one row per **event**, so one order may have three rows. Gold needs one row per **order**.

Why rank by lifecycle first and not just "latest timestamp"? Events can arrive late or with skewed clocks. If a PAID event shows up after SHIPPED, "latest wins" would wrongly move the order back to PAID. Lifecycle ranking makes that impossible. The unit test `test_current_status_ignores_late_lower_status` proves it.

### 8.2 Revenue: count each order once
```python
paid = current.filter(status in ("PAID", "SHIPPED"))
paid.groupBy("product").agg(count, sum("amount"), avg("amount"))
```
A classic mistake would be summing `amount` over all Silver events: an order with CREATED + PAID + SHIPPED would count **three times**. We sum over `orders_current`, so each paid order counts **once**, and unpaid carts count zero.

### 8.3 Sales by hour
```python
silver.filter(status == "PAID").groupBy(date_trunc("hour", "event_ts"))
```
Revenue is placed in the hour the **payment happened** (event time), not the hour we processed it (processing time). A late event still lands in the correct hour.

### 8.4 Why full recompute + overwrite?
```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
```
Gold tables are small. Recomputing them from Silver each run takes seconds and is **always correct**, including late data and replays, with no streaming state. When Silver grows to billions of rows, switch to **incremental** Gold: read only changed rows (e.g. Delta Change Data Feed), find which orders or hours they touch, and MERGE just those.

Overwriting a Delta table is safe for readers: a dashboard query sees either the old version or the new one, never half of each (ACID). Old versions stay available through **time travel** (`VERSION AS OF`).

---

## 9. Stop 7: Validate, the safety net

**Files:** `checks.py`, notebook `04_validate.py`

A job can be "green" and still produce wrong data. This step turns data problems into **job failures**, so you get alerted.

| Check | Meaning | Fails when |
|---|---|---|
| `silver_duplicate_event_keys` | Dedup is working | > 0 |
| `quarantine_duplicate_offsets` | Retries didn't double-insert bad rows | > 0 |
| `gold_orders` vs `silver_orders` | Gold matches Silver | Not equal |
| bad-row ratio | Producer is healthy | > 10% |
| `duplicates_removed` | Bronze offsets − Silver − quarantine = dups removed by MERGE | < 0 (impossible) |
| `bronze_duplicate_offsets` | Kafka was replayed into Bronze | Reported only. Allowed in raw |
| Minutes since last ingest | Freshness | Printed. Add an alert in production |

The accounting identity is worth remembering: **every unique Kafka record ends up in exactly one place: Silver, quarantine, or "removed as a duplicate".** With the producer's summary (`sent`, `dups`, `bad`) you can check this by hand.

---

## 10. Stop 8: The job that runs it all

**Files:** `databricks/databricks.yml`, `databricks/resources/order_pipeline_job.yml`

```text
bronze ──► silver ──► gold ──► validate        every 15 minutes (schedule starts PAUSED)
 retries: 2   retries: 2
```
- **Databricks Asset Bundle**: the job is defined as code (YAML) and deployed with `databricks bundle deploy`. It's versioned in Git and reproducible across environments.
- **`max_concurrent_runs: 1`**: two runs at the same time would fight over the same checkpoint. Never allow that.
- **`depends_on`**: Silver only starts once Bronze finished, and so on.
- **Retries** on Bronze/Silver: safe *because* every step is idempotent. Retrying a non-idempotent step can create duplicates.
- **Job `parameters`** (`catalog`, `schema`, `secret_scope`, `trigger`) are passed to every notebook's widgets with the same name. `settings.read_settings()` turns them into table names and checkpoint paths:

```text
workspace.orders_pipeline.bronze_orders
workspace.orders_pipeline.silver_order_events
workspace.orders_pipeline.silver_quarantine
workspace.orders_pipeline.gold_orders_current / gold_revenue_by_product / ...
/Volumes/workspace/orders_pipeline/checkpoints/bronze_orders
/Volumes/workspace/orders_pipeline/checkpoints/silver_order_events
```
- No cluster settings in the YAML means the job runs on **serverless** compute.

**Latency of this design:** schedule interval + run time, so roughly 15–20 minutes from order to dashboard. For seconds: run Bronze and Silver on classic job compute with `trigger = "30 seconds"` as always-on jobs, and schedule Gold separately.

---

## 11. What happens when things go wrong

Full steps are in `docs/failure_tests.md`. Here's the reasoning for each case:

| Situation | What happens | Why it's safe |
|---|---|---|
| Job stopped, restarted later | Reads from the checkpoint offsets, catches up in `maxOffsetsPerTrigger` chunks | Checkpoint |
| Job crashes mid-batch | Spark re-runs the unfinished batch | Delta sink + MERGE are idempotent |
| Producer sends duplicates | Bronze has both copies; Silver keeps one | MERGE on `event_key` |
| Bad messages | Stream keeps running; rows go to quarantine with reasons | Validation + quarantine |
| Bronze checkpoint deleted | Kafka replayed from `earliest`; Bronze gets duplicate rows | Silver/quarantine MERGEs ignore them |
| Silver logic had a bug | Fix code, rebuild Silver/Gold from Bronze | Bronze keeps raw data |
| Paused longer than Kafka retention | Job fails with a data-loss error | `failOnDataLoss=true`: loud, not silent |
| Wrong CA certificate | `SSLHandshakeException: PKIX path building failed` | TLS verification working as intended |
| Wrong password | `SASL authentication error` | Authentication working as intended |
| Late or out-of-order events | Placed by event time; status never goes backwards | Gold recompute + lifecycle ranking |

---

## 12. Testing: how we know it works

### Unit tests (`tests/`, run with `pytest -q`)
| Test | Proves |
|---|---|
| `test_good_rows_are_parsed_and_typed` | Trimming, upper-casing, decimal amount, timestamp conversion |
| `test_malformed_rows_go_to_quarantine_with_reasons` | Each kind of bad message gets the right reason |
| `test_duplicates_in_one_batch_are_removed` | In-batch dedup before MERGE |
| `test_silver_writer_is_idempotent_across_batches_and_retries` | Same batch twice, a replayed event, and a whole replayed batch still give the right counts |
| `test_current_status_ignores_late_lower_status` | SHIPPED can't go back to PAID |
| `test_revenue_counts_each_paid_order_once` | No triple counting; unpaid orders excluded |
| `test_build_gold_writes_all_tables` | Gold end to end on a small dataset |
| `test_lifecycle_only_moves_forward` | Producer never emits impossible transitions |
| `test_events_have_unique_ids_and_required_fields` | Producer contract |
| `test_bad_messages_are_actually_bad` | The failure injector really produces bad data |

Notice the pattern: tests call **plain functions** with tiny hand-made DataFrames. That only works because the logic isn't trapped inside notebooks.

### End-to-end run (done in a sandbox, not on your accounts)
A local Kafka was set up with **SASL_SSL + SCRAM-SHA-256 and its own CA**, so it had the same security shape as Aiven. The pipeline then ran with exactly the Kafka options the notebooks build:

| Step | Result |
|---|---|
| Producer: 300 events, 24 duplicates, 15 bad | All 300 delivered |
| Run 1 | Bronze 300 · Silver 261 · quarantine 15 · duplicates removed 24 ✔ |
| +100 events, run 2 | Bronze 400 (only new ones read) · Silver 340 · quarantine 20 ✔ |
| Delete Bronze checkpoint, run 3 | Bronze 800 (replayed) · **Silver still 340** · quarantine still 20 ✔ |
| Wrong CA | Failed in 16 s with `PKIX path building failed` ✔ |
| Wrong password | Producer exited 1 with `SASL authentication error` ✔ |

Check the arithmetic: 300 sent − 24 dups − 15 bad = **261** in Silver. That's the accounting identity from Stop 7.

**Not yet run:** the notebooks inside Databricks, and `databricks bundle deploy`. Run notebook 00 first; it catches setup problems early.

### Run it locally yourself
```bash
pip install -r requirements-dev.txt           # needs Java 17+
pytest -q
python local/run_local_pipeline.py            # same code, open-source Spark + Delta
python local/run_local_pipeline.py --drop-checkpoint bronze   # failure test 4
```

---

## 13. Your hands-on checklist

Do these on your own accounts. Each one gives you a first-hand story for interviews.

- [ ] Producer sends 200 events; summary shows `failed: 0`
- [ ] Notebook 00 shows records from all 3 partitions
- [ ] Run the job once; note Bronze/Silver/quarantine counts
- [ ] Produce 100 more, run again; Bronze grows by exactly 100
- [ ] `--dup-ratio 0.1`: `duplicates_removed` matches the producer's `dups`
- [ ] `--bad-ratio 0.05`: quarantine count matches `bad`; look at the reasons in notebook 02
- [ ] Delete the Bronze checkpoint (test schema only!) and rerun; Silver doesn't change
- [ ] Put a wrong CA in the secret; read the error; put the right one back
- [ ] Open `DESCRIBE HISTORY workspace.orders_pipeline.silver_order_events` and find a MERGE's metrics
- [ ] Query a Gold table `VERSION AS OF 0` (time travel)
- [ ] Unpause the schedule and watch 3 runs

---

## 14. Explaining it in an interview

### 30-second version
> "I built a near-real-time order pipeline: a Python producer sends order events over TLS with SCRAM to Kafka on Aiven; Databricks Structured Streaming lands them raw in Bronze Delta, then validates and deduplicates them into Silver with idempotent MERGEs, quarantining bad records, and builds Gold revenue and status tables. It runs every 15 minutes with the availableNow trigger, and I tested restarts, duplicates, malformed data and checkpoint loss."

### Questions to expect, and where the answer is in this walkthrough
| Question | Section |
|---|---|
| Why Kafka and not direct to Delta? | 4, and interview-guide/04 Q2 |
| Why key by customer_id? | 3.3, 4 |
| How do you avoid duplicates end to end? | 7.7 table |
| What's in a checkpoint? What if you lose it? | 6.3, 11 |
| Why availableNow? | 6.4, 10 |
| How do you handle bad data? | 7.3, 7.7 |
| Why decimal? | 7.4 |
| Why not sum amounts in Silver? | 8.2 |
| How would you scale it 100×? | interview-guide/04 Q12 |

### Three things to say you'd improve
1. Avro or Protobuf + Schema Registry instead of JSON, so broken messages are rejected at the producer.
2. Incremental Gold using Change Data Feed once Silver is large.
3. Freshness and quarantine-rate alerts wired to email or Slack, plus a daily reconciliation against a source-of-truth system.

Next: `docs/design_decisions.md` for the trade-off behind each choice, then `interview-guide/` to practise scenarios.

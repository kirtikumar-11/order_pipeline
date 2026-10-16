# Real-Time Order Processing Pipeline

Python producer → **Aiven Kafka** (SASL_SSL + SCRAM) → **Spark Structured Streaming on Databricks** → **Delta Lake Bronze / Silver / Gold**, with data-quality checks and tested failure handling.

```text
producer.py ──JSON──► Kafka topic "orders" (3 partitions, key = customer_id)
                              │
                 01_bronze    ▼   raw JSON + topic/partition/offset, append only
                        bronze_orders  (Delta)
                              │
                 02_silver    ▼   parse with explicit schema, validate, MERGE on event_key
            silver_order_events ──── silver_quarantine (bad rows + reasons)
                              │
                 03_gold      ▼   recompute + overwrite
   gold_orders_current · gold_revenue_by_product · gold_orders_by_status · gold_sales_by_hour
                              │
                 04_validate  ▼   fails the job if duplicates, lost rows or too many bad rows
```

## What's in here

| Path | What it is |
|---|---|
| `producer/producer.py` | Simulates real order lifecycles (CREATED → PAID → SHIPPED). Flags to inject duplicates, bad messages and late events. |
| `databricks/src/order_pipeline/` | All Spark logic as plain Python functions (`transforms.py`), Kafka options (`config.py`), health checks (`checks.py`). |
| `databricks/src/notebooks/` | Thin notebooks: `00_setup_and_connection_check`, `01_bronze`, `02_silver`, `03_gold`, `04_validate`. |
| `databricks/databricks.yml` + `resources/` | Databricks Asset Bundle: deploys the notebooks and a 4-task job, scheduled every 15 min (paused). |
| `local/run_local_pipeline.py` | Runs the same Bronze/Silver/Gold code on your laptop with open-source Spark + Delta. |
| `tests/` | Unit tests for the transforms and the producer (`pytest`). |
| `docs/failure_tests.md` | Step-by-step failure tests: restart, duplicates, bad data, lost checkpoint, wrong certificate. |
| `docs/design_decisions.md` | Why each choice was made. Read this before interviews. |
| `interview-guide/` | Scenario-based interview guide: how to gather requirements and pick technology. |

## What was tested, and what wasn't

Tested in a sandbox (not on your accounts):
- 10 unit tests pass (Spark 4.0.1 + Delta 4.0.0, Python 3.13).
- End to end against a **local Kafka 3.7 broker with SASL_SSL + SCRAM-SHA-256 and a self-signed CA**, using the exact Kafka options the Databricks notebooks build (CA passed inline as PEM text): producer sent 300 events with 24 duplicates and 15 bad messages → Bronze 300, Silver 261, quarantine 15, 24 duplicates removed, all checks passed. A second run picked up only the 100 new events. Deleting the Bronze checkpoint replayed all 400 records into Bronze and Silver stayed exactly the same.
- A wrong CA certificate fails fast with `SSLHandshakeException ... PKIX path building failed`. A wrong password makes the producer exit with `SASL authentication error`.

Not tested (I can't reach your Aiven or Databricks):
- The notebooks on Databricks itself (`dbutils`, widgets, Unity Catalog volumes, the `kafkashaded.` login class).
- `databricks bundle deploy` (the YAML parses; it was not deployed).
- Aiven's real broker. The settings are the same as your verified producer, so it should behave the same.

## Run it on your accounts

### 1. Aiven Kafka
1. In the Aiven console, open your Kafka service. Create topic `orders` with 3 partitions (you already have this).
2. On **Overview → Connection information**, pick **SASL**. Note the host:port, user and password, and download the **CA certificate** to `certs/ca.pem`.
3. Make sure SASL is enabled on the service (Advanced configuration → `kafka_authentication_methods.sasl = true`).

### 2. Producer on your laptop (Windows works)
```bash
python -m venv .venv
.venv\Scripts\activate            # Windows   (macOS/Linux: source .venv/bin/activate)
pip install -r producer/requirements.txt
copy .env.example .env            # macOS/Linux: cp .env.example .env   then fill in your values
python producer/producer.py --count 200 --rate 10
```
You should see `Done. {'sent': 200, 'delivered': 200, 'failed': 0, ...}`. Run it from this folder so it finds `.env`.

### 3. Databricks Secrets (no passwords in notebooks)
Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), then:
```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks secrets create-scope order-pipeline
databricks secrets put-secret order-pipeline kafka-bootstrap-servers --string-value "<host>:<port>"
databricks secrets put-secret order-pipeline kafka-username --string-value "avnadmin"
databricks secrets put-secret order-pipeline kafka-password --string-value "<password>"
databricks secrets put-secret order-pipeline kafka-ca-pem --string-value "$(cat certs/ca.pem)"
```
On Windows PowerShell use `--string-value (Get-Content certs/ca.pem -Raw)` for the last one.

Alternative for the CA: upload `ca.pem` to a Unity Catalog Volume and set the widget `ca_volume_path` to e.g. `/Volumes/workspace/orders_pipeline/config/ca.pem`. The secret is simpler and always works on serverless.

### 4. Deploy and run
**Option A, bundle (recommended):**
```bash
cd databricks
databricks bundle validate
databricks bundle deploy
```
Open the deployed `00_setup_and_connection_check` notebook in the workspace and run it once (it creates the schema and checkpoint volume and shows sample Kafka records). Then:
```bash
databricks bundle run order_pipeline
```

**Option B, by hand:** import this folder into a Databricks Git folder (Workspace → Create → Git folder) or upload `databricks/src`. Run the notebooks in order 00 → 04. Then create a Job with four notebook tasks (bronze → silver → gold → validate), set **Max concurrent runs = 1**, and add a schedule.

If your catalog is not `workspace`, change the `catalog` widget / bundle variable (for example `main`).

### 5. About the trigger
Your compute rejected always-on triggers (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`). That is serverless compute: it only allows `availableNow`. So the job runs every 15 minutes, each run reads everything new since the checkpoint, then stops. This is called **incremental batch streaming** and it is how many teams run "near real-time" pipelines cheaply. Latency = schedule interval + run time.

For second-level latency, run `01_bronze` and `02_silver` on **classic job compute** with `trigger = "30 seconds"` as two separate always-on jobs (they never finish, so Gold must be its own scheduled job).

## Run locally (no Databricks)
Needs Java 17+.
```bash
pip install -r requirements-dev.txt
pytest -q                                   # unit tests
python local/run_local_pipeline.py          # uses .env; talks to Aiven or a local Kafka
```
Tables and checkpoints go to `local-lakehouse/` (delete it to start over).

## Troubleshooting
| Error | Cause and fix |
|---|---|
| `SSLHandshakeException ... PKIX path building failed` | Spark doesn't trust the broker. The CA secret/volume is missing, wrong, or lost its line breaks. Re-put it with `$(cat certs/ca.pem)`. Never disable verification. |
| `SaslAuthenticationException` / `Authentication failed` | Wrong user/password, or SASL is not enabled on the Aiven service, or you used the SSL (client-cert) port instead of the SASL port. |
| `ClassNotFoundException ... ScramLoginModule` | On Databricks the class must start with `kafkashaded.` (`shaded=True`, already set). Locally it must not. |
| `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Serverless: keep `trigger = available_now`. |
| `Partition orders-0's offset was changed from X to Y, some data may have been missed` | Kafka deleted data (retention) before the job read it. Run more often or raise topic retention. To accept the loss for a test, set `failOnDataLoss` to `false` in `config.py`. |
| Checkpoint errors after changing a query | A checkpoint belongs to one query. For a changed query, use a new checkpoint path (and accept that it starts again from `startingOffsets`). |
| Notebook can't `import order_pipeline` | The notebooks must stay in `src/notebooks/` next to `src/order_pipeline/`. After editing a `.py` module, restart Python (`dbutils.library.restartPython()`). |

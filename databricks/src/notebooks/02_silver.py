# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: parse, validate, deduplicate
# MAGIC Streams **new Bronze rows only** (Delta as a streaming source). For each micro-batch:
# MAGIC 1. Parse JSON with an explicit schema, convert types (amount → decimal, epoch → timestamp).
# MAGIC 2. Bad rows → `silver_quarantine` with the reasons. Nothing is silently dropped.
# MAGIC 3. Good rows → `MERGE` into `silver_order_events` on `event_key`, so duplicates and replays are ignored.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import read_settings
from order_pipeline.transforms import start_silver

s = read_settings(dbutils)

# COMMAND ----------

query = start_silver(spark, s["bronze_table"], s["silver_table"], s["quarantine_table"],
                     s["silver_checkpoint"], s["trigger"])
query.awaitTermination()
print(query.lastProgress)

# COMMAND ----------

display(spark.sql(f"SELECT status, count(*) AS events FROM {s['silver_table']} GROUP BY status"))
display(spark.sql(f"""
  SELECT reason, count(*) AS rows
  FROM {s['quarantine_table']} LATERAL VIEW explode(dq_errors) AS reason
  GROUP BY reason ORDER BY rows DESC
"""))

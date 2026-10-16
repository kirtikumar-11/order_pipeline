# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: Kafka → Delta (raw)
# MAGIC Stores every Kafka record **as received** (raw JSON text + topic/partition/offset/timestamp).
# MAGIC The checkpoint remembers the last offset per partition, so each run continues where the last stopped.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import kafka_options_from_secrets, read_settings
from order_pipeline.transforms import start_bronze

s = read_settings(dbutils)
kafka_opts = kafka_options_from_secrets(dbutils, s)

# COMMAND ----------

query = start_bronze(spark, kafka_opts, s["bronze_table"], s["bronze_checkpoint"], s["trigger"])
query.awaitTermination()  # with available_now this returns once all current data is written
print(query.lastProgress)

# COMMAND ----------

display(spark.sql(f"""
  SELECT partition, count(*) AS records, min(offset) AS min_offset, max(offset) AS max_offset
  FROM {s['bronze_table']} GROUP BY partition ORDER BY partition
"""))

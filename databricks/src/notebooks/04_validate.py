# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validate the pipeline
# MAGIC Data-quality checks that **fail the job** if something is wrong, so you hear about it.
# MAGIC Run it after every failure test in `docs/failure_tests.md`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.checks import validate
from order_pipeline.settings import read_settings

s = read_settings(dbutils)

# COMMAND ----------

report, problems = validate(spark, s["bronze_table"], s["silver_table"], s["quarantine_table"], s["gold_prefix"])
report

# COMMAND ----------

lag = spark.sql(f"SELECT timestampdiff(MINUTE, max(ingested_at), current_timestamp()) FROM {s['bronze_table']}").first()[0]
print(f"Minutes since the last record reached Bronze: {lag}")
assert not problems, "Validation failed:\n- " + "\n- ".join(problems)
print("All checks passed.")

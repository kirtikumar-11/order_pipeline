# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: business tables
# MAGIC Recomputed from Silver on every run and overwritten (small tables, always correct, late data included).
# MAGIC - `gold_orders_current`: one row per order with its latest lifecycle status
# MAGIC - `gold_revenue_by_product`: paid/shipped orders only, each order counted once
# MAGIC - `gold_orders_by_status`
# MAGIC - `gold_sales_by_hour`: revenue at the time of payment

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import read_settings
from order_pipeline.transforms import build_gold

s = read_settings(dbutils)

# COMMAND ----------

counts = build_gold(spark, s["silver_table"], s["gold_prefix"])
counts

# COMMAND ----------

display(spark.table(f"{s['gold_prefix']}revenue_by_product").orderBy("revenue", ascending=False))
display(spark.table(f"{s['gold_prefix']}orders_by_status"))
display(spark.table(f"{s['gold_prefix']}sales_by_hour").orderBy("hour"))

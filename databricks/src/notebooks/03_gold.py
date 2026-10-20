










import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import read_settings
from order_pipeline.transforms import build_gold

s = read_settings(dbutils)



counts = build_gold(spark, s["silver_table"], s["gold_prefix"])
counts



display(spark.table(f"{s['gold_prefix']}revenue_by_product").orderBy("revenue", ascending=False))
display(spark.table(f"{s['gold_prefix']}orders_by_status"))
display(spark.table(f"{s['gold_prefix']}sales_by_hour").orderBy("hour"))

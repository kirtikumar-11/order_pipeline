







import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import kafka_options_from_secrets, read_settings
from order_pipeline.transforms import start_bronze


s = read_settings(dbutils)
kafka_opts = kafka_options_from_secrets(dbutils, s)



query = start_bronze(spark, kafka_opts, s["bronze_table"], s["bronze_checkpoint"], s["trigger"])
query.awaitTermination()  
from order_pipeline.metrics import throughput_summary

throughput_summary(query)



display(spark.sql(f"""
  SELECT partition, count(*) AS records, min(offset) AS min_offset, max(offset) AS max_offset
  FROM {s['bronze_table']} GROUP BY partition ORDER BY partition
"""))

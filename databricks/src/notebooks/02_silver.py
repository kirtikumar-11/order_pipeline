









import os
import sys

sys.path.append(os.path.abspath(".."))

from order_pipeline.settings import read_settings
from order_pipeline.transforms import start_silver

s = read_settings(dbutils)



import time

from order_pipeline.metrics import rows_written_since, throughput_summary

started = time.time()
query = start_silver(spark, s["bronze_table"], s["silver_table"], s["quarantine_table"],
                     s["silver_checkpoint"], s["trigger"])
query.awaitTermination()
wall_seconds = time.time() - started

summary = throughput_summary(query)
if summary["rows"] == 0:  
    rows = (rows_written_since(spark, s["silver_table"], started)
            + rows_written_since(spark, s["quarantine_table"], started))
    summary = {"rows_written": rows, "wall_seconds": round(wall_seconds, 1),
               "rows_per_second": round(rows / wall_seconds, 1) if wall_seconds else 0.0}
summary



display(spark.sql(f"SELECT status, count(*) AS events FROM {s['silver_table']} GROUP BY status"))
display(spark.sql(f"""
  SELECT reason, count(*) AS rows
  FROM {s['quarantine_table']} LATERAL VIEW explode(dq_errors) AS reason
  GROUP BY reason ORDER BY rows DESC
"""))

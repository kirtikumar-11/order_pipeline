









import os
import sys

sys.path.append(os.path.abspath(".."))  

from order_pipeline.settings import kafka_options_from_secrets, read_settings

s = read_settings(dbutils)
s



spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s['catalog']}.{s['schema']}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {s['catalog']}.{s['schema']}.checkpoints")
print("Schema and checkpoint volume are ready.")




needed = ["kafka-bootstrap-servers", "kafka-username", "kafka-password"]
if not s["ca_volume_path"]:
    needed.append("kafka-ca-pem")
present = {m.key for m in dbutils.secrets.list(s["secret_scope"])}
missing = [k for k in needed if k not in present]
assert not missing, f"Missing secrets in scope '{s['secret_scope']}': {missing}"
print("All secrets present (values are redacted by Databricks).")




from pyspark.sql import functions as F

opts = kafka_options_from_secrets(dbutils, s)
opts.pop("maxOffsetsPerTrigger")  
sample = (
    spark.read.format("kafka").options(**opts)
    .option("endingOffsets", "latest")
    .load()
    .select("partition", "offset", "timestamp", F.col("key").cast("string"), F.col("value").cast("string"))
)
display(sample.orderBy(F.col("offset").desc()).limit(10))
display(sample.groupBy("partition").agg(F.count("*").alias("records"), F.max("offset").alias("max_offset")))

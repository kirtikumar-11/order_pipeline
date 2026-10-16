"""Bronze / Silver / Gold logic as plain functions so it can be unit-tested without Databricks."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from order_pipeline.config import apply_trigger

# The contract with the producer. Parsing with an explicit schema (not schema inference)
# means a surprise field type can't silently change the table.
ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("product", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("timestamp", DoubleType()),  # epoch seconds from Python time.time()
])
VALID_STATUSES = ["CREATED", "PAID", "SHIPPED"]
# Lifecycle order. Used to pick an order's current status even if events arrive out of order.
def status_rank():
    return F.when(F.col("status") == "SHIPPED", 3).when(F.col("status") == "PAID", 2).otherwise(1)


# ---------------------------------------------------------------- Bronze
def bronze_from_kafka(kafka_df: DataFrame) -> DataFrame:
    """Keep the raw payload exactly as received, plus Kafka metadata for traceability.
    No parsing here: if the parser has a bug, Bronze still has the original bytes to replay.
    """
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def start_bronze(spark: SparkSession, kafka_opts: dict, bronze_table: str, checkpoint: str, trigger: str):
    stream = spark.readStream.format("kafka").options(**kafka_opts).load()
    writer = (
        bronze_from_kafka(stream).writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .queryName("bronze_orders")
    )
    return apply_trigger(writer, trigger).toTable(bronze_table)


# ---------------------------------------------------------------- Silver
def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    """Parse JSON with the explicit schema and attach a list of data-quality errors per row.
    Rows with an empty dq_errors list are good; the rest go to quarantine.
    """
    parsed = bronze_df.withColumn("o", F.from_json("raw_value", ORDER_SCHEMA))
    o = lambda name: F.col("o." + name)  # noqa: E731
    checks = F.array(
        F.when(F.col("o").isNull() | o("order_id").isNull() & o("event_id").isNull() & o("status").isNull(),
               F.lit("unparseable_json")),
        F.when(o("order_id").isNull(), F.lit("missing_order_id")),
        F.when(o("timestamp").isNull(), F.lit("missing_timestamp")),
        F.when(o("product").isNull(), F.lit("missing_product")),
        F.when(o("amount").isNull() | (o("amount") <= 0), F.lit("invalid_amount")),
        F.when(~F.coalesce(F.upper(F.trim(o("status"))).isin(VALID_STATUSES), F.lit(False)), F.lit("invalid_status")),
    )
    return parsed.select(
        # event_id from the producer is the dedup key. Older events without it fall back to a
        # hash of the fields that identify one state change of one order.
        F.coalesce(
            o("event_id"),
            F.sha2(F.concat_ws("|", o("order_id"), o("status"), o("timestamp")), 256),
        ).alias("event_key"),
        o("event_id").alias("event_id"),
        o("order_id").alias("order_id"),
        o("customer_id").alias("customer_id"),
        F.initcap(F.trim(o("product"))).alias("product"),
        o("amount").cast("decimal(12,2)").alias("amount"),  # money as decimal, never float
        F.upper(F.trim(o("status"))).alias("status"),
        F.timestamp_seconds(o("timestamp")).alias("event_ts"),
        F.to_date(F.timestamp_seconds(o("timestamp"))).alias("event_date"),
        "kafka_key", "raw_value", "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
        F.filter(checks, lambda c: c.isNotNull()).alias("dq_errors"),
    )


SILVER_COLUMNS = ["event_key", "event_id", "order_id", "customer_id", "product", "amount", "status",
                  "event_ts", "event_date", "topic", "partition", "offset", "kafka_timestamp", "ingested_at"]
QUARANTINE_COLUMNS = ["raw_value", "dq_errors", "kafka_key", "topic", "partition", "offset",
                      "kafka_timestamp", "ingested_at"]


def split_good_bad(bronze_df: DataFrame):
    checked = parse_and_validate(bronze_df)
    good = checked.filter(F.size("dq_errors") == 0)
    # Same event twice in one batch: keep the copy Kafka received first.
    w = Window.partitionBy("event_key").orderBy("kafka_timestamp", "partition", "offset")
    good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(*SILVER_COLUMNS)
    bad = checked.filter(F.size("dq_errors") > 0).select(*QUARANTINE_COLUMNS)
    return good, bad


def ensure_silver_tables(spark: SparkSession, silver_table: str, quarantine_table: str):
    """Create empty tables up front so the first MERGE has a target."""
    empty = parse_and_validate(spark.createDataFrame(
        [], "kafka_key string, raw_value string, topic string, partition int, offset long, "
            "kafka_timestamp timestamp, ingested_at timestamp"))
    if not spark.catalog.tableExists(silver_table):
        empty.select(*SILVER_COLUMNS).write.format("delta").saveAsTable(silver_table)
    if not spark.catalog.tableExists(quarantine_table):
        (empty.select(*QUARANTINE_COLUMNS).withColumn("batch_id", F.lit(None).cast("long"))
         .write.format("delta").saveAsTable(quarantine_table))


def make_silver_batch_writer(silver_table: str, quarantine_table: str):
    """Return the foreachBatch function. Both writes are insert-if-new MERGEs, so it is safe when
    Spark retries a batch, when Kafka is replayed, or when the producer sends the same event twice:
    - Silver is keyed by event_key (the business identity of the event).
    - Quarantine is keyed by Kafka topic/partition/offset (the physical identity of the record).
    """
    from delta.tables import DeltaTable

    def write_batch(batch_df: DataFrame, batch_id: int):
        spark = batch_df.sparkSession
        good, bad = split_good_bad(batch_df)

        (DeltaTable.forName(spark, silver_table).alias("t")
         .merge(good.alias("s"), "t.event_key = s.event_key")
         .whenNotMatchedInsertAll()
         .execute())

        bad = bad.dropDuplicates(["topic", "partition", "offset"]).withColumn("batch_id", F.lit(batch_id).cast("long"))
        (DeltaTable.forName(spark, quarantine_table).alias("t")
         .merge(bad.alias("s"), "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset")
         .whenNotMatchedInsertAll()
         .execute())

    return write_batch


def start_silver(spark: SparkSession, bronze_table: str, silver_table: str, quarantine_table: str,
                 checkpoint: str, trigger: str):
    ensure_silver_tables(spark, silver_table, quarantine_table)
    writer = (
        spark.readStream.table(bronze_table)  # Delta table as a streaming source: reads only new rows
        .writeStream
        .foreachBatch(make_silver_batch_writer(silver_table, quarantine_table))
        .option("checkpointLocation", checkpoint)
        .queryName("silver_orders")
    )
    return apply_trigger(writer, trigger).start()


# ---------------------------------------------------------------- Gold
def orders_current(silver_df: DataFrame) -> DataFrame:
    """One row per order with its furthest lifecycle status. Ranking by lifecycle first means a
    late-arriving PAID event can't move an order back from SHIPPED."""
    w = Window.partitionBy("order_id").orderBy(status_rank().desc(), F.col("event_ts").desc())
    first_seen = Window.partitionBy("order_id")
    return (
        silver_df
        .withColumn("created_ts", F.min("event_ts").over(first_seen))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("order_id", "customer_id", "product", "amount", "status",
                F.col("event_ts").alias("status_ts"), "created_ts")
    )


def revenue_by_product(current_df: DataFrame) -> DataFrame:
    paid = current_df.filter(F.col("status").isin("PAID", "SHIPPED"))
    return paid.groupBy("product").agg(
        F.count("*").alias("paid_orders"),
        F.sum("amount").alias("revenue"),
        F.round(F.avg("amount"), 2).cast("decimal(12,2)").alias("avg_order_value"),
    )


def orders_by_status(current_df: DataFrame) -> DataFrame:
    return current_df.groupBy("status").agg(F.count("*").alias("orders"))


def sales_by_hour(silver_df: DataFrame) -> DataFrame:
    """Revenue counted at the moment of payment (the PAID event), bucketed per hour."""
    return (
        silver_df.filter(F.col("status") == "PAID")
        .groupBy(F.date_trunc("hour", "event_ts").alias("hour"))
        .agg(F.count("*").alias("paid_orders"), F.sum("amount").alias("revenue"))
    )


def build_gold(spark: SparkSession, silver_table: str, gold_prefix: str) -> dict:
    """Recompute the small Gold tables from Silver and overwrite them.
    Simple and always correct (late data included). Switch to incremental MERGE when Silver gets big.
    """
    silver = spark.read.table(silver_table)
    current = orders_current(silver)
    outputs = {
        f"{gold_prefix}orders_current": current,
        f"{gold_prefix}revenue_by_product": revenue_by_product(current),
        f"{gold_prefix}orders_by_status": orders_by_status(current),
        f"{gold_prefix}sales_by_hour": sales_by_hour(silver),
    }
    for table, df in outputs.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    return {table: spark.read.table(table).count() for table in outputs}

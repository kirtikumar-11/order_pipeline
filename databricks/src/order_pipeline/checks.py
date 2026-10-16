"""Pipeline health checks shared by the 04_validate notebook and the local runner."""

from pyspark.sql import SparkSession


def validate(spark: SparkSession, bronze: str, silver: str, quarantine: str, gold_prefix: str,
             max_bad_ratio: float = 0.10):
    q = lambda sql: spark.sql(sql).collect()[0][0]  # noqa: E731
    offsets = "count(DISTINCT topic, partition, offset)"

    bronze_rows = q(f"SELECT count(*) FROM {bronze}")
    bronze_offsets = q(f"SELECT {offsets} FROM {bronze}")
    silver_rows = q(f"SELECT count(*) FROM {silver}")
    silver_keys = q(f"SELECT count(DISTINCT event_key) FROM {silver}")
    silver_offsets = q(f"SELECT {offsets} FROM {silver}")
    quarantine_rows = q(f"SELECT count(*) FROM {quarantine}")
    quarantine_offsets = q(f"SELECT {offsets} FROM {quarantine}")
    gold_orders = q(f"SELECT count(*) FROM {gold_prefix}orders_current")
    silver_orders = q(f"SELECT count(DISTINCT order_id) FROM {silver}")

    report = {
        "bronze_rows": bronze_rows,
        # > 0 is allowed: a lost Bronze checkpoint replays Kafka. Silver absorbs it.
        "bronze_duplicate_offsets": bronze_rows - bronze_offsets,
        "silver_rows": silver_rows,
        "silver_duplicate_event_keys": silver_rows - silver_keys,
        "quarantine_rows": quarantine_rows,
        "quarantine_duplicate_offsets": quarantine_rows - quarantine_offsets,
        # Records that are neither in Silver nor quarantine = duplicate events removed by the MERGE.
        "duplicates_removed": bronze_offsets - silver_offsets - quarantine_offsets,
        "gold_orders": gold_orders,
        "silver_orders": silver_orders,
    }

    problems = []
    if report["silver_duplicate_event_keys"]:
        problems.append("Silver has duplicate event_keys (dedup is broken)")
    if report["quarantine_duplicate_offsets"]:
        problems.append("Quarantine has the same Kafka record twice (retry/replay was not idempotent)")
    if report["duplicates_removed"] < 0:
        problems.append("Silver + quarantine hold offsets that Bronze does not (tables out of sync)")
    if gold_orders != silver_orders:
        problems.append("Gold orders_current does not match distinct orders in Silver (rerun Gold)")
    if bronze_offsets and quarantine_offsets / bronze_offsets > max_bad_ratio:
        problems.append(f"{quarantine_offsets} of {bronze_offsets} records are bad (> {max_bad_ratio:.0%})")
    return report, problems

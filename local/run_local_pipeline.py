"""Run Bronze -> Silver -> Gold -> Validate on your laptop with open-source Spark + Delta,
using the same code the Databricks notebooks use. Handy for learning and for failure tests
without spending Databricks credits.

It reads the same .env as the producer, so it can talk to Aiven or to a local Kafka.
Needs Java 17+ and `pip install -r requirements-dev.txt`. Run from the repo root:

  python local/run_local_pipeline.py                         # all stages, once (AvailableNow)
  python local/run_local_pipeline.py --stage silver          # one stage
  python local/run_local_pipeline.py --drop-checkpoint bronze  # failure test: lose the Bronze checkpoint
"""

import argparse
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "databricks", "src"))

from dotenv import find_dotenv, load_dotenv  # noqa: E402

from order_pipeline.checks import validate  # noqa: E402
from order_pipeline.config import kafka_options  # noqa: E402
from order_pipeline.transforms import build_gold, start_bronze, start_silver  # noqa: E402

SPARK_VERSION = "4.0.1"
PACKAGES = [f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"]


def get_spark(lake_dir):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName("order-pipeline-local")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", os.path.join(lake_dir, "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={lake_dir}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Local Hive metastore keeps table names between runs. The flag below silences a harmless
        # Hive-vs-Delta schema warning when Gold overwrites tables.
        .config("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=PACKAGES).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold", "validate"], default="all")
    parser.add_argument("--lake-dir", default=os.path.join(ROOT, "local-lakehouse"))
    parser.add_argument("--drop-checkpoint", choices=["bronze", "silver"],
                        help="delete a checkpoint before running (failure test)")
    args = parser.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    ca_pem = None
    if protocol.endswith("SSL"):
        with open(os.getenv("KAFKA_CA_LOCATION", "certs/ca.pem")) as f:
            ca_pem = f.read()  # passed inline, exactly like the Databricks secret
    kafka_opts = kafka_options(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        username=os.environ["KAFKA_USERNAME"],
        password=os.environ["KAFKA_PASSWORD"],
        topic=os.getenv("KAFKA_TOPIC", "orders"),
        ca_pem=ca_pem,
        security_protocol=protocol,
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        shaded=False,  # open-source Spark uses the normal Kafka class names
    )

    lake = os.path.abspath(args.lake_dir)
    checkpoints = {name: os.path.join(lake, "checkpoints", name) for name in ("bronze", "silver")}
    if args.drop_checkpoint:
        shutil.rmtree(checkpoints[args.drop_checkpoint], ignore_errors=True)
        print(f"Deleted the {args.drop_checkpoint} checkpoint.")

    spark = get_spark(lake)
    bronze, silver, quarantine, gold = "bronze_orders", "silver_order_events", "silver_quarantine", "gold_"
    run = lambda stage: args.stage in ("all", stage)  # noqa: E731

    if run("bronze"):
        q = start_bronze(spark, kafka_opts, bronze, checkpoints["bronze"], "available_now")
        q.awaitTermination()
        print("Bronze rows:", spark.read.table(bronze).count())
    if run("silver"):
        q = start_silver(spark, bronze, silver, quarantine, checkpoints["silver"], "available_now")
        q.awaitTermination()
        print("Silver rows:", spark.read.table(silver).count(), "| quarantine:", spark.read.table(quarantine).count())
    if run("gold"):
        print("Gold:", build_gold(spark, silver, gold))
        spark.read.table(f"{gold}revenue_by_product").orderBy("product").show()
        spark.read.table(f"{gold}orders_by_status").show()
    if run("validate"):
        report, problems = validate(spark, bronze, silver, quarantine, gold)
        for k, v in report.items():
            print(f"  {k}: {v}")
        if problems:
            sys.exit("VALIDATION FAILED:\n- " + "\n- ".join(problems))
        print("All checks passed.")


if __name__ == "__main__":
    main()

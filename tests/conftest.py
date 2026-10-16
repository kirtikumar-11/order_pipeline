import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "databricks", "src"))
sys.path.insert(0, os.path.join(ROOT, "producer"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark with Delta Lake. Needs Java 17+ and `pip install -r requirements-dev.txt`."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("order-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()

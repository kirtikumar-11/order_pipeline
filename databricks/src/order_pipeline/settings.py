"""Reads notebook widgets + Databricks Secrets and turns them into table names, paths and Kafka options.
Only used inside Databricks (it needs dbutils)."""

from order_pipeline.config import kafka_options

WIDGET_DEFAULTS = {
    "catalog": "workspace",          # Free Edition's default catalog. Use "main" or your own elsewhere.
    "schema": "orders_pipeline",
    "secret_scope": "order-pipeline",
    "topic": "orders",
    "trigger": "available_now",      # or e.g. "30 seconds" on compute that allows always-on streams
    "ca_volume_path": "",            # optional: /Volumes/<catalog>/<schema>/config/ca.pem instead of a secret
    "starting_offsets": "earliest",
    "max_offsets_per_trigger": "100000",
}


def read_settings(dbutils) -> dict:
    for name, default in WIDGET_DEFAULTS.items():
        dbutils.widgets.text(name, default)  # job parameters with the same name override these
    s = {name: dbutils.widgets.get(name).strip() for name in WIDGET_DEFAULTS}
    base = f"{s['catalog']}.{s['schema']}"
    volume = f"/Volumes/{s['catalog']}/{s['schema']}/checkpoints"
    s.update(
        bronze_table=f"{base}.bronze_orders",
        silver_table=f"{base}.silver_order_events",
        quarantine_table=f"{base}.silver_quarantine",
        gold_prefix=f"{base}.gold_",
        bronze_checkpoint=f"{volume}/bronze_orders",
        silver_checkpoint=f"{volume}/silver_order_events",
    )
    return s


def kafka_options_from_secrets(dbutils, s: dict) -> dict:
    scope = s["secret_scope"]
    ca_pem, ca_path = None, None
    if s["ca_volume_path"]:
        ca_path = s["ca_volume_path"]
    else:
        ca_pem = dbutils.secrets.get(scope, "kafka-ca-pem")
    return kafka_options(
        bootstrap_servers=dbutils.secrets.get(scope, "kafka-bootstrap-servers"),
        username=dbutils.secrets.get(scope, "kafka-username"),
        password=dbutils.secrets.get(scope, "kafka-password"),
        topic=s["topic"],
        ca_pem=ca_pem,
        ca_path=ca_path,
        starting_offsets=s["starting_offsets"],
        max_offsets_per_trigger=int(s["max_offsets_per_trigger"]),
        shaded=True,
    )

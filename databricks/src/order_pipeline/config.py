"""Builds Spark Kafka source options. Secrets are passed in; nothing is hardcoded here."""

# Databricks ships a "shaded" (renamed) copy of the Kafka client, so the JAAS login class
# has a "kafkashaded." prefix there. Open-source Spark uses the normal class name.
SHADED_SCRAM = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
PLAIN_SCRAM = "org.apache.kafka.common.security.scram.ScramLoginModule"


def kafka_options(
    bootstrap_servers: str,
    username: str,
    password: str,
    topic: str = "orders",
    ca_pem: str = None,
    ca_path: str = None,
    security_protocol: str = "SASL_SSL",
    sasl_mechanism: str = "SCRAM-SHA-256",
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 10000,
    shaded: bool = True,
) -> dict:
    """Return the options dict for spark.readStream.format("kafka").options(**opts).

    TLS trust: pass the Aiven CA either as text (ca_pem, e.g. from a Databricks secret)
    or as a file path (ca_path, e.g. a Unity Catalog Volume path). Both use the PEM format,
    so no Java keystore conversion is needed.
    """
    login_module = SHADED_SCRAM if shaded else PLAIN_SCRAM
    opts = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": starting_offsets,  # only used the very first time (no checkpoint yet)
        "maxOffsetsPerTrigger": str(max_offsets_per_trigger),  # caps batch size, also with AvailableNow
        "failOnDataLoss": "true",  # fail loudly if Kafka deleted data we never read
        "kafka.security.protocol": security_protocol,
        "kafka.sasl.mechanism": sasl_mechanism,
        "kafka.sasl.jaas.config": f'{login_module} required username="{username}" password="{password}";',
    }
    if security_protocol.endswith("SSL"):
        if not (ca_pem or ca_path):
            raise ValueError("SASL_SSL needs the broker CA: pass ca_pem or ca_path")
        opts["kafka.ssl.truststore.type"] = "PEM"
        if ca_pem:
            opts["kafka.ssl.truststore.certificates"] = ca_pem
        else:
            opts["kafka.ssl.truststore.location"] = ca_path
    return opts


def apply_trigger(writer, trigger: str):
    """'available_now' -> process everything available, then stop (works on serverless).
    Anything else, e.g. '30 seconds' -> run forever with a micro-batch every 30 s (needs classic compute).
    """
    if trigger.strip().lower() in ("available_now", "availablenow", ""):
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=trigger)

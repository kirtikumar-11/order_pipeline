# Real-Time Order Processing Pipeline

A streaming data engineering project that simulates e-commerce order
events, ingests them through Apache Kafka, processes them with Spark
Structured Streaming, and organizes the data into Bronze, Silver, and
Gold layers using Delta Lake.

The project focuses on reliable event processing, data quality,
duplicate handling, malformed-message quarantine, and incremental
processing.

## Architecture

``` text
Python Order Producer
        |
        v
Apache Kafka (`orders` topic)
        |
        v
Spark Structured Streaming
        |
        v
Bronze Layer — Raw Events + Kafka Metadata
        |
        v
Silver Layer — Parsed, Validated, Deduplicated Records
        |                         |
        |                         +--> Quarantine / Invalid Records
        v
Gold Layer — Business Aggregations
        |
        v
Data Quality Checks
```

### Processing layers

-   **Producer:** Generates realistic order lifecycle events and
    publishes JSON messages to Kafka.
-   **Kafka:** Acts as the event-streaming layer between event
    generation and processing.
-   **Bronze:** Persists raw event data and relevant Kafka metadata for
    traceability and replay.
-   **Silver:** Parses event payloads, validates records, removes
    duplicate events, and separates invalid records for investigation.
-   **Gold:** Builds business-oriented tables for order analysis,
    revenue, order status, and hourly sales.
-   **Data quality checks:** Validate processing outcomes, including
    record consistency, duplicate handling, and invalid-record
    thresholds.

## Technology Stack

-   **Python** --- event generation and test utilities
-   **Apache Kafka** --- event streaming
-   **Aiven for Apache Kafka** --- hosted Kafka service configuration
-   **Apache Spark Structured Streaming** --- stream processing
-   **Delta Lake** --- reliable storage for layered data
-   **Databricks** --- notebooks and deployment configuration for cloud
    execution
-   **pytest** --- automated tests

A local execution path is also provided to support development and
testing without requiring every stage to run in a Databricks workspace.

## Key Features

-   Simulates an e-commerce order lifecycle: `CREATED → PAID → SHIPPED`
-   Generates unique `event_id` values for event-level duplicate
    detection
-   Supports configurable event volume and production rate
-   Injects duplicate events to test idempotent downstream processing
-   Generates malformed messages, including invalid JSON, missing
    fields, invalid statuses, and incorrect values
-   Simulates late-arriving events by shifting event timestamps
-   Separates invalid records for quarantine and investigation
-   Uses Bronze, Silver, and Gold data layers to separate raw ingestion
    from cleaned data and business aggregates
-   Includes automated tests and data quality checks
-   Provides local execution scripts and Databricks deployment
    configuration

## Repository Structure

``` text
.
├── producer/
│   └── producer.py
├── databricks/
│   ├── src/
│   │   ├── order_pipeline/
│   │   └── notebooks/
│   ├── resources/
│   └── databricks.yml
├── local/
│   └── run_local_pipeline.py
├── tests/
├── docs/
└── README.md
```

The exact files may evolve as the project changes. Refer to the
repository for the current structure and implementation.

## Getting Started

### Prerequisites

-   Python installed
-   Access to a Kafka broker, such as an Aiven Kafka service, or a local
    Kafka environment
-   Required Python dependencies installed
-   For the cloud workflow, access to a Databricks workspace with the
    required permissions

### Configure Kafka

Create a `.env` file for the producer using the variable names expected
by the project. For example:

``` dotenv
KAFKA_BOOTSTRAP_SERVERS=your-kafka-bootstrap-server
KAFKA_USERNAME=your-kafka-username
KAFKA_PASSWORD=your-kafka-password
KAFKA_TOPIC=orders
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_CA_LOCATION=certs/ca.pem
```

Use the actual values supplied by your Kafka provider. Do not commit
`.env` files, credentials, private keys, or certificates that should
remain private. Check `.gitignore` before pushing changes.

### Install dependencies

Install the dependencies listed by the repository's dependency file, if
provided. For example, if the project uses `requirements.txt`:

``` bash
pip install -r requirements.txt
```

### Generate order events

Run the producer from the project root:

``` bash
python producer/producer.py --count 200
```

Generate events at a target rate and enable failure scenarios:

``` bash
python producer/producer.py \
  --count 500 \
  --rate 20 \
  --dup-ratio 0.10 \
  --bad-ratio 0.05 \
  --late-ratio 0.05 \
  --seed 42
```

Producer arguments:

  -----------------------------------------------------------------------
  Argument                            Purpose
  ----------------------------------- -----------------------------------
  `--count`                           Number of messages to submit; `0`
                                      runs until interrupted

  `--rate`                            Target message submission rate in
                                      events per second

  `--dup-ratio`                       Approximate probability of
                                      replaying the previous valid
                                      message

  `--bad-ratio`                       Approximate probability of
                                      generating a malformed message

  `--late-ratio`                      Approximate probability of shifting
                                      a generated event timestamp two
                                      hours into the past

  `--seed`                            Optional seed for reproducible
                                      pseudorandom choices
  -----------------------------------------------------------------------

The configured topic defaults to `orders` if `KAFKA_TOPIC` is not set.

### Run the local pipeline

Use the local runner included in the repository:

``` bash
python local/run_local_pipeline.py
```

Check the script and project documentation for required environment
variables, Spark settings, and storage paths before running it.

## Validation and Reliability

The project includes automated tests and local end-to-end validation for
core streaming scenarios.

-   **Unit tests:** The repository reports 10 tests covering producer
    and transformation behavior.
-   **End-to-end processing:** A documented local run processed 300
    events through the pipeline.
-   **Duplicate handling:** The documented run removed 24 duplicate
    events from the Silver output.
-   **Invalid-record handling:** The documented run quarantined 15
    malformed or invalid records.
-   **Incremental processing:** Tests cover processing new events across
    subsequent runs without duplicating previously processed Silver
    records.
-   **Replay and recovery:** The project includes a checkpoint-replay
    scenario for validating reprocessing behavior.
-   **Data quality:** Checks cover record consistency, duplicate counts,
    and invalid-record thresholds.

These figures describe the documented test run, not a performance
benchmark or a guarantee that every run will produce identical counts.
Randomized input and runtime conditions can change the results.

## Databricks Deployment

The repository includes Databricks notebooks and deployment
configuration for running the pipeline in a Databricks workspace.

The cloud workflow requires appropriate workspace permissions, compute
configuration, secrets, storage configuration, and Kafka connectivity.
Follow the deployment instructions and validate the job in the target
workspace before enabling scheduled execution.

## Failure Scenarios

The producer can generate test events for:

-   Duplicate messages
-   Truncated or invalid JSON
-   Missing required fields
-   Negative order amounts
-   Invalid order statuses
-   Incorrect field types
-   Late event timestamps

These scenarios help evaluate the downstream pipeline's validation,
deduplication, quarantine, and recovery behavior.

## Security Notes

-   Keep Kafka credentials in environment variables or a secrets
    manager.
-   Do not commit passwords, private keys, or sensitive certificates.
-   Use TLS and the authentication mechanism required by the Kafka
    provider.
-   Grant the pipeline only the permissions it needs.
-   Store production secrets in the platform's supported
    secret-management facility.

## Future Improvements

Potential extensions include:

-   Monitoring and alerting for lag, processing failures, and
    data-quality violations
-   More extensive recovery tests for checkpoint loss and replay
-   Performance testing with larger event volumes
-   Automated integration tests against an isolated Kafka environment
-   Operational dashboards for throughput, rejected records, and
    processing latency

## Project Purpose

This project demonstrates core data engineering practices for
event-driven pipelines: Kafka-based ingestion, Spark Structured
Streaming, layered Delta Lake processing, data quality enforcement,
duplicate handling, malformed-record quarantine, and repeatable testing.

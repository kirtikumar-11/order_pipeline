import json
from datetime import datetime, timezone

from order_pipeline.transforms import (
    build_gold,
    ensure_silver_tables,
    make_silver_batch_writer,
    orders_current,
    revenue_by_product,
    split_good_bad,
)

T0 = 1790664086.0  # some fixed epoch second


def event(event_id, order_id, status, amount=100.0, ts=T0, product="Keyboard", customer_id=7):
    return json.dumps({"event_id": event_id, "order_id": order_id, "customer_id": customer_id,
                       "product": product, "amount": amount, "status": status, "timestamp": ts})


def bronze(spark, values):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [("7", v, "orders", 0, i, now, now) for i, v in enumerate(values)]
    return spark.createDataFrame(
        rows, "kafka_key string, raw_value string, topic string, partition int, offset long, "
              "kafka_timestamp timestamp, ingested_at timestamp")


def test_good_rows_are_parsed_and_typed(spark):
    good, bad = split_good_bad(bronze(spark, [event("e1", 1, "paid ", amount=84508.364, product=" keyboard")]))
    row = good.collect()[0]
    assert bad.count() == 0
    assert row.status == "PAID" and row.product == "Keyboard"
    assert str(row.amount) == "84508.36"  # decimal(12,2)
    assert row.event_ts.year == 2026


def test_malformed_rows_go_to_quarantine_with_reasons(spark):
    values = [
        '{"order_id": 12, "amount": 10.0, ',                       # truncated JSON
        json.dumps({"event_id": "x", "customer_id": 1, "product": "Mouse", "amount": 5.0,
                    "status": "PAID", "timestamp": T0}),            # no order_id
        event("e2", 2, "PAID", amount=-50.0),
        event("e3", 3, "TELEPORTED"),
        json.dumps({"event_id": "e4", "order_id": 4, "product": "Mouse", "amount": "nine hundred",
                    "status": "PAID", "timestamp": T0}),            # wrong type
    ]
    good, bad = split_good_bad(bronze(spark, values))
    assert good.count() == 0
    reasons = {r.raw_value: r.dq_errors for r in bad.collect()}
    assert "unparseable_json" in reasons[values[0]]
    assert reasons[values[1]] == ["missing_order_id"]
    assert reasons[values[2]] == ["invalid_amount"]
    assert reasons[values[3]] == ["invalid_status"]
    assert "invalid_amount" in reasons[values[4]]


def test_duplicates_in_one_batch_are_removed(spark):
    good, _ = split_good_bad(bronze(spark, [event("e1", 1, "PAID")] * 3 + [event("e2", 1, "SHIPPED")]))
    assert sorted(r.event_key for r in good.collect()) == ["e1", "e2"]


def test_silver_writer_is_idempotent_across_batches_and_retries(spark):
    silver, quarantine = "silver_idem", "quarantine_idem"
    ensure_silver_tables(spark, silver, quarantine)
    write = make_silver_batch_writer(silver, quarantine)
    batch = bronze(spark, [event("e1", 1, "CREATED"), event("e2", 1, "PAID"), "not json"])

    write(batch, 0)
    write(batch, 0)  # Spark retried the same batch after a failure
    write(bronze(spark, [event("e1", 1, "CREATED"), event("e3", 2, "CREATED")]), 1)  # replayed e1
    write(batch, 2)  # whole batch replayed from Kafka after a lost checkpoint

    assert spark.read.table(silver).count() == 3
    assert spark.read.table(quarantine).count() == 1  # not doubled by the retry


def test_current_status_ignores_late_lower_status(spark):
    values = [
        event("a", 1, "CREATED", ts=T0),
        event("b", 1, "SHIPPED", ts=T0 + 60),
        event("c", 1, "PAID", ts=T0 + 120),  # arrives late with a later clock, must not win
        event("d", 2, "CREATED", ts=T0, product="Mouse"),
    ]
    good, _ = split_good_bad(bronze(spark, values))
    current = {r.order_id: r.status for r in orders_current(good).collect()}
    assert current == {1: "SHIPPED", 2: "CREATED"}


def test_revenue_counts_each_paid_order_once(spark):
    values = [
        event("a", 1, "CREATED", amount=100.0),
        event("b", 1, "PAID", amount=100.0),
        event("c", 1, "SHIPPED", amount=100.0),
        event("d", 2, "CREATED", amount=50.0),  # never paid: no revenue
    ]
    good, _ = split_good_bad(bronze(spark, values))
    rows = revenue_by_product(orders_current(good)).collect()
    assert len(rows) == 1 and rows[0].paid_orders == 1 and float(rows[0].revenue) == 100.0


def test_build_gold_writes_all_tables(spark):
    silver, quarantine = "silver_gold", "quarantine_gold"
    ensure_silver_tables(spark, silver, quarantine)
    make_silver_batch_writer(silver, quarantine)(
        bronze(spark, [event("a", 1, "CREATED"), event("b", 1, "PAID"), event("c", 2, "CREATED")]), 0)
    counts = build_gold(spark, silver, "gold_")
    assert counts == {"gold_orders_current": 2, "gold_revenue_by_product": 1,
                      "gold_orders_by_status": 2, "gold_sales_by_hour": 1}

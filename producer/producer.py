"""Order event producer: simulates an e-commerce order lifecycle and sends JSON events to Kafka.

Each order goes CREATED -> PAID -> SHIPPED (some orders stop early, like real life).
Every event carries a unique event_id so the pipeline can remove duplicates later.

Failure-test switches (used in docs/failure_tests.md):
  --dup-ratio 0.1   re-send 10% of events with the same event_id (simulates a producer retry/replay)
  --bad-ratio 0.05  send 5% malformed messages (broken JSON, missing fields, negative amount)
  --late-ratio 0.05 send 5% of events with an event timestamp 2 hours in the past (late data)

Examples:
  python producer/producer.py --count 200
  python producer/producer.py --count 500 --rate 20 --dup-ratio 0.1 --bad-ratio 0.05
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Producer
from dotenv import find_dotenv, load_dotenv

PRODUCTS = {
    "Keyboard": (1500, 9000),
    "Mouse": (400, 3500),
    "Monitor": (8000, 45000),
    "Laptop": (35000, 180000),
    "Headphones": (1000, 25000),
    "Webcam": (1500, 12000),
}
NEXT_STATUS = {"CREATED": "PAID", "PAID": "SHIPPED"}


def build_config() -> dict:
    """Read Kafka settings from the environment (.env). Fails fast if something is missing."""
    load_dotenv(find_dotenv(usecwd=True))  # .env in the folder you run from (or a parent)
    required = ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        sys.exit(f"Missing settings in .env: {', '.join(missing)} (see .env.example)")

    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": protocol,
        "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        # Reliability: wait for all in-sync replicas, and let the broker drop producer retries.
        "acks": "all",
        "enable.idempotence": True,
        # Throughput: wait up to 20 ms to batch messages together, then compress the batch.
        "linger.ms": 20,
        "compression.type": "lz4",
        "client.id": "order-producer",
        "broker.address.family": os.getenv("KAFKA_ADDRESS_FAMILY", "any"),
    }
    if protocol.endswith("SSL"):
        ca = os.getenv("KAFKA_CA_LOCATION", "certs/ca.pem")
        if not os.path.exists(ca):
            sys.exit(f"CA certificate not found at {ca}. Download it from the Aiven console.")
        config["ssl.ca.location"] = ca
    return config


class OrderSimulator:
    """Keeps a pool of open orders so later events (PAID, SHIPPED) refer to real earlier orders."""

    def __init__(self, seed=None, start_order_id=None):
        self.rng = random.Random(seed)
        self.next_order_id = start_order_id or int(time.time() * 1000) % 10_000_000_000
        self.open_orders = {}  # order_id -> (customer_id, product, amount, status)

    def next_event(self, now=None) -> dict:
        now = time.time() if now is None else now
        advance = self.open_orders and self.rng.random() < 0.55
        if advance:
            order_id = self.rng.choice(list(self.open_orders))
            customer_id, product, amount, status = self.open_orders[order_id]
            status = NEXT_STATUS[status]
            if status in NEXT_STATUS and self.rng.random() > 0.1:
                self.open_orders[order_id] = (customer_id, product, amount, status)
            else:
                del self.open_orders[order_id]  # shipped, or abandoned after payment
        else:
            order_id = self.next_order_id
            self.next_order_id += 1
            customer_id = self.rng.randint(100, 999)
            product = self.rng.choice(list(PRODUCTS))
            low, high = PRODUCTS[product]
            amount = round(self.rng.uniform(low, high), 2)
            status = "CREATED"
            if self.rng.random() > 0.15:  # 15% of carts are abandoned and never paid
                self.open_orders[order_id] = (customer_id, product, amount, status)

        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "order_id": order_id,
            "customer_id": customer_id,
            "product": product,
            "amount": amount,
            "status": status,
            "timestamp": round(now, 3),
        }


def make_bad_message(rng: random.Random) -> bytes:
    """Return one of several realistic kinds of broken payload."""
    kind = rng.choice(["not_json", "missing_order_id", "negative_amount", "bad_status", "wrong_type"])
    base = {"event_id": str(uuid.uuid4()), "order_id": rng.randint(1, 10**6), "customer_id": 1,
            "product": "Mouse", "amount": 999.0, "status": "PAID", "timestamp": time.time()}
    if kind == "not_json":
        return b'{"order_id": 12, "amount": 10.0, '  # truncated JSON
    if kind == "missing_order_id":
        base.pop("order_id")
    elif kind == "negative_amount":
        base["amount"] = -50.0
    elif kind == "bad_status":
        base["status"] = "TELEPORTED"
    elif kind == "wrong_type":
        base["amount"] = "nine hundred"
    return json.dumps(base).encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=100, help="events to send (0 = run until Ctrl+C)")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--dup-ratio", type=float, default=0.0)
    parser.add_argument("--bad-ratio", type=float, default=0.0)
    parser.add_argument("--late-ratio", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=None, help="fix the random seed for repeatable runs")
    args = parser.parse_args()

    topic = os.getenv("KAFKA_TOPIC", "orders")
    producer = Producer(build_config())
    sim = OrderSimulator(seed=args.seed)
    rng = random.Random(args.seed)
    stats = {"sent": 0, "delivered": 0, "failed": 0, "dups": 0, "bad": 0, "late": 0}

    def on_delivery(err, msg):
        # Called from producer.poll()/flush() once the broker answers (or retries give up).
        if err is not None:
            stats["failed"] += 1
            print(f"DELIVERY FAILED key={msg.key()}: {err}", file=sys.stderr)
        else:
            stats["delivered"] += 1

    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))

    last_good = None
    started = time.perf_counter()
    while not stop["now"] and (args.count == 0 or stats["sent"] < args.count):
        roll = rng.random()
        if roll < args.bad_ratio:
            key, value = b"bad", make_bad_message(rng)
            stats["bad"] += 1
        elif roll < args.bad_ratio + args.dup_ratio and last_good is not None:
            key, value = last_good  # exact same bytes, same event_id
            stats["dups"] += 1
        else:
            event = sim.next_event()
            if rng.random() < args.late_ratio:
                event["timestamp"] = round(event["timestamp"] - 7200, 3)
                stats["late"] += 1
            # Key = customer_id: all events of one customer land on the same partition, in order.
            key, value = str(event["customer_id"]).encode(), json.dumps(event).encode()
            last_good = (key, value)

        while True:
            try:
                producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: let it drain, then try again
        stats["sent"] += 1
        producer.poll(0)  # serve delivery callbacks without blocking
        if args.rate > 0:
            time.sleep(1.0 / args.rate)

    remaining = producer.flush(60)
    elapsed = time.perf_counter() - started
    print(f"Done. {stats} still_in_queue={remaining}")
    print(f"Elapsed {elapsed:.1f} s | delivered {stats['delivered'] / elapsed:,.0f} events/s")


if __name__ == "__main__":
    main()

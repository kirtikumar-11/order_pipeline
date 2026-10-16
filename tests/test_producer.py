import json
import random

from producer import NEXT_STATUS, OrderSimulator, make_bad_message


def test_lifecycle_only_moves_forward():
    sim = OrderSimulator(seed=1, start_order_id=1000)
    last = {}
    order = {"CREATED": 1, "PAID": 2, "SHIPPED": 3}
    for _ in range(2000):
        e = sim.next_event(now=0)
        if e["order_id"] in last:
            assert order[e["status"]] == order[last[e["order_id"]]] + 1
        else:
            assert e["status"] == "CREATED"
        last[e["order_id"]] = e["status"]
    assert set(last.values()) == set(order)


def test_events_have_unique_ids_and_required_fields():
    sim = OrderSimulator(seed=2)
    events = [sim.next_event() for _ in range(500)]
    assert len({e["event_id"] for e in events}) == 500
    for e in events:
        assert set(e) == {"event_id", "order_id", "customer_id", "product", "amount", "status", "timestamp"}
        assert e["amount"] > 0 and e["status"] in ("CREATED", *NEXT_STATUS.values())


def test_bad_messages_are_actually_bad():
    rng = random.Random(3)
    for _ in range(50):
        raw = make_bad_message(rng)
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        broken = ("order_id" not in data or data["amount"] == -50.0
                  or data["status"] == "TELEPORTED" or isinstance(data["amount"], str))
        assert broken

"""Throughput summary for a finished streaming query (all micro-batches, not just the last one)."""


def _field(progress, name):
    # Classic Spark returns plain dicts; serverless (Spark Connect) returns progress objects
    # whose fields can be missing or None. Read either form and treat "no value" as 0.
    value = getattr(progress, name, None)
    if value is None and isinstance(progress, dict):
        value = progress.get(name)
    return value or 0


def throughput_summary(query) -> dict:
    batches = [p for p in query.recentProgress if _field(p, "numInputRows") > 0]
    rows = sum(_field(p, "numInputRows") for p in batches)
    # durationMs.triggerExecution = wall time of each micro-batch (read + transform + write)
    busy_ms = sum((_field(p, "durationMs") or {}).get("triggerExecution", 0) or 0 for p in batches)
    return {
        "batches": len(batches),
        "rows": rows,
        "processing_seconds": round(busy_ms / 1000, 1),
        "rows_per_second": round(rows / (busy_ms / 1000), 1) if busy_ms else 0.0,
        "largest_batch_rows": max((_field(p, "numInputRows") for p in batches), default=0),
    }
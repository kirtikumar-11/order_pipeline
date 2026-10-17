"""Throughput summary for a finished streaming query (all micro-batches)."""


def throughput_summary(query) -> dict:
    batches = [p for p in query.recentProgress if p.get("numInputRows", 0) > 0]
    rows = sum(p["numInputRows"] for p in batches)
    busy_ms = sum(p["durationMs"].get("triggerExecution", 0) for p in batches)
    return {
        "batches": len(batches),
        "rows": rows,
        "processing_seconds": round(busy_ms / 1000, 1),
        "rows_per_second": round(rows / (busy_ms / 1000), 1) if busy_ms else 0.0,
        "largest_batch_rows": max((p["numInputRows"] for p in batches), default=0),
    }
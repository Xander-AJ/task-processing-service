"""Shared Prometheus metric definitions for the API and the worker.

All metrics live on the prometheus_client default REGISTRY so both the API's
``/metrics`` endpoint and the worker's ``start_http_server`` expose them without
extra wiring.

The processing counters/histograms (``tasks_claimed_total``,
``tasks_processed_total``, ``task_processing_duration_seconds``,
``claim_duration_seconds``, ``claim_batch_size``) are fired inside
``worker_service`` and therefore increment wherever processing happens: both the
standalone worker loop (``app.worker``) and the synchronous ``POST
/workers/process`` API endpoint. Each process keeps its own counter values;
Prometheus sums them across scrape targets.

The HTTP metrics are recorded by the API middleware only.
"""

import logging
from collections.abc import Iterator

from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import text

from app.db import SessionLocal
from app.models import TaskStatus

log = logging.getLogger("tasks.metrics")

# --- HTTP request metrics (recorded by the API middleware) -------------------

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled, by method, matched route template and status.",
    ["method", "route", "status"],
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds, by method and matched route template.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

# --- Task processing metrics (fired from worker_service) ---------------------

tasks_claimed_total = Counter(
    "tasks_claimed_total",
    "Total tasks claimed (locked to processing) across all claim batches.",
)
tasks_processed_total = Counter(
    "tasks_processed_total",
    "Total tasks processed, by terminal outcome.",
    ["outcome"],  # one of: completed, failed, retried
)
tasks_released_total = Counter(
    "tasks_released_total",
    "Tasks returned to pending without completing "
    "(e.g. released on graceful shutdown).",
)
task_processing_duration_seconds = Histogram(
    "task_processing_duration_seconds",
    "Wall-clock time spent processing a single task, in seconds.",
    buckets=(0.1, 0.25, 0.5, 0.75, 1, 2, 5),
)
claim_duration_seconds = Histogram(
    "claim_duration_seconds",
    "Time spent running the claim SELECT, in seconds.",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)
claim_batch_size = Histogram(
    "claim_batch_size",
    "Number of tasks returned by a single claim batch.",
    buckets=(1, 2, 5, 10, 20),
)


# --- Queue-depth gauges (custom collector, scraped on demand) -----------------


class QueueDepthCollector(Collector):
    """Reports live queue depth by querying the database at scrape time.

    Implemented as a custom collector (rather than gauges the app must keep in
    sync) so the numbers are always the current DB truth. ``collect()`` opens a
    short-lived session and is fully guarded: any DB error is logged and yields
    nothing, so a failed scrape degrades to missing series instead of a 500.
    """

    def collect(self) -> Iterator[GaugeMetricFamily]:
        """Yield queue-depth gauges, or nothing if the DB is unreachable."""
        try:
            db = SessionLocal()
            try:
                # Seed every status at 0 so all four series always exist even
                # when a status currently has no rows.
                counts: dict[str, float] = {s.value: 0.0 for s in TaskStatus}
                rows = db.execute(
                    text("SELECT status, count(*) FROM tasks GROUP BY status")
                ).all()
                for status, count in rows:
                    key = status.value if isinstance(status, TaskStatus) else str(status)
                    counts[key] = float(count)

                oldest = db.execute(
                    text(
                        "SELECT EXTRACT(EPOCH FROM (now() - min(created_at))) "
                        "FROM tasks WHERE status = 'pending' AND run_after <= now()"
                    )
                ).scalar()
            finally:
                db.close()
        except Exception:
            log.warning("queue_depth_scrape_failed", exc_info=True)
            return

        by_status = GaugeMetricFamily(
            "tasks_by_status",
            "Current number of tasks in each status.",
            labels=["status"],
        )
        for status_value, count in counts.items():
            by_status.add_metric([status_value], count)
        yield by_status

        age = GaugeMetricFamily(
            "oldest_eligible_pending_age_seconds",
            "Age in seconds of the oldest currently-eligible pending task (0 if none).",
        )
        age.add_metric([], float(oldest) if oldest is not None else 0.0)
        yield age


# Registered from API startup only (never at import time). Guarded so repeated
# startups (e.g. one TestClient per test) don't double-register on the global
# default REGISTRY.
_queue_depth_collector: QueueDepthCollector | None = None


def register_queue_depth_collector() -> None:
    """Register the queue-depth collector on the default REGISTRY, once."""
    global _queue_depth_collector
    if _queue_depth_collector is not None:
        return
    from prometheus_client import REGISTRY

    _queue_depth_collector = QueueDepthCollector()
    REGISTRY.register(_queue_depth_collector)

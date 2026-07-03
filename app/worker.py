import logging
import signal
import threading
from types import FrameType

import prometheus_client

from app.config import settings
from app.db import SessionLocal
from app.log import setup_logging
from app.services.worker_service import process_available_tasks

log = logging.getLogger("tasks.worker")

# Set by the signal handlers; checked by the loop and passed into each pass so a
# shutdown releases un-started tasks instead of stranding them as processing.
_stop = threading.Event()


def _handle_stop(signum: int, frame: FrameType | None) -> None:
    """SIGTERM/SIGINT handler: request a graceful stop."""
    _stop.set()


def main() -> None:
    """Run the claim/process loop until a stop signal, then exit cleanly."""
    setup_logging()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    # The worker's own metrics endpoint. start_http_server spawns a daemon
    # thread, so it needs no draining on shutdown.
    prometheus_client.start_http_server(settings.worker_metrics_port)

    log.info("worker_started", extra={"worker_id": settings.worker_id})
    while not _stop.is_set():
        db = SessionLocal()
        try:
            process_available_tasks(db, stop=_stop)
        finally:
            db.close()
        # wait() (not sleep) so a signal wakes us immediately instead of after a
        # full poll interval.
        _stop.wait(settings.poll_interval_seconds)

    log.info("worker_stopped", extra={"worker_id": settings.worker_id})


if __name__ == "__main__":
    main()

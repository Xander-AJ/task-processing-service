import logging
import time

from app.config import settings
from app.db import SessionLocal
from app.log import setup_logging
from app.services.worker_service import process_available_tasks

log = logging.getLogger("tasks.worker")


def main():
    setup_logging()
    log.info("worker_started", extra={"worker_id": settings.worker_id})
    try:
        while True:
            db = SessionLocal()
            try:
                process_available_tasks(db)
            finally:
                db.close()
            time.sleep(settings.poll_interval_seconds)
    except KeyboardInterrupt:
        log.info("worker_stopped", extra={"worker_id": settings.worker_id})


if __name__ == "__main__":
    main()

import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import prometheus_client
from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api import tasks, workers
from app.db import get_db
from app.log import setup_logging
from app.metrics import (
    http_request_duration_seconds,
    http_requests_total,
    register_queue_depth_collector,
)

setup_logging()

# Routes whose traffic is deliberately kept out of the HTTP metrics: probes and
# the scrape endpoint itself would only add noise and self-referential load.
_EXCLUDED_ROUTES = {"/health", "/ready", "/metrics"}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Register the queue-depth collector once the app comes up."""
    register_queue_depth_collector()
    yield


app = FastAPI(title="Task Processing Service", lifespan=lifespan)
app.include_router(tasks.router)
app.include_router(workers.router)

# Prometheus scrape endpoint. Mounted as a sub-app so it bypasses the routers.
app.mount("/metrics", prometheus_client.make_asgi_app())


@app.middleware("http")
async def metrics_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Record request count and latency, labelled by the matched route template.

    Labelling by the template (``/companies/{company_id}/tasks``) instead of the
    concrete path keeps cardinality bounded. Unmatched requests (404s) and the
    excluded probe/scrape routes are not recorded.
    """
    start = time.perf_counter()
    response = await call_next(request)

    route = request.scope.get("route")
    if route is not None:
        template = route.path
        if template not in _EXCLUDED_ROUTES:
            elapsed = time.perf_counter() - start
            http_requests_total.labels(
                request.method, template, str(response.status_code)
            ).inc()
            http_request_duration_seconds.labels(request.method, template).observe(
                elapsed
            )
    return response


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe: the process is up. No dependencies are checked."""
    return {"status": "ok"}


def _readiness_check(db: Session) -> None:
    """Confirm the database is reachable; raises on failure."""
    db.execute(text("SELECT 1"))


@app.get("/ready")
def ready(db: Session = Depends(get_db)) -> Response:
    """Readiness probe: 200 only when the database answers, else 503."""
    try:
        _readiness_check(db)
    except Exception:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    return JSONResponse(status_code=200, content={"status": "ready"})

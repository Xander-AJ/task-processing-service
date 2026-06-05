from fastapi import FastAPI

from app.api import tasks, workers
from app.log import setup_logging

setup_logging()

app = FastAPI(title="Task Processing Service")
app.include_router(tasks.router)
app.include_router(workers.router)


@app.get("/health")
def health():
    return {"status": "ok"}

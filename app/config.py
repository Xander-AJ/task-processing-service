from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/tasks"
    test_database_url: str = (
        "postgresql+psycopg2://postgres:postgres@localhost:5432/tasks_test"
    )

    # Worker tuning. A task is retried up to max_retries times before it lands
    # in 'failed'. A lock is considered stale after lock_timeout_seconds (the
    # spec's 5 minutes) so a crashed worker doesn't hold a task forever.
    worker_id: str = "worker-1"
    poll_interval_seconds: float = 1.0
    max_retries: int = 3
    lock_timeout_seconds: int = 300

    # Full-jitter exponential backoff bounds for failed-task retries.
    retry_backoff_base_seconds: float = 2.0
    retry_backoff_factor: float = 2.0
    retry_backoff_cap_seconds: float = 300.0

    # Max tasks a single company contributes to one claim batch before fairness
    # ordering. Bounds the per-company LATERAL subquery's work in claim_tasks().
    per_company_claim_cap: int = 10

    # Port the worker exposes its own Prometheus metrics on (each replica is its
    # own scrape target).
    worker_metrics_port: int = 9100


settings = Settings()

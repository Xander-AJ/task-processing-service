import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import sessionmaker

from app import models  # noqa: F401  (register models on Base.metadata)
from app.config import settings
from app.db import get_db
from app.main import app

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _alembic_config() -> Config:
    """Alembic config pointed at the test database, resolved to absolute paths so
    it works regardless of the pytest working directory."""
    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", settings.test_database_url)
    return cfg


def _ensure_test_database():
    # Connect to the default 'postgres' db to create tasks_test if it's missing.
    url = settings.test_database_url
    db_name = url.rsplit("/", 1)[1]
    admin_url = url.rsplit("/", 1)[0] + "/postgres"
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": db_name}
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    admin.dispose()


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    _ensure_test_database()
    eng = create_engine(settings.test_database_url, pool_pre_ping=True)
    # Build the schema exactly as production does — `alembic upgrade head` — so the
    # tests exercise the migrated schema, not a model-derived one that can drift
    # from it. Wipe the schema first (dropping any leftover create_all state and
    # the alembic_version marker) so the run is deterministic across repeats.
    with eng.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    command.upgrade(_alembic_config(), "head")
    yield eng
    eng.dispose()


@pytest.fixture(scope="session")
def TestSession(engine):
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def clean_tables(engine):
    # Truncate (not rollback) so committed rows are visible across the separate
    # connections the concurrency test uses.
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE tasks"))
    yield
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE tasks"))


@pytest.fixture
def db(TestSession):
    session = TestSession()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(TestSession):
    def override_get_db():
        session = TestSession()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def company_id():
    return str(uuid.uuid4())

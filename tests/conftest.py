import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.db import Base, get_db
from app.main import app
from app import models  # noqa: F401  (register models on Base.metadata)


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
def engine():
    _ensure_test_database()
    eng = create_engine(settings.test_database_url, pool_pre_ping=True)
    # create_all over alembic here: faster for tests, and the migration is
    # verified separately by `alembic check`.
    Base.metadata.create_all(eng)
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

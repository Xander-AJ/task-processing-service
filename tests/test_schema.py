from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, text

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _head_revision() -> str | None:
    """Resolve the current head revision from the migration scripts, so this
    tracks head automatically as new migrations are added (never hardcoded)."""
    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "alembic"))
    return ScriptDirectory.from_config(cfg).get_current_head()


def test_migrated_schema_has_partial_index(engine: Engine) -> None:
    """The partial index only exists if the migration ran. create_all would not
    reproduce its exact WHERE predicate, so finding it proves the test schema was
    built by `alembic upgrade head`, not model metadata.
    """
    with engine.connect() as conn:
        indexdef = conn.execute(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE indexname = 'ix_tasks_pending_run_after'"
            )
        ).scalar()

    assert indexdef is not None, "partial index missing — schema was not migrated"
    assert "WHERE (status = 'pending'::task_status)" in indexdef


def test_schema_built_by_migrations_not_create_all(engine: Engine) -> None:
    """alembic_version is stamped only by `alembic upgrade`, never by create_all.
    Asserting it equals the scripts' head proves the fixture migrated the schema
    (and that the DB is at the latest revision).
    """
    with engine.connect() as conn:
        version = conn.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar()

    head = _head_revision()
    assert head is not None, "no alembic head revision found — migration scripts missing"
    assert version == head

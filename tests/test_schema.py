from sqlalchemy import Engine, text


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

"""Alembic environment for the LegTracker `tracker` schema.

Scoped so that this Alembic only ever sees and manages the `tracker`
schema. The ingestion project's tables live in `public` under their own
Alembic; nothing here can create, alter or drop them.
"""
import os

from alembic import context
from sqlalchemy import create_engine, text

TRACKER_SCHEMA = "tracker"

# Set to the tracker models' Base.metadata once they exist, so that
# `alembic revision --autogenerate` works. Until then migrations are
# written by hand.
target_metadata = None


def include_object(obj, name, type_, reflected, compare_to):
    """Only objects in the tracker schema are visible to autogenerate."""
    if type_ == "table":
        return obj.schema == TRACKER_SCHEMA
    if hasattr(obj, "table") and obj.table is not None:
        return obj.table.schema == TRACKER_SCHEMA
    return True


def include_name(name, type_, parent_names):
    """Don't even reflect other schemas."""
    if type_ == "schema":
        return name == TRACKER_SCHEMA
    return True


def _url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("Set DATABASE_URL to the new database.")
    # Railway/Heroku-style URLs
    url = url.replace("postgres://", "postgresql://", 1)
    # A bare postgresql:// URL uses SQLAlchemy's default driver, which differs
    # between versions; pick whichever driver is actually installed.
    if url.startswith("postgresql://"):
        try:
            import psycopg  # noqa: F401  (psycopg 3)
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        except ImportError:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        version_table_schema=TRACKER_SCHEMA,
        include_schemas=True,
        include_object=include_object,
        include_name=include_name,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as connection:
        # The version table lives in tracker, so the schema must exist first.
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TRACKER_SCHEMA}"))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=TRACKER_SCHEMA,
            include_schemas=True,
            include_object=include_object,
            include_name=include_name,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

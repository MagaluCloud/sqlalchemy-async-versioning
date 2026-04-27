"""Advanced tests for sa_versioning.

Covers:
- __version_track__: only tracked columns trigger UPDATE version rows
- __version_exclude__: excluded columns absent from version table schema
- History accumulation and correct operation_type sequence
- changed_at populated as datetime
- Multi-model version table isolation
- setup_versioning idempotency (double-call does not duplicate rows)
"""

import pytest
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column

from conftest import Base
from sa_versioning import Versioned, setup_versioning


# ---------------------------------------------------------------------------
# Test models
# Defined at module level so __init_subclass__ registers them in
# _versioned_registry before the first setup_versioning() call in init_db.
# ---------------------------------------------------------------------------


class TrackedModel(Versioned, Base):
    """Only records a version row on UPDATE when `status` changes."""

    __tablename__ = "tracked_model"
    __version_track__ = ["status"]

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.String(100))
    status: Mapped[str] = mapped_column(sa.String(50))


class ExcludedModel(Versioned, Base):
    """The `secret` column must not appear in the version table schema."""

    __tablename__ = "excluded_model"
    __version_exclude__ = ["created_at", "updated_at", "secret"]

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.String(100))
    secret: Mapped[str] = mapped_column(sa.String(100), nullable=True)


class SecondModel(Versioned, Base):
    """Plain versioned model used for isolation and lifecycle tests."""

    __tablename__ = "second_model"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(sa.String(100))


# ---------------------------------------------------------------------------
# Phase 1 — __version_track__ behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_non_tracked_column_no_version(session):
    """UPDATE to a non-tracked column must not create a new version row."""
    obj = TrackedModel(name="initial", status="pending")
    session.add(obj)
    await session.commit()

    obj.name = "changed"
    await session.commit()

    VersionClass = TrackedModel.version_class
    result = await session.execute(
        sa.select(VersionClass).where(VersionClass.resource_id == obj.id)
    )
    versions = result.scalars().all()

    assert len(versions) == 1
    assert versions[0].operation_type == "INSERT"


@pytest.mark.asyncio
async def test_update_tracked_column_creates_version(session):
    """UPDATE to the tracked column must create a new version row."""
    obj = TrackedModel(name="initial", status="pending")
    session.add(obj)
    await session.commit()

    obj.status = "running"
    await session.commit()

    VersionClass = TrackedModel.version_class
    result = await session.execute(
        sa.select(VersionClass)
        .where(VersionClass.resource_id == obj.id)
        .order_by(VersionClass.changed_at)
    )
    versions = result.scalars().all()

    assert len(versions) == 2
    assert versions[1].operation_type == "UPDATE"
    assert versions[1].status == "running"


@pytest.mark.asyncio
async def test_version_track_none_any_column_triggers(session):
    """With __version_track__ = None (default), any column change creates a version row."""
    obj = SecondModel(value="initial")
    session.add(obj)
    await session.commit()

    obj.value = "changed"
    await session.commit()

    VersionClass = SecondModel.version_class
    result = await session.execute(
        sa.select(VersionClass)
        .where(VersionClass.resource_id == obj.id)
        .order_by(VersionClass.changed_at)
    )
    versions = result.scalars().all()

    assert len(versions) == 2
    assert versions[1].operation_type == "UPDATE"


# ---------------------------------------------------------------------------
# Phase 2 — __version_exclude__ behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_excluded_column_absent_from_version_schema(session):
    """Excluded column must not appear in the version table column definitions."""
    version_cols = {col.name for col in ExcludedModel.version_class.__table__.columns}
    assert "secret" not in version_cols


# ---------------------------------------------------------------------------
# Phase 3 — History accumulation and ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_updates_accumulate(session):
    """Five sequential tracked-column UPDATEs must produce five UPDATE rows."""
    obj = TrackedModel(name="vm", status="s0")
    session.add(obj)
    await session.commit()

    for i in range(1, 6):
        obj.status = f"s{i}"
        await session.commit()

    VersionClass = TrackedModel.version_class
    result = await session.execute(
        sa.select(VersionClass).where(VersionClass.resource_id == obj.id)
    )
    versions = result.scalars().all()

    assert len(versions) == 6  # 1 INSERT + 5 UPDATEs
    ops = [v.operation_type for v in versions]
    assert ops.count("INSERT") == 1
    assert ops.count("UPDATE") == 5


@pytest.mark.asyncio
async def test_full_lifecycle_operation_sequence(session):
    """INSERT → UPDATE → UPDATE → DELETE must produce rows in that exact order."""
    obj = SecondModel(value="v0")
    session.add(obj)
    await session.commit()

    obj.value = "v1"
    await session.commit()

    obj.value = "v2"
    await session.commit()

    obj_id = obj.id
    await session.delete(obj)
    await session.commit()

    VersionClass = SecondModel.version_class
    result = await session.execute(
        sa.select(VersionClass)
        .where(VersionClass.resource_id == obj_id)
        .order_by(VersionClass.changed_at)
    )
    versions = result.scalars().all()

    ops = [v.operation_type for v in versions]
    assert ops == ["INSERT", "UPDATE", "UPDATE", "DELETE"]


# ---------------------------------------------------------------------------
# Phase 4 — changed_at is a datetime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_changed_at_is_datetime(session):
    """changed_at on a version row must be a datetime instance."""
    obj = SecondModel(value="ts check")
    session.add(obj)
    await session.commit()

    VersionClass = SecondModel.version_class
    result = await session.execute(
        sa.select(VersionClass).where(VersionClass.resource_id == obj.id)
    )
    version = result.scalars().one()

    assert isinstance(version.changed_at, datetime)


# ---------------------------------------------------------------------------
# Phase 5 — Multi-model version table isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_models_version_isolation(session):
    """Version rows for different models must be stored in separate tables."""
    tracked = TrackedModel(name="t1", status="active")
    second = SecondModel(value="s1")
    session.add_all([tracked, second])
    await session.commit()

    tracked.status = "stopped"
    second.value = "s2"
    await session.commit()

    TVersion = TrackedModel.version_class
    SVersion = SecondModel.version_class

    assert TVersion.__tablename__ != SVersion.__tablename__

    t_result = await session.execute(
        sa.select(TVersion).where(TVersion.resource_id == tracked.id)
    )
    s_result = await session.execute(
        sa.select(SVersion).where(SVersion.resource_id == second.id)
    )

    assert len(t_result.scalars().all()) == 2
    assert len(s_result.scalars().all()) == 2


# ---------------------------------------------------------------------------
# Phase 6 — setup_versioning idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_setup_versioning_idempotent(session):
    """Calling setup_versioning a second time must not duplicate event listeners
    and therefore must not produce duplicate version rows."""
    setup_versioning(Base)  # already called by init_db fixture — must be a no-op

    obj = SecondModel(value="idempotent")
    session.add(obj)
    await session.commit()

    obj.value = "changed"
    await session.commit()

    VersionClass = SecondModel.version_class
    result = await session.execute(
        sa.select(VersionClass).where(VersionClass.resource_id == obj.id)
    )
    versions = result.scalars().all()

    # Exactly 2 rows — not 4, which would happen if listeners were registered twice
    assert len(versions) == 2

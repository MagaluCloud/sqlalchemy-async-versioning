"""sa_versioning
~~~~~~~~~~~~~~
Automatic versioning for SQLAlchemy 2.x models, compatible with AsyncSession.

Public API:
    Versioned       – mixin to add to models you want to version
    setup_versioning(base) – call once after all models are defined
"""

from __future__ import annotations

import inspect as _std_inspect
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, ClassVar

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, event
from sqlalchemy.orm import Session, attributes

__all__ = ["Versioned", "setup_versioning"]

# ---------------------------------------------------------------------------
# Internal registry
# ---------------------------------------------------------------------------

_versioned_registry: list[type] = []
_initialized: bool = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Public mixin
# ---------------------------------------------------------------------------


class Versioned:
    """Mixin that enables automatic versioning for a SQLAlchemy model.

    Class-level configuration (all optional):

    ``__version_track__``
        List of column names that *trigger* a new version record when any of
        them changes.  A record is always created on INSERT.
        If ``None`` (default) any column change triggers a new version.

    ``__version_exclude__``
        List of column names to omit from the version table snapshot.
        Defaults to ``["created_at", "updated_at"]``.

    After setup, ``Model.version_class`` holds the generated version class.
    """

    __version_track__: ClassVar[list[str] | None] = None
    __version_exclude__: ClassVar[list[str]] = ["created_at", "updated_at"]
    version_class: ClassVar[type]

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        # Only register concrete models that declare a table name directly.
        if "__tablename__" in cls.__dict__:
            _versioned_registry.append(cls)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup_versioning(base: type) -> None:
    """Create version classes and register the before_flush listener.

    Must be called **once** after all models are defined (and after
    ``configure_mappers()`` has run, which happens automatically here).

    Args:
        base: The ``DeclarativeBase`` subclass shared by your models.  The
              generated version classes are added to the same metadata so
              Alembic ``autogenerate`` detects them without any extra wiring.
    """
    global _initialized
    if _initialized:
        return

    from sqlalchemy.orm import configure_mappers

    configure_mappers()

    for cls in _versioned_registry:
        _build_version_class(cls, base)

    event.listen(Session, "before_flush", _handle_before_flush)
    event.listen(Session, "after_flush", _handle_after_flush)
    _initialized = True


# ---------------------------------------------------------------------------
# Version class factory
# ---------------------------------------------------------------------------


def _build_version_class(cls: type, base: type) -> type:
    """Dynamically create the ``<Model>Version`` class for *cls*.

    The version table contains:
    - ``id``          – UUID primary key (auto-generated)
    - ``resource_id`` – FK to the original table's PK (indexed, CASCADE delete)
    - ``changed_at``  – timestamp of the change (indexed)
    - ``operation_type`` – INSERT, UPDATE, DELETE (indexed)
    - one nullable column for every non-PK, non-excluded column in *cls*
    """
    mapper = sa.inspect(cls)
    pk_cols = {c.name: c for c in mapper.primary_key}
    if len(pk_cols) != 1:
        raise TypeError(
            f"sa_versioning: {cls.__name__} must have exactly one primary key "
            f"column (got {list(pk_cols)})."
        )

    pk_name, pk_col = next(iter(pk_cols.items()))
    exclude = set(getattr(cls, "__version_exclude__", ["created_at", "updated_at"]))

    attrs: dict[str, Any] = {
        "__tablename__": f"{cls.__tablename__[:54]}_version",
        "id": Column(sa.Uuid, primary_key=True, default=_uuid.uuid4),
        "resource_id": Column(
            pk_col.type.copy(),
            sa.ForeignKey(
                f"{cls.__tablename__}.{pk_name}", ondelete="CASCADE"
            ),
            nullable=False,
            index=True,
        ),
        "changed_at": Column(
            DateTime(timezone=True), nullable=False, default=_now, index=True
        ),
        "operation_type": Column(
            sa.String(10), nullable=False, index=True
        ),
    }

    for col in mapper.columns:
        if col.name in pk_cols or col.name in exclude:
            continue
        # Copy type; version columns are always nullable and have no defaults.
        attrs[col.name] = Column(col.type.copy(), nullable=True)

    version_cls = type(f"{cls.__name__}Version", (base,), attrs)
    cls.version_class = version_cls
    return version_cls


# ---------------------------------------------------------------------------
# Event listener
# ---------------------------------------------------------------------------


def _handle_before_flush(
    session: Session, flush_context: Any, instances: Any
) -> None:
    """``before_flush`` handler — tracks updates and deletes, and buffers inserts."""
    now = _now()

    # Buffer new instances for after_flush to capture DB-generated PKs and defaults
    new_objs = [obj for obj in session.new if isinstance(obj, Versioned)]
    if new_objs:
        session.info.setdefault("sa_versioning_new", []).extend(new_objs)

    for obj in list(session.dirty):
        if isinstance(obj, Versioned):
            _take_snapshot(session, obj, now, "UPDATE")

    for obj in list(session.deleted):
        if isinstance(obj, Versioned):
            _take_snapshot(session, obj, now, "DELETE")


def _handle_after_flush(
    session: Session, flush_context: Any
) -> None:
    """``after_flush`` handler — inserts version records for buffered inserts."""
    new_objs = session.info.pop("sa_versioning_new", [])
    if not new_objs:
        return

    now = _now()
    for obj in new_objs:
        _take_snapshot(session, obj, now, "INSERT")


def _take_snapshot(
    session: Session, obj: Any, now: datetime, operation_type: str
) -> None:
    cls = type(obj)
    mapper = sa.inspect(cls)
    pk_col = next(iter(mapper.primary_key))
    track: list[str] | None = getattr(cls, "__version_track__", None)
    state = attributes.instance_state(obj)

    if operation_type == "INSERT":
        should_snapshot = True
    elif operation_type == "DELETE":
        should_snapshot = True
    else:  # UPDATE
        if track:
            should_snapshot = any(
                attributes.get_history(obj, col_name).added
                for col_name in track
            )
        else:
            should_snapshot = any(
                attributes.get_history(obj, col.key).added
                for col in mapper.column_attrs
            )

    if not should_snapshot:
        return

    resource_id = getattr(obj, pk_col.name)
    version_cls = cls.version_class
    version_mapper = sa.inspect(version_cls)

    data: dict[str, Any] = {
        "resource_id": resource_id,
        "changed_at": now,
        "operation_type": operation_type,
    }

    for col in version_mapper.columns:
        if col.name in ("id", "resource_id", "changed_at", "operation_type"):
            continue
        
        # Prevent MissingGreenlet for lazy loaded / deferred columns
        if col.name in state.unloaded:
            val = None
        else:
            val = getattr(obj, col.name, None)
            
        data[col.name] = val

    session.add(version_cls(**data))

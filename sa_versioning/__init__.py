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

    event.listen(Session, "before_flush", _handle_flush)
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
        "__tablename__": f"{cls.__tablename__}_version",
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


def _handle_flush(
    session: Session, flush_context: Any, instances: Any
) -> None:
    """``before_flush`` handler — inserts version records for all changes.

    Fires on the underlying sync ``Session`` that ``AsyncSession`` wraps, so
    this works transparently with asyncpg.  Only ``session.add()`` is called
    here — no async I/O.
    """
    now = _now()

    for obj in list(session.new):
        if isinstance(obj, Versioned):
            _take_snapshot(session, obj, now, is_new=True)

    for obj in list(session.dirty):
        if isinstance(obj, Versioned):
            _take_snapshot(session, obj, now, is_new=False)


def _take_snapshot(
    session: Session, obj: Any, now: datetime, is_new: bool
) -> None:
    cls = type(obj)
    mapper = sa.inspect(cls)
    pk_col = next(iter(mapper.primary_key))
    track: list[str] | None = getattr(cls, "__version_track__", None)

    if is_new:
        # Column-level defaults (e.g. uuid4, VMStatus.creating) are applied by
        # SQLAlchemy only when building the INSERT statement, so the attributes
        # may still be None here.  Pre-apply Python-side defaults so the FK and
        # any snapshot columns have sensible values.
        _ensure_pk(obj, pk_col)
        should_snapshot = True
    else:
        if track:
            should_snapshot = any(
                attributes.get_history(obj, col_name).added
                for col_name in track
            )
        else:
            should_snapshot = any(
                attributes.get_history(obj, col.key).added
                for col in mapper.mapper.column_attrs
            )

    if not should_snapshot:
        return

    resource_id = getattr(obj, pk_col.name)
    version_cls = cls.version_class
    version_mapper = sa.inspect(version_cls)

    data: dict[str, Any] = {"resource_id": resource_id, "changed_at": now}

    for col in version_mapper.columns:
        if col.name in ("id", "resource_id", "changed_at"):
            continue
        val = getattr(obj, col.name, None)
        # For new objects fall back to the column's Python-side default so the
        # snapshot is consistent even before the INSERT fires.
        if val is None and is_new:
            original_col = mapper.columns.get(col.name)
            if original_col is not None:
                val = _python_default(original_col)
        data[col.name] = val

    session.add(version_cls(**data))


def _ensure_pk(obj: Any, pk_col: Any) -> None:
    """Pre-generate the PK if it hasn't been set yet."""
    if getattr(obj, pk_col.name) is None:
        val = _python_default(pk_col)
        if val is not None:
            setattr(obj, pk_col.name, val)


def _python_default(col: Any) -> Any:
    """Return the Python-side default value for *col*, or ``None``.

    Handles both scalar defaults and zero-argument callables (``uuid.uuid4``,
    enum members, etc.).  Ignores server-side defaults.
    """
    if col.default is None:
        return None
    arg = col.default.arg
    if callable(arg):
        try:
            return arg()
        except TypeError:
            try:
                return arg(None)  # some SA callables accept an ExecutionContext
            except Exception:
                return None
    # Scalar default (e.g. VMStatus.creating)
    return arg

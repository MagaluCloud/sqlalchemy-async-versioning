# sqlalchemy-async-versioning

Automatic versioning for SQLAlchemy 2.x models, fully compatible with
`AsyncSession` (asyncpg).

## How it works

A `before_flush` event listener fires on the underlying sync `Session` that
`AsyncSession` wraps internally.  The handler only calls `session.add()` — no
async I/O — so it is safe inside a sync event.

For every versioned model a version table is derived automatically from the
original model's columns via `inspect()`.  No need to define a separate history
model per entity.

## Usage

### 1. Mark models with the `Versioned` mixin

```python
from sa_versioning import Versioned

class VirtualMachine(Versioned, Base):
    __tablename__ = "virtual_machines"

    # Columns that *trigger* a new version record when changed.
    # A record is always created on INSERT regardless of this setting.
    # If None (default), any column change triggers a version.
    __version_track__ = ["status"]

    # Columns to omit from the version table snapshot.
    # Defaults to ["created_at", "updated_at"].
    __version_exclude__ = ["created_at", "updated_at", "deleted_at"]

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(...)
    ...
```

### 2. Call `setup_versioning` once after all models are defined

```python
# models/__init__.py
from sa_versioning import setup_versioning
from myapp.database import Base
from myapp.models.virtual_machine import VirtualMachine

setup_versioning(Base)
```

This:
- Creates a `virtual_machines_version` table with columns derived from the
  original model (non-excluded, non-PK columns become nullable snapshots).
- Attaches the `resource_id` FK back to `virtual_machines.id`.
- Registers the global `before_flush` listener.
- Exposes the generated class as `VirtualMachine.version_class`.

### 3. Generated version table schema

For `VirtualMachine` with `__version_track__ = ["status"]` and
`__version_exclude__ = ["created_at", "updated_at", "deleted_at"]`:

```sql
CREATE TABLE virtual_machines_version (
    id          UUID PRIMARY KEY,
    resource_id UUID NOT NULL REFERENCES virtual_machines(id) ON DELETE CASCADE,
    changed_at  TIMESTAMPTZ NOT NULL,
    -- All non-excluded, non-PK columns from the original model:
    name        VARCHAR(255),
    owner       UUID,
    status      vmstatus
);
```

### 4. Alembic

`alembic revision --autogenerate` detects the version table automatically
because `setup_versioning` adds it to the shared `Base.metadata`.

For `env.py` with async SQLAlchemy see the asyncpg alembic pattern
(`create_async_engine` + `connection.run_sync`).

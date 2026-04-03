"""
Legacy Migration Utilities (Workers) — Backward Compatibility Re-Export

This module exists so existing worker code that imports from
'workers.legacy_migration' continues to work without changes.
It re-exports the CMG migration utilities, which are the original implementation.

For new code, import from the source-specific submodules:
    CMG:    from workers.legacy_migration.cmg import ...
    Xenium: from workers.legacy_migration.xenium import ...
"""

from workers.legacy_migration.cmg import (
    has_reached_state,
    is_legacy_dataset,
    is_legacy_conversion,
    is_hydrated,
    is_migrated,
    get_migration_status,
    get_retrieved_archive_retrieval_path,
    get_retrieved_archive_extraction_path,
)
from workers.legacy_migration.xenium import is_dataset_archived_in_xenium

__all__ = [
    'has_reached_state',
    'is_legacy_dataset',
    'is_legacy_conversion',
    'is_hydrated',
    'is_migrated',
    'get_migration_status',
    'get_retrieved_archive_retrieval_path',
    'get_retrieved_archive_extraction_path',
    'is_dataset_archived_in_xenium',
]

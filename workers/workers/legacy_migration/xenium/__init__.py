"""
Xenium Legacy Migration Utilities (Workers)

Helper functions for Xenium legacy dataset migration and staging.
Handles datasets migrated from the Xenium PostgreSQL database (origin: 'legacy_xenium').

Xenium does not have Sessions, Conversions, or Tracks features.
"""

import logging
import os
from pathlib import Path
from typing import Dict

import workers.api as api
from workers.config import config
import workers.xenium_api as xenium_api

logger = logging.getLogger(__name__)


def is_legacy_xenium_dataset(dataset: Dict) -> bool:
    """
    Check if a dataset is a legacy Xenium dataset (origin: 'legacy_xenium').

    Args:
        dataset: The dataset dictionary

    Returns:
        True if metadata.origin == 'legacy_xenium', False otherwise
    """
    return (dataset.get('metadata') or {}).get('origin') == 'legacy_xenium'


def _normalize_path(path_value: str) -> str:
    return os.path.normpath(path_value.strip())


def is_dataset_archived_in_xenium(
    origin_path: str,
    use_auth: bool = True,
) -> bool:
    """
    Check archival completion in Xenium by origin_path lookup.

    Returns True only when:
    - Xenium finds the dataset by origin_path
    - Returned dataset.origin_path matches the queried origin_path (normalized)
    - Returned dataset.archive_path is present and non-empty
    """
    dataset = xenium_api.get_dataset_by_origin_path(origin_path, use_auth=use_auth)
    if not dataset:
        return False

    dataset_origin_path = dataset.get('origin_path')
    if not dataset_origin_path:
        logger.warning('Xenium dataset found but origin_path missing in response')
        return False

    if _normalize_path(dataset_origin_path) != _normalize_path(origin_path):
        logger.warning(
            f'Xenium origin_path mismatch: expected={origin_path!r}, got={dataset_origin_path!r}'
        )
        return False

    archive_path = dataset.get('archive_path')
    return bool(archive_path and str(archive_path).strip())


def has_reached_state(dataset_id: int, state: str) -> bool:
    """
    Check if a dataset has reached a specific migration workflow state.

    Xenium migration uses the same dataset_state table and state names as CMG migration.

    Args:
        dataset_id: The dataset ID
        state: The state to check for (e.g., 'MIGRATION_INITIATED', 'STAGED', etc.)

    Returns:
        True if the state exists, False otherwise
    """
    try:
        dataset = api.get_dataset(dataset_id=dataset_id, states=True)
        states = dataset.get('states', [])
        return any(s.get('state') == state for s in states)
    except Exception as e:
        print(f"Error checking state {state} for xenium dataset {dataset_id}: {e}")
        return False


def is_hydrated(dataset_id: int) -> bool:
    """
    Check if a legacy Xenium dataset has been hydrated.

    Xenium datasets with analysis_summary_file_dir in metadata are hydrated when
    that file directory has been parsed and metadata populated.

    Args:
        dataset_id: The dataset ID

    Returns:
        True if the dataset has reached METADATA_POPULATED state
    """
    return has_reached_state(dataset_id, 'METADATA_POPULATED')


def is_migrated(dataset_id: int) -> bool:
    """
    Check if a legacy Xenium dataset has completed migration.

    Args:
        dataset_id: The dataset ID

    Returns:
        True if the dataset has reached MIGRATED state
    """
    return has_reached_state(dataset_id, 'MIGRATED')


def get_migration_status(dataset_id: int) -> Dict[str, bool]:
    """
    Get comprehensive migration status for a Xenium-migrated dataset.

    Args:
        dataset_id: The dataset ID

    Returns:
        Dictionary with migration status flags:
        {
            'is_legacy_xenium': bool,
            'is_migration_initiated': bool,
            'is_retrieved': bool,
            'is_inspected': bool,
            'is_metadata_populated': bool,
            'is_hydrated': bool,
            'is_validated': bool,
            'is_migrated': bool
        }
    """
    try:
        dataset = api.get_dataset(dataset_id=dataset_id, states=True)

        if not is_legacy_xenium_dataset(dataset):
            return {
                'is_legacy_xenium': False,
                'is_migration_initiated': False,
                'is_retrieved': False,
                'is_inspected': False,
                'is_metadata_populated': False,
                'is_hydrated': False,
                'is_validated': False,
                'is_migrated': False,
            }

        state_names = {s.get('state') for s in dataset.get('states', [])}

        return {
            'is_legacy_xenium': True,
            'is_migration_initiated': 'MIGRATION_INITIATED' in state_names,
            'is_retrieved': 'RETRIEVED' in state_names,
            'is_inspected': 'INSPECTED' in state_names,
            'is_metadata_populated': 'METADATA_POPULATED' in state_names,
            'is_hydrated': 'METADATA_POPULATED' in state_names,
            'is_validated': 'STAGED' in state_names,
            'is_migrated': 'MIGRATED' in state_names,
        }
    except Exception as e:
        print(f"Error getting Xenium migration status for dataset {dataset_id}: {e}")
        return {
            'is_legacy_xenium': False,
            'is_migration_initiated': False,
            'is_retrieved': False,
            'is_inspected': False,
            'is_metadata_populated': False,
            'is_hydrated': False,
            'is_validated': False,
            'is_migrated': False,
        }


def get_staging_path(dataset: Dict) -> Path:
    """
    Get the path where Xenium staged dataset files will reside.

    Xenium datasets are staged directly from their origin paths on the
    Xenium instrument file system; no archive retrieval is needed.

    Path structure: {staging}/{dataset_type}/{dataset_id}/

    Args:
        dataset: The dataset dictionary (must have 'id' and 'type')

    Returns:
        Path to the directory where staged Xenium files will be placed
    """
    dataset_type = dataset['type']
    dataset_id = dataset['id']
    staging_dir = Path(config['paths'][dataset_type].get('staging', config['paths'][dataset_type]['migration']))
    return staging_dir / str(dataset_id)

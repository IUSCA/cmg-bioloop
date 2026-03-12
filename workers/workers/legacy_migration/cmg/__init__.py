"""
CMG Legacy Migration Utilities (Workers)

Helper functions for CMG legacy dataset migration and hydration.
Handles datasets migrated from MongoDB (origin: 'legacy') that need metadata population.
"""

from pathlib import Path
from typing import Dict

import workers.api as api
from workers.config import config


def has_reached_state(dataset_id: int, state: str) -> bool:
    """
    Check if a dataset has reached a specific migration workflow state.

    Args:
        dataset_id: The dataset ID
        state: The state to check for (e.g., 'MIGRATION_INITIATED', 'RETRIEVED', etc.)

    Returns:
        True if the state exists, False otherwise
    """
    try:
        dataset = api.get_dataset(dataset_id=dataset_id, states=True)
        states = dataset.get('states', [])
        return any(s.get('state') == state for s in states)
    except Exception as e:
        print(f"Error checking state {state} for dataset {dataset_id}: {e}")
        return False


def is_legacy_dataset(dataset: Dict) -> bool:
    """
    Check if a dataset is a legacy CMG dataset (origin: 'legacy').

    Args:
        dataset: The dataset dictionary

    Returns:
        True if metadata.origin == 'legacy', False otherwise
    """
    # Use `or {}` to safely handle both a missing metadata key and a None value.
    # dataset.get('metadata', {}) alone returns None if the key exists with value None,
    # causing an AttributeError on the subsequent .get('origin') call.
    return (dataset.get('metadata') or {}).get('origin') == 'legacy'


def is_legacy_conversion(conversion: Dict) -> bool:
    """
    Check if a conversion is a legacy CMG conversion (origin: 'legacy').

    Args:
        conversion: The conversion dictionary

    Returns:
        True if metadata.origin == 'legacy', False otherwise
    """
    return (conversion.get('metadata') or {}).get('origin') == 'legacy'


def is_hydrated(dataset_id: int) -> bool:
    """
    Check if a legacy CMG dataset has been hydrated (metadata populated).

    Args:
        dataset_id: The dataset ID

    Returns:
        True if the dataset has reached METADATA_POPULATED state
    """
    return has_reached_state(dataset_id, 'METADATA_POPULATED')


def is_migrated(dataset_id: int) -> bool:
    """
    Check if a legacy CMG dataset has completed migration.

    Args:
        dataset_id: The dataset ID

    Returns:
        True if the dataset has reached MIGRATED state
    """
    return has_reached_state(dataset_id, 'MIGRATED')


def get_migration_status(dataset_id: int) -> Dict[str, bool]:
    """
    Get comprehensive migration status for a CMG-migrated dataset.

    Args:
        dataset_id: The dataset ID

    Returns:
        Dictionary with migration status flags:
        {
            'is_legacy': bool,
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

        if not is_legacy_dataset(dataset):
            return {
                'is_legacy': False,
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
            'is_legacy': True,
            'is_migration_initiated': 'MIGRATION_INITIATED' in state_names,
            'is_retrieved': 'RETRIEVED' in state_names,
            'is_inspected': 'INSPECTED' in state_names,
            'is_metadata_populated': 'METADATA_POPULATED' in state_names,
            'is_hydrated': 'METADATA_POPULATED' in state_names,
            'is_validated': 'STAGED' in state_names,
            'is_migrated': 'MIGRATED' in state_names,
        }
    except Exception as e:
        print(f"Error getting CMG migration status for dataset {dataset_id}: {e}")
        return {
            'is_legacy': False,
            'is_migration_initiated': False,
            'is_retrieved': False,
            'is_inspected': False,
            'is_metadata_populated': False,
            'is_hydrated': False,
            'is_validated': False,
            'is_migrated': False,
        }


def get_retrieved_archive_retrieval_path(dataset: Dict) -> Path:
    """
    Get the path where the retrieved archive file should be downloaded to.

    Path structure: {migration}/{dataset_type}/retrieved_archives/{dataset_id}/

    Args:
        dataset: The dataset dictionary (must have 'id' and 'type')

    Returns:
        Path to the directory where the archive file will be stored
    """
    dataset_type = dataset['type']
    dataset_id = dataset['id']
    migration_dir = Path(config['paths'][dataset_type]['migration'])
    return migration_dir / 'retrieved_archives' / str(dataset_id)


def get_retrieved_archive_extraction_path(dataset: Dict) -> Path:
    """
    Get the path where the extracted archive contents will be placed.

    Path structure: {migration}/{dataset_type}/extracted_archives/{dataset_id}/

    This is where retrieve_archive places the extracted contents and where
    inspect_dataset reads from for legacy CMG datasets.

    Args:
        dataset: The dataset dictionary (must have 'id' and 'type')

    Returns:
        Path to the directory containing extracted archive contents
    """
    dataset_type = dataset['type']
    dataset_id = dataset['id']
    migration_dir = Path(config['paths'][dataset_type]['migration'])
    return migration_dir / 'extracted_archives' / str(dataset_id)

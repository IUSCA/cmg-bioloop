import os
import tarfile
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.api as api
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
from workers.config import config
from workers.dataset import get_bundle_name
from workers.legacy_migration import get_retrieved_archive_retrieval_path

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def populate_bundle_metadata(dataset: dict, archive_path: Path) -> dict:
    """
    Populate bundle metadata for a legacy dataset from the retrieved archive.
    This is the core logic for generating bundle information from an existing archive.
    
    Args:
        dataset: Dataset object from Bioloop API
        archive_path: Path to the archive tar file
    
    Returns:
        dict: Bundle attributes (name, size, md5)
    """
    logger.info(f'Populating bundle metadata for dataset {dataset["name"]}')
    
    # Calculate bundle metadata from the archive
    bundle_size = archive_path.stat().st_size
    bundle_checksum = utils.checksum(archive_path)
    
    bundle_attrs = {
        'name': archive_path.name,
        'size': bundle_size,
        'md5': bundle_checksum,
    }
    
    logger.info(f'Bundle metadata calculated: {archive_path.name}, size={bundle_size}, md5={bundle_checksum}')
    return bundle_attrs


def populate_metadata(celery_task: WorkflowTask, dataset: dict) -> dict:
    """
    Populate metadata for legacy datasets, including bundle information.
    This step is specifically for hydrating legacy CMG datasets that were
    migrated from the old system but lack complete metadata.
    
    Currently focuses on bundle population. Can be extended for other metadata
    population needs in the future.
    
    Args:
        celery_task: WorkflowTask instance
        dataset: Dataset object from Bioloop API
    
    Returns:
        dict: Updated bundle attributes
    """
    logger.info(f'Populating metadata for dataset {dataset["id"]}')
    
    # Get the archive path from the new retrieval location
    archive_retrieval_dir = get_retrieved_archive_retrieval_path(dataset)
    archive_filename = get_bundle_name(dataset)
    archive_path = archive_retrieval_dir / archive_filename
    
    if not archive_path.exists():
        raise FileNotFoundError(f'Archive file not found at {archive_path}. '
                              f'Ensure retrieve_archive step completed successfully.')
    
    # Populate bundle metadata using the retrieved archive
    # This is isolated in its own method to make it clear that
    # this step can be used for other metadata population in the future
    bundle_attrs = populate_bundle_metadata(dataset, archive_path)
    
    # TODO: Add other metadata population logic here if needed
    # For example: genomic_details, additional_metadata, etc.
    
    logger.info(f'Metadata population complete for dataset {dataset["id"]}')
    return bundle_attrs


def populate_metadata_dataset(celery_task, dataset_id, **kwargs):
    """
    Celery task wrapper for populate_metadata.
    Populates bundle and other metadata, then sets METADATA_POPULATED state.
    
    Args:
        celery_task: WorkflowTask instance
        dataset_id: ID of the dataset
    
    Returns:
        dataset_id: For passing to next workflow step
    """
    dataset = api.get_dataset(dataset_id=dataset_id, bundle=True)
    bundle_attrs = populate_metadata(celery_task, dataset)
    
    # Update dataset with bundle metadata
    # Note: We update bundle info here, not in archive step,
    # because this is for legacy datasets that already have archives
    update_data = {
        'bundle': bundle_attrs
    }
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    api.add_state_to_dataset(dataset_id=dataset_id, state='METADATA_POPULATED')
    
    logger.info(f'Metadata populated for dataset {dataset_id}')
    return dataset_id,


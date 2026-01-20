import os
import shutil
import tarfile
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.api as api
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
import workers.workflow_utils as wf_utils
from workers import exceptions as exc
from workers.config import config
from workers.dataset import get_bundle_name
from workers.legacy_migration import (
    get_retrieved_archive_retrieval_path,
    get_retrieved_archive_extraction_path
)

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def retrieve_archive(celery_task: WorkflowTask, dataset: dict) -> None:
    """
    Retrieve archived legacy dataset from archive location (SDA or local) and extract it.
    
    The archive is downloaded to: {migration}/retrieved_archives/{dataset_id}/
    The extracted contents are placed in: {migration}/extracted_archives/{dataset_id}/
    
    Args:
        celery_task: WorkflowTask instance for progress reporting
        dataset: Dataset object from Bioloop API
    """
    archive_path = dataset['archive_path']
    
    # Get paths and ensure clean state by removing any existing directories
    archive_retrieval_dir = get_retrieved_archive_retrieval_path(dataset)
    extraction_path = get_retrieved_archive_extraction_path(dataset)
    
    # Clean up existing paths if they exist (from previous runs/retries)
    if archive_retrieval_dir.exists():
        logger.info(f'Removing existing retrieval directory: {archive_retrieval_dir}')
        shutil.rmtree(archive_retrieval_dir)
    
    if extraction_path.exists():
        logger.info(f'Removing existing extraction directory: {extraction_path}')
        shutil.rmtree(extraction_path)
    
    # Create retrieval directory
    archive_retrieval_dir.mkdir(parents=True, exist_ok=True)
    
    # Use dataset-specific archive filename
    archive_filename = get_bundle_name(dataset)
    archive_download_path = archive_retrieval_dir / archive_filename
    
    # Determine if archive_path is SDA or local filesystem
    # Use SDA if APP_ENV is 'production'
    app_env = os.environ.get('APP_ENV', None)
    is_sda_path = (app_env == 'production')
    
    if is_sda_path:
        # Production mode: Download from SDA
        logger.info(f'Downloading archive from SDA {archive_path} to {archive_download_path}')
        wf_utils.download_file_from_sda(sda_file_path=archive_path,
                                        local_file_path=archive_download_path,
                                        celery_task=celery_task)
    else:
        # Docker/local mode: Copy from local archive
        logger.info(f'Copying archive from {archive_path} to {archive_download_path}')
        
        # Ensure the archive file exists
        if not Path(archive_path).exists():
            raise FileNotFoundError(f'Archive file not found at: {archive_path}')
        
        # Copy the file
        shutil.copy2(archive_path, archive_download_path)
        
        # Verify the copy was successful
        if not archive_download_path.exists():
            raise Exception(f'Failed to copy archive to: {archive_download_path}')
  
    logger.info(f'Successfully retrieved archive to {archive_download_path}')
    
    # Extract the archive to a temporary directory
    logger.info(f'Extracting archive {archive_download_path}')
    temp_extraction_dir = archive_retrieval_dir / 'temp_extraction'
    temp_extraction_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with tarfile.open(archive_download_path, 'r') as tar:
            tar.extractall(path=temp_extraction_dir)
        logger.info(f'Successfully extracted archive to {temp_extraction_dir}')
    except Exception as e:
        logger.error(f'Failed to extract archive {archive_download_path}: {e}')
        # Clean up temp directory
        if temp_extraction_dir.exists():
            shutil.rmtree(temp_extraction_dir)
        raise exc.RetryableException(f'Failed to extract archive: {e}')
    
    # Determine what was extracted
    extracted_items = list(temp_extraction_dir.iterdir())
    
    if len(extracted_items) == 0:
        # Empty archive
        logger.error(f'Archive {archive_download_path} is empty')
        shutil.rmtree(temp_extraction_dir)
        raise exc.RetryableException(f'Archive is empty: {archive_download_path}')
    
    # Create the final extraction path (already cleaned up at the start)
    extraction_path.mkdir(parents=True, exist_ok=True)
    
    # If archive contains a single top-level directory, move its contents
    # Otherwise, move all items as-is
    if len(extracted_items) == 1 and extracted_items[0].is_dir():
        # Single directory case - move the contents of the directory, not the directory itself
        single_dir = extracted_items[0]
        logger.info(f'Archive contains single directory: {single_dir.name}')
        logger.info(f'Moving contents of {single_dir.name} to {extraction_path}')
        
        for item in single_dir.iterdir():
            dest = extraction_path / item.name
            shutil.move(str(item), str(dest))
    else:
        # Multiple items or single file - move all items as-is
        logger.info(f'Archive contains {len(extracted_items)} items')
        logger.info(f'Moving all extracted contents to {extraction_path}')
        
        for item in extracted_items:
            dest = extraction_path / item.name
            shutil.move(str(item), str(dest))
    
    # Remove the temp extraction directory
    shutil.rmtree(temp_extraction_dir)
    logger.info(f'Extracted content available at: {extraction_path}')


def retrieve_archive_dataset(celery_task, dataset_id, **kwargs):
    """
    Celery task wrapper for retrieve_archive.
    Retrieves archived dataset, extracts it, and sets RETRIEVED state.
    
    Args:
        celery_task: WorkflowTask instance
        dataset_id: ID of the dataset
    
    Returns:
        dataset_id: For passing to next workflow step
    """
    dataset = api.get_dataset(dataset_id=dataset_id)
    retrieve_archive(celery_task, dataset)
    
    # Set RETRIEVED state
    api.add_state_to_dataset(dataset_id=dataset_id, state='RETRIEVED')
    
    logger.info(f'Archive retrieved and extracted for dataset {dataset_id}')
    return dataset_id,


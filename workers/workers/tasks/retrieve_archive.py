import os
import shutil
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.api as api
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
import workers.workflow_utils as wf_utils
from workers.dataset import get_bundle_staged_path
from workers import exceptions as exc

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def retrieve_archive(celery_task: WorkflowTask, dataset: dict) -> str:
    """
    Retrieve archived legacy dataset from archive location (SDA or local).
    Similar to stage.py but specifically for legacy datasets that need hydration.
    
    Args:
        celery_task: WorkflowTask instance for progress reporting
        dataset: Dataset object from Bioloop API
    
    Returns:
        bundle_download_path: Path where the bundle was retrieved
    """
    archive_path = dataset['archive_path']
    bundle = dataset["bundle"]
    bundle_md5 = bundle["md5"]
    bundle_download_path = Path(get_bundle_staged_path(dataset=dataset))
    
    # Ensure parent directory exists
    bundle_download_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Determine if archive_path is SDA or local filesystem
    # Use SDA if APP_ENV is 'production'
    app_env = os.environ.get('APP_ENV', None)
    is_sda_path = (app_env == 'production')
    
    if is_sda_path:
        # Production mode: Download from SDA
        logger.info(f'Downloading bundle from SDA {archive_path} to {bundle_download_path}')
        wf_utils.download_file_from_sda(sda_file_path=archive_path,
                                        local_file_path=bundle_download_path,
                                        celery_task=celery_task)
    else:
        # Docker/local mode: Copy from local archive
        logger.info(f'Copying bundle from local archive {archive_path} to {bundle_download_path}')
        
        # Ensure the archive file exists
        if not Path(archive_path).exists():
            raise FileNotFoundError(f'Archive file not found at: {archive_path}')
        
        # Copy the file from archive to staging
        shutil.copy2(archive_path, bundle_download_path)
        
        # Verify the copy was successful
        if not bundle_download_path.exists():
            raise Exception(f'Failed to copy bundle from archive to staging: {bundle_download_path}')
    
    # Verify checksum
    evaluated_checksum = utils.checksum(bundle_download_path)
    if evaluated_checksum != bundle_md5:
        raise exc.ValidationFailed(f'Expected checksum of downloaded/copied file to be {bundle_md5},'
                                   f' but evaluated checksum was {evaluated_checksum}')
    
    logger.info(f'Successfully retrieved archive to {bundle_download_path}')
    return str(bundle_download_path)


def retrieve_archive_dataset(celery_task, dataset_id, **kwargs):
    """
    Celery task wrapper for retrieve_archive.
    Retrieves archived dataset and sets RETRIEVED state.
    
    Args:
        celery_task: WorkflowTask instance
        dataset_id: ID of the dataset
    
    Returns:
        dataset_id: For passing to next workflow step
    """
    dataset = api.get_dataset(dataset_id=dataset_id, bundle=True)
    bundle_path = retrieve_archive(celery_task, dataset)
    
    # Update dataset metadata with bundle path info
    update_data = {
        'metadata': {
            'retrieved_bundle_path': bundle_path,
        }
    }
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    api.add_state_to_dataset(dataset_id=dataset_id, state='RETRIEVED')
    
    logger.info(f'Archive retrieved for dataset {dataset_id}')
    return dataset_id,


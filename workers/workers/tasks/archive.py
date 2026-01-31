import json
import os
import shutil
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.api as api
import workers.cmd as cmd
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
import workers.workflow_utils as wf_utils
from workers.config import config
from workers.legacy_migration import is_legacy_dataset

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def make_tarfile(celery_task: WorkflowTask, tar_path: Path, source_dir: str, source_size: int):
    """

    @param celery_task:
    @param tar_path:
    @param source_dir:
    @param source_size:
    @return:
    """
    logger.info(f'creating tar of {source_dir} at {tar_path}')
    # if the tar file already exists, delete it
    if tar_path.exists():
        tar_path.unlink()

    with wf_utils.track_progress_parallel(celery_task=celery_task,
                                          name='tar',
                                          progress_fn=lambda: tar_path.stat().st_size,
                                          total=source_size,
                                          units='bytes'):
        # using python to create tar files does not support --sparse
        # SDA has trouble uploading sparse tar files
        cmd.tar(tar_path=tar_path, source_dir=source_dir)

    # TODO: validate files inside tar
    return tar_path


def archive(celery_task: WorkflowTask, dataset: dict, delete_local_file: bool = False):
    # Check if this is a legacy dataset and if legacy migration is still in progress
    is_legacy = is_legacy_dataset(dataset)
    legacy_migration_incomplete = not config.get('legacy_migration', {}).get('completed', True)
    
    # Determine if SDA upload should be used based on APP_ENV
    app_env = os.environ.get('APP_ENV', None)
    use_sda = (app_env == 'production')
    
    # Special handling for legacy datasets during ongoing migration
    if is_legacy and legacy_migration_incomplete and use_sda:
        logger.info(f'Legacy dataset detected during ongoing migration: {dataset["name"]}')
        logger.info(f'Skipping bundle creation and SDA upload - CMG app handles archival')
        
        # Get the legacy SDA archive path (dataset.py already returns legacy path for legacy datasets)
        # SDA-archival directories are different for legacy Datasets (the ones
        # created by the legacy CMG application) and new Datasets (the ones created
        # by this application).
        sda_dir = wf_utils.get_archive_dir(dataset['type'], legacy_dataset=True)
        bundle_name = f"{dataset['name']}.tar"
        sda_bundle_path = f'{sda_dir}/{bundle_name}'
        
        logger.info(f'Expected SDA archive path: {sda_bundle_path}')
        logger.info(f'Waiting for CMG app to complete SDA upload...')
        
        # Wait for CMG's upload to complete
        # PLACEHOLDER: This will wait until the file appears in SDA
        wf_utils.wait_for_sda_upload(
            sda_file_path=sda_bundle_path,
            celery_task=celery_task
        )
        
        logger.info(f'CMG app SDA upload completed: {sda_bundle_path}')
        
        # Note: We don't have bundle_attrs (size, checksum) since we didn't create the tar
        # The bundle info should already exist in the dataset metadata from CMG
        bundle_attrs = None
        archive_path = sda_bundle_path
        
    else:
        # Standard flow: Create tar bundle and upload/archive
        logger.info(f'Creating tar bundle for dataset: {dataset["name"]}')
        
        # Tar the dataset directory and compute checksum
        bundle = Path(f'{config["paths"][dataset["type"]]["bundle"]["generate"]}/{dataset["name"]}.tar')
        
        # Ensure parent directory exists
        bundle.parent.mkdir(parents=True, exist_ok=True)

        make_tarfile(celery_task=celery_task,
                     tar_path=bundle,
                     source_dir=dataset['origin_path'],
                     source_size=dataset['du_size'])

        bundle_size = bundle.stat().st_size
        bundle_checksum = utils.checksum(bundle)
        bundle_attrs = {
            'name': bundle.name,
            'size': bundle_size,
            'md5': bundle_checksum,
        }
        
        if use_sda:
            # Production mode: Upload to SDA
            
            # SDA-archival directories are different for legacy Datasets (the ones
            # created by the legacy CMG application) and new Datasets (the ones created
            # by this application).
            sda_dir = wf_utils.get_archive_dir(dataset['type'], legacy_dataset=is_legacy)
            sda_bundle_path = f'{sda_dir}/{bundle.name}'
            
            logger.info(f'Uploading bundle {bundle} to SDA at {sda_bundle_path}')
            wf_utils.upload_file_to_sda(local_file_path=bundle,
                                        sda_file_path=sda_bundle_path,
                                        celery_task=celery_task)
            
            logger.info(f'Successfully uploaded bundle to SDA: {sda_bundle_path}')
            archive_path = sda_bundle_path
        else:
            # Docker/local mode: Use local archive directory
            local_archive_dir = Path(f'/opt/sca/data/archive/{dataset["type"].lower()}')
            local_archive_dir.mkdir(parents=True, exist_ok=True)
            
            local_archive_path = local_archive_dir / bundle.name
            
            # Copy the bundle to the local archive location
            logger.info(f'Copying bundle {bundle} to local archive at {local_archive_path}')
            shutil.copy2(bundle, local_archive_path)
            
            # Verify the copy was successful
            if not local_archive_path.exists():
                raise Exception(f'Failed to copy bundle to local archive: {local_archive_path}')
            
            # Verify checksum of archived file
            archived_checksum = utils.checksum(local_archive_path)
            if archived_checksum != bundle_checksum:
                raise Exception(f'Checksum mismatch after archiving. Original: {bundle_checksum}, Archived: {archived_checksum}')
            
            logger.info(f'Successfully archived bundle to local storage: {local_archive_path}')
            archive_path = str(local_archive_path)

        if delete_local_file:
            # file successfully archived, delete the local copy
            logger.info("Deleting local bundle after successful archiving")
            bundle.unlink()

    return archive_path, bundle_attrs


def archive_dataset(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id, bundle=True)
    archive_path, bundle_attrs = archive(celery_task, dataset)
    
    # Prepare update data
    update_data = {
        'archive_path': archive_path,
    }
    
    # Only update bundle attributes if they were generated
    # (legacy datasets during migration may not have new bundle_attrs)
    if bundle_attrs is not None:
        update_data['bundle'] = bundle_attrs
    
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    api.add_state_to_dataset(dataset_id=dataset_id, state='ARCHIVED')

    return dataset_id,

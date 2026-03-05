import json
import shutil
import time
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.sda as sda
import workers.api as api
import workers.cmd as cmd
import workers.cmg_api as cmg_api
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


def wait_for_cmg_archival(dataset_name: str, origin_path: str, dataset_type: str,
                          celery_task: WorkflowTask = None,
                          poll_interval_seconds: int = 300,
                          timeout_seconds: int = 86400) -> None:
    """
    Wait for CMG to complete archival by polling CMG database state.

    For RAW_DATA: polls until dataset.archived === true
    For DATA_PRODUCT: polls until dataproduct.paths.archive is set

    Args:
        dataset_name: Dataset name for logging
        origin_path: Origin path of the dataset (used to query CMG)
        dataset_type: Either 'RAW_DATA' or 'DATA_PRODUCT'
        celery_task: Celery task for progress tracking (optional)
        poll_interval_seconds: How often to check CMG (default: 300 seconds / 5 minutes)
        timeout_seconds: Maximum time to wait (default: 86400 seconds / 24 hours)

    Raises:
        TimeoutError: If CMG archival doesn't complete within timeout_seconds
    """
    logger.info(f'{dataset_name} - waiting for CMG to complete archival')
    logger.info(f'{dataset_name} - origin_path: {origin_path}, type: {dataset_type}')
    logger.info(f'{dataset_name} - poll interval: {poll_interval_seconds}s, timeout: {timeout_seconds}s')

    start_time = time.time()
    poll_count = 0

    while True:
        elapsed_time = time.time() - start_time
        poll_count += 1

        # Check timeout
        if elapsed_time > timeout_seconds:
            error_msg = (
                f'{dataset_name} - timeout waiting for CMG archival completion '
                f'(elapsed: {int(elapsed_time)}s / timeout: {timeout_seconds}s, polls: {poll_count})'
            )
            logger.error(error_msg)
            raise TimeoutError(error_msg)

        # Check if CMG has completed archival
        try:
            is_archived = cmg_api.is_dataset_archived_in_cmg(origin_path, dataset_type)

            if is_archived:
                logger.info(
                    f'{dataset_name} - CMG archival completed '
                    f'(elapsed: {int(elapsed_time)}s, polls: {poll_count})'
                )
                return
            else:
                logger.info(
                    f'{dataset_name} - CMG archival not yet complete '
                    f'(elapsed: {int(elapsed_time)}s / timeout: {timeout_seconds}s, poll #{poll_count})'
                )
        except Exception as e:
            logger.warning(
                f'{dataset_name} - error checking CMG archival status (poll #{poll_count}): {e}'
            )

        # Update progress if celery_task provided
        if celery_task:
            time_remaining_sec = max(0, timeout_seconds - elapsed_time)
            progress_obj = {
                'name': f'Waiting for CMG archival (poll #{poll_count})',
                'time_remaining_sec': time_remaining_sec,
            }
            celery_task.update_progress(progress_obj)

        # Wait before next poll
        time.sleep(poll_interval_seconds)


def archive(celery_task: WorkflowTask, dataset: dict, delete_local_file: bool = False):
    """
    Archive a dataset by creating a tar bundle and uploading to SDA.

    Handles two scenarios:
    1. Concurrent CMG/Bioloop registration (cmg_id exists):
       - Verifies dataset exists in CMG API (STRICT: fails if not found when legacy_migration enabled)
       - Waits for CMG to complete archival
       - Retrieves bundle metadata (hash, size) from SDA/HSI
       - STRICT: Fails if archive cannot be found or hash cannot be retrieved from HSI
       - Saves bundle metadata to database (md5 from HSI)

    2. Registrations which occur in CMG-Bioloop only, and not in CMG.
       - Creates tar bundle locally
       - Computes hash locally
       - Uploads to SDA
       - Saves bundle metadata to database

    Args:
        celery_task: Celery task for progress tracking
        dataset: Dataset dict with metadata
        delete_local_file: Whether to delete local bundle after archival

    Returns:
        tuple: (archive_path, bundle_attrs) where bundle_attrs may be None

    Raises:
        Exception: If legacy_migration enabled and CMG validation fails
        Exception: If SDA archive cannot be verified or hash cannot be retrieved
    """
    # Check if this is a legacy dataset and if the legacy CMG application is still active
    is_legacy = is_legacy_dataset(dataset)
    legacy_application_active = config.get('legacy_application_active', False)

    # Check if dataset has a CMG ID (was registered in CMG concurrently)
    cmg_id = dataset.get('cmg_id')
    dataset_type = dataset['type']
    dataset_name = dataset['name']
    origin_path = dataset.get('origin_path')

    # Tar the dataset directory and compute checksum
    bundle = Path(f'{config["paths"][dataset["type"]]["bundle"]["generate"]}/{dataset["name"]}.tar')

    # Ensure parent directory exists
    bundle.parent.mkdir(parents=True, exist_ok=True)

    # If dataset is legacy, it means the legacy CMG application is also archiving this
    #  dataset concurrently. Wait for CMG to complete archival
    if is_legacy:
        logger.info(f'{dataset_name} - detected CMG ID: {cmg_id}')

        # STRICT VALIDATION: If legacy_migration is enabled, we MUST be able to verify CMG's archival
        if legacy_application_active:
            logger.info(f'{dataset_name} - legacy_migration is enabled, strict validation required')

            # Verify we can find the dataset in CMG
            try:
                if dataset_type == 'RAW_DATA':
                    cmg_entity = cmg_api.get_dataset_by_origin_path(origin_path)
                elif dataset_type == 'DATA_PRODUCT':
                    cmg_entity = cmg_api.get_dataproduct_by_origin_path(origin_path)
                else:
                    cmg_entity = None

                if not cmg_entity:
                    error_msg = (
                        f'{dataset_name} - FATAL: dataset has cmg_id but cannot be found in CMG API. '
                        f'This indicates a data consistency issue. '
                        f'CMG ID: {cmg_id}, origin_path: {origin_path}, type: {dataset_type}'
                    )
                    logger.error(error_msg)
                    raise Exception(error_msg)

                logger.info(f'{dataset_name} - verified dataset exists in CMG')
            except Exception as e:
                error_msg = (
                    f'{dataset_name} - FATAL: failed to verify dataset in CMG API: {e}. '
                    f'Cannot proceed with archival when legacy_migration is enabled.'
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        logger.info(f'{dataset_name} - waiting for CMG to complete archival to avoid concurrent SDA uploads')

        # Wait for CMG archival to complete
        wait_for_cmg_archival(
            dataset_name=dataset_name,
            origin_path=origin_path,
            dataset_type=dataset_type,
            celery_task=celery_task
        )

        # Get the SDA path where CMG uploaded the archive
        dataset_type_archive_dir = wf_utils.get_archive_dir(dataset['type'], legacy_dataset=is_legacy)
        dataset_bundle_path = f'{dataset_type_archive_dir}/{bundle.name}'

        logger.info(f'{dataset_name} - CMG archival completed, verifying archive in SDA')

        # STRICT VALIDATION: Verify the archive exists in SDA and get its hash
        try:
            # Check if file exists in SDA
            if not sda.exists(dataset_bundle_path):
                error_msg = (
                    f'{dataset_name} - FATAL: archive path does not exist in SDA: {dataset_bundle_path}. '
                    f'CMG reported archival complete but file not found.'
                )
                logger.error(error_msg)
                raise Exception(error_msg)

            logger.info(f'{dataset_name} - archive exists in SDA, retrieving hash')

            # Get hash from SDA
            bundle_checksum = sda.get_hash(dataset_bundle_path, missing_ok=False)
            if not bundle_checksum:
                error_msg = (
                    f'{dataset_name} - FATAL: cannot retrieve hash from SDA for: {dataset_bundle_path}. '
                    f'File exists but hash is not available.'
                )
                logger.error(error_msg)
                raise Exception(error_msg)

            logger.info(f'{dataset_name} - retrieved hash from SDA: {bundle_checksum}')

            # Get file size from SDA
            bundle_size = sda.get_size(dataset_bundle_path)
            logger.info(f'{dataset_name} - retrieved size from SDA: {bundle_size} bytes')

            # Create bundle attributes from SDA metadata
            # These will be saved to the bundle table in the database
            bundle_attrs = {
                'name': bundle.name,
                'size': bundle_size,
                'md5': bundle_checksum,  # Hash retrieved from HSI/SDA
            }

            logger.info(f'{dataset_name} - successfully verified CMG archive with bundle metadata from SDA')
            logger.info(f'{dataset_name} - bundle will be saved: size={bundle_size}, md5={bundle_checksum}')
        except Exception as e:
            error_msg = (
                f'{dataset_name} - FATAL: failed to retrieve bundle metadata from SDA: {e}. '
                f'Archive path: {dataset_bundle_path}'
            )
            logger.error(error_msg)
            raise Exception(error_msg)
    else:
        logger.info(f'Creating tar bundle for dataset: {dataset["name"]}')
        
        # Determine bundle generation path based on workflow
        # Datasets can be ingested via the 'Integrated' workflow, or the 'Intake Integrated' workflow.
        # If the Dataset being ingested in present on one of the intake nodes (k2/k3/k4), the archive will be created on the Archive node.
        # If the Dataset being ingested in present on one of the fetch nodes (Slate-scratch / Slate-project), the archive will be created on the Fetch node.
        workflow_name = celery_task.workflow.workflow.get('name', '') if celery_task.workflow else ''
        # Determine if this task is running on the Archive node or the Fetch node
        is_archive_node = workflow_name == 'intake_integrated'
        is_fetch_node = workflow_name == 'integrated'

        bundle_config = config["paths"][dataset["type"]]["bundle"]        

        # Determine the bundle-generation base path based on whether this task is running on the Archive node or the Fetch node
        if is_archive_node:
            bundle_base = bundle_config['generate_archive_node']
        elif is_fetch_node:
            bundle_base = bundle_config['generate']
        else:
            raise Exception(f'Invalid workflow name: {workflow_name}. Expected workflow name to be either "intake_integrated" or "integrated".')
        
        # Tar the dataset directory and compute checksum
        bundle = Path(f'{bundle_base}/{dataset["name"]}.tar')
        
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

        dataset_type_archive_dir = wf_utils.get_archive_dir(dataset['type'], legacy_dataset=is_legacy)
        dataset_bundle_path = f'{dataset_type_archive_dir}/{bundle.name}'

        wf_utils.archive(local_file_path=bundle,
                          archive_path=dataset_bundle_path,
                          celery_task=celery_task)

        # If the task is running on the Archive node, or if the delete_local_file flag is set, delete the local bundle
        if is_archive_node or delete_local_file:
            # file successfully archived, delete the local copy
            logger.info("Deleting local bundle after successful archiving")
            bundle.unlink()

    return dataset_bundle_path, bundle_attrs


def archive_dataset(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id, bundle=True)
    archived_bundle_path, bundle_attrs = archive(celery_task, dataset)

    update_data = {
        'archive_path': archived_bundle_path,
        'bundle': bundle_attrs
    }

    # TODO for AI-agent - is this needed? Is it possible that this was
    #  introduced at a point when this archive step was planned to run
    #  as part of the 'integrated' workflow? Or is this conditional block
    #  really serving any purpose, as opposed to doing:
    #  ```
    #  update_data = {
    #         'archive_path': archived_bundle_path,
    #         'bundle': bundle_attrs
    #     }
    #  ```
    # # Only update bundle attributes if they were generated
    # # (legacy datasets during migration may not have new bundle_attrs)
    # if bundle_attrs is not None:
    #     update_data['bundle'] = bundle_attrs

    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    api.add_state_to_dataset(dataset_id=dataset_id, state='ARCHIVED')

    return dataset_id,

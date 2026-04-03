import datetime
import itertools
import re
import time
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.cmg_api as cmg_api
import workers.config.celeryconfig as celeryconfig
import workers.xenium_api as xenium_api
from workers.config import config
from workers.dataset import is_nanopore_dataset

logger = get_task_logger(__name__)

app = Celery("tasks")
app.config_from_object(celeryconfig)


def dir_last_modified_time(dataset_path: Path) -> float:
    """
    Obtain the most recent modification time for a directory and all its contents in a recursive manner.
    At times, when copying files, outdated modification times may be retained.
    To address this, monitor the modification time of the root directory as well.

    If the copy process is configured to preserve the metadata of the source file, it will update the m_time
    of the target file after the copy process. This will update the c_time of the target file. In these cases,
    c_time will be bigger than m_time. So, we will consider the maximum of c_time and m_time of the file / directory
    as the last modified time.


    Args:
    dataset_path (Path): Path object to the directory.

    Returns:
    float: The last modified time in epoch seconds.
    """
    paths = itertools.chain([dataset_path], dataset_path.rglob('*'))
    return max(
        (max(p.lstat().st_mtime, p.lstat().st_ctime) for p in paths if p.exists()),
        default=time.time()
    )


def update_progress(celery_task, mod_time, time_remaining_sec):
    d1 = datetime.datetime.utcfromtimestamp(mod_time)
    prog_obj = {
        'name': d1.isoformat(),
        'time_remaining_sec': time_remaining_sec,
    }
    celery_task.update_progress(prog_obj)


def check_completion_markers(source: Path) -> bool:
    """
    Check if a dataset directory contains completion marker files.
    Required for standard Illumina datasets, not required for nanopore datasets.
    
    Completion markers:
    - Any file matching pattern '*CopyComplete*'
    - Exact file 'RTAComplete.txt'
    
    Args:
        source: Path to the dataset directory
        
    Returns:
        bool: True if completion markers are found, False otherwise
    """
    try:
        # Iterate through all files in the directory (not recursive)
        for item in source.iterdir():
            if item.is_file():
                filename = item.name
                
                # Check for CopyComplete pattern (case-sensitive)
                if re.search('CopyComplete', filename):
                    logger.info(f'Found completion marker: {filename}')
                    return True
                
                # Check for exact RTAComplete.txt match
                if filename == 'RTAComplete.txt':
                    logger.info(f'Found completion marker: {filename}')
                    return True
        
        return False
    except Exception as e:
        logger.error(f'Error checking completion markers in {source}: {e}')
        return False


def await_stability(celery_task, dataset_id, wait_seconds: int = None, recency_threshold=None, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id)
    origin_path = Path(dataset['origin_path'])
    origin_path_str = dataset['origin_path']
    dataset_type = dataset['type']
    dataset_name = dataset['name']

    # Determine if this is a nanopore dataset
    is_nanopore = is_nanopore_dataset(origin_path_str)
    is_raw_data = dataset_type == 'RAW_DATA'
    
    # recency_threshold is the time to wait before considering the dataset stable
    # precedence order:
    # 1. recency_threshold parameter (explicit override)
    # 2. nanopore-specific threshold for nanopore datasets
    # 3. standard threshold for other datasets
    if recency_threshold is not None:
        threshold = recency_threshold
        logger.info(f'{dataset_name} - using explicit threshold parameter: {threshold} seconds')
    elif is_nanopore:
        threshold = config['registration']['recency_threshold_seconds_nanopore']
        logger.info(f'{dataset_name} - nanopore dataset detected, using nanopore threshold: {threshold} seconds')
    else:
        threshold = config['registration']['recency_threshold_seconds']
        logger.info(f'{dataset_name} - standard dataset, using standard threshold: {threshold} seconds')

    # wait_seconds is the time to wait between stability checks
    # precedence order:
    # 1. wait_seconds parameter
    # 2. config file
    _wait_seconds = (wait_seconds or
                     config['registration']['wait_between_stability_checks_seconds'])
    logger.info(f'{dataset_name} - wait_seconds: {_wait_seconds} seconds')

    # Maximum timeout for standard Illumina datasets waiting for completion markers
    max_timeout_seconds = 43200  # 12 hours
    start_time = time.time()

    # For standard Illumina RAW_DATA datasets, we need to wait for completion markers
    needs_completion_markers = is_raw_data and not is_nanopore
    completion_markers_found = False

    if needs_completion_markers:
        logger.info(f'{dataset_name} - standard Illumina dataset, will check for completion markers')

    if not origin_path.exists():
        logger.warning(
            f'{dataset_name} - origin path does not exist at start of stability check: {origin_path}; '
            f'skipping stability loop'
        )

    while origin_path.exists():
        elapsed_time = time.time() - start_time
        
        # Check for completion markers if needed
        if needs_completion_markers and not completion_markers_found:
            completion_markers_found = check_completion_markers(origin_path)
            if completion_markers_found:
                logger.info(f'{dataset_name} - completion markers found, now waiting for stability')
            else:
                # Check if we've exceeded the maximum timeout waiting for completion markers
                if elapsed_time > max_timeout_seconds:
                    error_msg = (
                        f'{dataset_name} - exceeded maximum timeout ({max_timeout_seconds}s / 12 hours) '
                        f'waiting for completion markers. No "*CopyComplete*" or "RTAComplete.txt" found in {origin_path}'
                    )
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                logger.info(f'{dataset_name} - no completion markers yet, continuing to wait '
                           f'(elapsed: {int(elapsed_time)}s / max: {max_timeout_seconds}s)')
        
        # Check directory stability (recency)
        mod_time = dir_last_modified_time(origin_path)
        delta = time.time() - mod_time

        logger.info(f'{dataset_name} - dataset last modified {int(delta)}s ago')
        update_progress(celery_task, mod_time, threshold - delta)

        # Determine if dataset is ready
        # For standard Illumina: need completion markers AND stability
        # For nanopore: just need stability
        # For other types: just need stability
        if needs_completion_markers:
            if completion_markers_found and delta > threshold:
                logger.info(f'{dataset_name} - completion markers found and stability achieved')
                break
        else:
            if delta > threshold:
                logger.info(f'{dataset_name} - stability threshold met')
                break

        time.sleep(_wait_seconds)

    if not origin_path.exists():
        logger.warning(
            f'{dataset_name} - origin path disappeared during or before stability check: {origin_path}'
        )

    dataset_origin = (dataset.get('metadata') or {}).get('origin')
    if dataset_origin == 'legacy_xenium':
        # Check if dataset already exists in Xenium and persist Xenium ID.
        logger.info(f'{dataset_name} - origin=legacy_xenium; checking if dataset exists in Xenium by origin_path')
        try:
            xenium_entity = xenium_api.get_dataset_by_origin_path(origin_path_str)
            if xenium_entity:
                xenium_id = xenium_entity.get('id', xenium_entity.get('_id'))
                if xenium_id:
                    logger.info(f'{dataset_name} - found in Xenium with ID: {xenium_id}, persisting to database')
                    api.update_dataset(dataset_id=dataset_id, update_data={'xenium_id': str(xenium_id)})
                else:
                    logger.warning(f'{dataset_name} - found in Xenium but no id field present')
            else:
                logger.info(f'{dataset_name} - not found in Xenium, will proceed with standard archival')
        except Exception as e:
            # Don't fail the workflow if Xenium check fails - just log and continue
            logger.warning(f'{dataset_name} - error checking Xenium, will proceed with standard archival: {e}')
    else:
        # Check if dataset already exists in CMG and persist CMG ID.
        # This allows the archive step to coordinate with CMG's archival process.
        logger.info(f'{dataset_name} - checking if dataset exists in CMG by origin_path')
        try:
            if dataset_type == 'RAW_DATA':
                cmg_entity = cmg_api.get_dataset_by_origin_path(origin_path_str)
            elif dataset_type == 'DATA_PRODUCT':
                cmg_entity = cmg_api.get_dataproduct_by_origin_path(origin_path_str)
            else:
                logger.info(f'{dataset_name} - unknown dataset type {dataset_type}, skipping CMG check')
                cmg_entity = None

            if cmg_entity:
                cmg_id = cmg_entity.get('_id')
                if cmg_id:
                    logger.info(f'{dataset_name} - found in CMG with ID: {cmg_id}, persisting to database')
                    api.update_dataset(dataset_id=dataset_id, update_data={'cmg_id': str(cmg_id)})
                else:
                    logger.warning(f'{dataset_name} - found in CMG but no _id field present')
            else:
                logger.info(f'{dataset_name} - not found in CMG, will proceed with standard archival')
        except Exception as e:
            # Don't fail the workflow if CMG check fails - just log and continue
            logger.warning(f'{dataset_name} - error checking CMG, will proceed with standard archival: {e}')

    api.add_state_to_dataset(dataset_id=dataset_id, state='READY')
    return dataset_id,

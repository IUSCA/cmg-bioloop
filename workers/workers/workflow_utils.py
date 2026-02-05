from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path

# import multiprocessing
# https://stackoverflow.com/questions/30624290/celery-daemonic-processes-are-not-allowed-to-have-children
import billiard as multiprocessing
from sca_rhythm import WorkflowTask
from sca_rhythm.progress import Progress

from workers import sda, utils
from workers.config import config

logger = logging.getLogger(__name__)


# def make_task_name(task_name):
#     app_id = config['app_id']
#     return f'{app_id}.{task_name}'


def get_wf_body(wf_name: str) -> dict:
    wf_body = config['workflow_registry'][wf_name]
    wf_body['name'] = wf_body.get('name', wf_name)
    wf_body['app_id'] = config['app_id']
    default_queue = config.get('default_queue', f'{config["app_id"]}.q')
    for step in wf_body['steps']:
        if 'queue' not in step:
            step['queue'] = default_queue
    return wf_body


# legacy_dataset: True if the dataset is a legacy dataset (i.e. from the legacy CMG application).
def get_archive_dir(dataset_type: str,
                    legacy_dataset: bool = False) -> str:
    if legacy_dataset:
        sda_dir = config["paths"][dataset_type]["archive_legacy"]
    else:
        sda_dir = config["paths"][dataset_type]["archive"]
    sda.ensure_directory(sda_dir)  # create the directory if it does not exist
    return sda_dir


@contextmanager
def track_progress_parallel(celery_task: WorkflowTask,
                            name: str,
                            progress_fn,
                            total: int = None,
                            units: str = None,
                            loop_delay=5):
    def progress_loop():
        prog = Progress(celery_task=celery_task, name=name, total=total, units=units)
        while True:
            time.sleep(loop_delay)
            try:
                done = progress_fn()
                prog.update(done)
            except Exception as e:
                # log the exception message without stacktrace
                logger.warning('exception in parallel progress loop: %s', e)

    p = None
    try:
        # start a subprocess to call progress_loop every loop_delay seconds
        p = multiprocessing.Process(target=progress_loop)
        p.start()
        logger.info(f'starting a sub process to track progress with pid: {p.pid}')
        yield p  # inside the context manager
    finally:
        # terminate the sub process
        logger.info(f'terminating progress tracker: {p.pid}')
        if p is not None:
            p.terminate()


def upload_file_to_sda(local_file_path: Path,
                       sda_file_path: str,
                       *,
                       celery_task: WorkflowTask = None,
                       verify_checksum: bool = True,
                       preflight_check: bool = True) -> None:
    """

    @param local_file_path:
    @param sda_file_path:
    @param celery_task:
    @param verify_checksum:
    @param preflight_check:
    """
    local_digest = None
    sda_digest = None

    if preflight_check:
        sda_digest = sda.get_hash(sda_file_path, missing_ok=True)
        if sda_digest is not None:
            logger.info(f'computing checksum of local file {local_file_path} to compare with sda_digest')
            local_digest = utils.checksum(local_file_path)

    if sda_digest is not None and local_digest is not None and sda_digest == local_digest:
        logger.warning(f'The checksums of local file {local_file_path} and SDA file {sda_file_path} match - not '
                       f'uploading')
    else:
        if celery_task is not None:
            local_file_size = local_file_path.stat().st_size
            cm = track_progress_parallel(celery_task=celery_task,
                                         name='sda put',
                                         progress_fn=lambda: sda.get_size(sda_file_path),
                                         total=local_file_size,
                                         units='bytes')
        else:
            cm = utils.empty_context_manager()
        with cm:
            logging.info(f'putting {local_file_path} on SDA at {sda_file_path}')
            sda.put(local_file=str(local_file_path), sda_file=sda_file_path, verify_checksum=verify_checksum)


def download_file_from_sda(sda_file_path: str,
                           local_file_path: Path,
                           *,
                           celery_task: WorkflowTask = None,
                           verify_checksum: bool = True,
                           preflight_check: bool = False) -> None:
    """
    Before downloading, check if the file exists and the checksums match.
    If not, download from SDA and validate if the checksums match.
    @param sda_file_path:
    @param local_file_path:
    @param celery_task:
    @param verify_checksum:
    @param preflight_check:
    """
    file_exists = False

    if preflight_check:
        sda_digest = sda.get_hash(sda_path=sda_file_path)
        if local_file_path.exists() and local_file_path.is_file():
            # if local file exists, validate checksum against SDA
            logger.info(f'computing checksum of local file {local_file_path}')
            local_digest = utils.checksum(local_file_path)
            if sda_digest == local_digest:
                file_exists = True
                logger.warning(f'local file exists and the checksums match - not getting from the SDA')

    if not file_exists:
        logger.info('getting file from SDA')

        # delete the local file if possible
        local_file_path.unlink(missing_ok=True)

        if celery_task is not None:
            source_size = sda.get_size(sda_file_path)
            cm = track_progress_parallel(celery_task=celery_task,
                                         name='sda get',
                                         progress_fn=lambda: local_file_path.stat().st_size,
                                         total=source_size,
                                         units='bytes')
        else:
            cm = utils.empty_context_manager()
        with cm:
            logger.info(f'getting file from SDA {sda_file_path} to {local_file_path}')
            sda.get(sda_file=sda_file_path, local_file=str(local_file_path), verify_checksum=verify_checksum)


def wait_for_sda_upload(sda_file_path: str,
                       expected_checksum: str = None,
                       *,
                       celery_task: WorkflowTask = None,
                       poll_interval_seconds: int = 300,
                       timeout_seconds: int = 86400) -> None:
    """
    Wait for a file to finish uploading to SDA (HSI-based tape storage).
    
    This function is used during legacy migration when the CMG application
    is uploading files to SDA concurrently. This worker waits for the upload
    to complete before proceeding.
    
    HSI Version: hsi.10.3.0.p3
    
    Implementation Strategy:
    1. Poll SDA at regular intervals to check if file exists
    2. Once file exists, verify it's complete by checking:
       - File size is stable between checks
       - Checksum matches expected value (if provided)
    3. Update progress periodically via celery_task
    4. Timeout if file doesn't appear within timeout_seconds
    
    Args:
        sda_file_path: Path to file in SDA (e.g., '/path/on/sda/dataset.tar')
        expected_checksum: Expected MD5 checksum of the file (optional)
        celery_task: Celery task for progress tracking (optional)
        poll_interval_seconds: How often to check SDA (default: 300 seconds / 5 minutes)
        timeout_seconds: Maximum time to wait (default: 86400 seconds / 24 hours)
    
    Raises:
        TimeoutError: If file doesn't appear or complete within timeout_seconds
        Exception: If checksum verification fails
    
    PLACEHOLDER: Implementation to be completed with actual HSI commands
    """
    logger.info(f'Waiting for SDA upload to complete: {sda_file_path}')
    logger.info(f'Poll interval: {poll_interval_seconds}s, Timeout: {timeout_seconds}s')
    
    # TODO: Implement actual waiting logic using HSI commands
    # Suggested implementation:
    # 1. Use sda.exists() to check if file exists
    # 2. Use sda.get_size() to check if size is stable
    # 3. Use sda.get_hash() to verify checksum
    # 4. Track elapsed time and raise TimeoutError if exceeded
    # 5. Update celery_task progress periodically
    
    raise NotImplementedError(
        'wait_for_sda_upload is a placeholder. Implementation required:\n'
        '1. Poll SDA using HSI commands at regular intervals\n'
        '2. Check file existence with sda.exists()\n'
        '3. Verify file size stability with sda.get_size()\n'
        '4. Verify checksum with sda.get_hash() if expected_checksum provided\n'
        '5. Update progress via celery_task.update_progress()\n'
        '6. Raise TimeoutError if timeout_seconds exceeded'
    )

"""
Test Utilities for Upload Verification Tests

Shared helper functions for creating test data, computing manifests,
and interacting with the upload verification system.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_manifest_hash(files_dict):
    """
    Compute BLAKE3 manifest hash from files (same algorithm as client-side).
    
    Args:
        files_dict: Dict with file info from test_files fixture
                   {'filename': {'path': Path, 'hash': str, 'size': int}}
    
    Returns:
        str: Hex hash of the manifest
    """
    import blake3
    
    manifest_lines = ['blake3-manifest-v1']
    
    # Sort files alphabetically (same as client)
    for filename in sorted(files_dict.keys()):
        file_info = files_dict[filename]
        manifest_lines.append(
            f"{filename}\t{file_info['size']}\t{file_info['hash']}"
        )
    
    manifest_str = '\n'.join(manifest_lines)
    manifest_hash = blake3.blake3(manifest_str.encode('utf-8')).hexdigest()
    
    logger.info(f"Computed manifest hash: {manifest_hash}")
    logger.info(f"Files in manifest: {list(files_dict.keys())}")
    
    return manifest_hash


def wait_for_status(api_client, dataset_id, expected_status, timeout=30, poll_interval=1):
    """
    Poll upload_log until status changes to expected value or timeout.
    
    Args:
        api_client: API module
        dataset_id: Dataset ID to check
        expected_status: Expected status string
        timeout: Max seconds to wait
        poll_interval: Seconds between polls
    
    Returns:
        dict: Final upload_log state
    
    Raises:
        TimeoutError: If status doesn't change within timeout
    """
    logger.info(f"Waiting for upload status to be {expected_status} (timeout: {timeout}s)...")
    
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < timeout:
        try:
            upload_log = api_client.get_dataset_upload_log(dataset_id)
            current_status = upload_log.get('status')
            
            if current_status != last_status:
                logger.info(f"  Status: {current_status}")
                last_status = current_status
            
            if current_status == expected_status:
                elapsed = time.time() - start_time
                logger.info(f"✓ Status reached {expected_status} after {elapsed:.1f}s")
                return upload_log
            
        except Exception as e:
            logger.warning(f"Error polling status: {e}")
        
        time.sleep(poll_interval)
    
    elapsed = time.time() - start_time
    logger.error(f"✗ Timeout waiting for status {expected_status} after {elapsed:.1f}s")
    logger.error(f"  Last status: {last_status}")
    raise TimeoutError(f"Status did not reach {expected_status} within {timeout}s (last: {last_status})")


def wait_for_celery_task_state(celery_app, task_id, expected_state, timeout=30, poll_interval=1):
    """
    Poll Celery task state until it reaches expected state or timeout.
    
    Args:
        celery_app: Celery app instance
        task_id: Task ID to check
        expected_state: Expected state string (SUCCESS, FAILURE, etc.)
        timeout: Max seconds to wait
        poll_interval: Seconds between polls
    
    Returns:
        str: Final task state
    
    Raises:
        TimeoutError: If state doesn't change within timeout
    """
    logger.info(f"Waiting for Celery task {task_id} to be {expected_state} (timeout: {timeout}s)...")
    
    start_time = time.time()
    last_state = None
    
    while time.time() - start_time < timeout:
        try:
            task_result = celery_app.AsyncResult(task_id)
            current_state = task_result.state
            
            if current_state != last_state:
                logger.info(f"  Task state: {current_state}")
                last_state = current_state
            
            if current_state == expected_state:
                elapsed = time.time() - start_time
                logger.info(f"✓ Task reached {expected_state} after {elapsed:.1f}s")
                return current_state
            
        except Exception as e:
            logger.warning(f"Error polling task state: {e}")
        
        time.sleep(poll_interval)
    
    elapsed = time.time() - start_time
    logger.error(f"✗ Timeout waiting for task state {expected_state} after {elapsed:.1f}s")
    logger.error(f"  Last state: {last_state}")
    raise TimeoutError(f"Task state did not reach {expected_state} within {timeout}s (last: {last_state})")


def set_upload_status(api_client, dataset_id, status, metadata=None):
    """
    Manually set upload_log status (for simulating failure scenarios).
    
    Args:
        api_client: API module
        dataset_id: Dataset ID
        status: Status to set
        metadata: Optional metadata dict to merge
    """
    logger.info(f"Setting upload status to {status} for dataset {dataset_id}")
    
    update_data = {'status': status}
    if metadata:
        update_data['metadata'] = metadata
    
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data=update_data
    )
    
    logger.info(f"✓ Status set to {status}")


def set_upload_timestamp(api_client, dataset_id, updated_at):
    """
    Manually set upload_log updated_at timestamp (for simulating stale/hung scenarios).
    
    Args:
        api_client: API module
        dataset_id: Dataset ID
        updated_at: datetime object to set
    """
    logger.info(f"Setting upload timestamp to {updated_at} for dataset {dataset_id}")
    
    # Note: This requires direct DB access or API endpoint that allows timestamp override
    # For now, we'll use metadata to track "simulated_timestamp" and check in tests
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'metadata': {
                'test_simulated_timestamp': updated_at.isoformat(),
            }
        }
    )
    
    logger.info(f"✓ Simulated timestamp set")


def get_worker_process_logs(api_client, worker_process_id):
    """
    Fetch worker_process logs from API.
    
    Args:
        api_client: API module
        worker_process_id: Worker process ID
    
    Returns:
        list: List of log entries
    """
    import requests
    from workers.config import config
    
    url = f"{config['api_base_url']}/workflows/processes/{worker_process_id}/logs"
    token = os.environ.get('APP_API_TOKEN')
    
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    response.raise_for_status()
    
    logs = response.json()
    logger.info(f"Fetched {len(logs)} log entries for worker_process {worker_process_id}")
    
    return logs


def assert_logs_contain(logs, expected_text):
    """
    Assert that worker logs contain expected text.
    
    Args:
        logs: List of log entries
        expected_text: Text to search for
    
    Raises:
        AssertionError: If text not found
    """
    messages = [log.get('message', '') for log in logs]
    full_log = '\n'.join(messages)
    
    if expected_text not in full_log:
        logger.error(f"Expected text not found in logs: {expected_text}")
        logger.error(f"Actual logs:\n{full_log}")
        raise AssertionError(f"Expected text '{expected_text}' not found in worker logs")
    
    logger.info(f"✓ Found expected text in logs: {expected_text}")


def corrupt_file_hash(test_files, filename):
    """
    Corrupt a file's hash in the test_files dict to simulate checksum mismatch.
    
    Args:
        test_files: Dict from test_files fixture
        filename: File to corrupt
    """
    if filename in test_files:
        original_hash = test_files[filename]['hash']
        # Flip last character of hash
        corrupted_hash = original_hash[:-1] + ('0' if original_hash[-1] != '0' else '1')
        test_files[filename]['hash'] = corrupted_hash
        logger.info(f"Corrupted hash for {filename}")
        logger.info(f"  Original: {original_hash}")
        logger.info(f"  Corrupted: {corrupted_hash}")

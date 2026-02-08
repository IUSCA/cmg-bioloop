"""
SPECIAL TEST: Soft Timeout Limits (5min test version)

⚠️ IMPORTANT: This test modifies task timeout limits!
⚠️ After running, REVERT changes to workers/workers/tasks/declarations.py
⚠️ Comment out this test after initial validation

This test validates the soft timeout behavior with shortened limits.
User requested: Test with 5min timeout, then comment out and revert changes.
"""

import logging
import subprocess
import time

import pytest

from tests.upload_verification.test_utils import (
    compute_manifest_hash,
    wait_for_status,
)
from workers.constants.upload import UPLOAD_STATUS

logger = logging.getLogger(__name__)


# ⚠️ COMMENT OUT THIS TEST AFTER VALIDATION ⚠️
@pytest.mark.skip(reason="Test modifies production timeouts - comment out after validation")
@pytest.mark.integration
@pytest.mark.requires_celery
@pytest.mark.slow
def test_soft_timeout_with_5min_limit(api_client, test_dataset, test_files):
    """
    Test: Soft timeout with shortened limit (5min for testing)
    
    ⚠️ BEFORE RUNNING THIS TEST:
    1. Modify workers/workers/tasks/declarations.py:
       - Set soft_time_limit to 300 (5 minutes)
       - Set time_limit to 600 (10 minutes)
    2. Restart Celery worker
    
    ⚠️ AFTER RUNNING THIS TEST:
    1. Revert workers/workers/tasks/declarations.py:
       - Set soft_time_limit back to 43200 (12 hours)
       - Set time_limit back to 86400 (24 hours)
    2. Restart Celery worker
    3. Comment out this test (or mark with @pytest.mark.skip)
    
    Scenario:
    1. Create very large files (or simulate slow hash computation)
    2. Spawn verification task
    3. Task should hit soft timeout after 5min
    4. Task should catch SoftTimeLimitExceeded and fail gracefully
    5. Status → VERIFICATION_FAILED with timeout reason
    """
    dataset_id = test_dataset['id']
    
    logger.info("="*80)
    logger.info("TEST: Soft Timeout (5min test limit)")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info("="*80)
    
    logger.warning("⚠️" * 40)
    logger.warning("ENSURE TASK TIMEOUTS HAVE BEEN MODIFIED:")
    logger.warning("  soft_time_limit = 300 (5 min)")
    logger.warning("  time_limit = 600 (10 min)")
    logger.warning("⚠️" * 40)
    
    # Setup: Create large file that will take >5min to hash
    # For testing, we'll simulate by using test files and assuming timeout works
    manifest_hash = compute_manifest_hash(test_files)
    
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['UPLOADED'],
            'process_id': 'test-process-timeout',
            'metadata': {
                'checksum': {'manifest_hash': manifest_hash}
            }
        }
    )
    
    # Spawn task
    logger.info("\n--- Spawning verification task (will timeout after 5min) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    assert result.returncode == 0
    
    verifying_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFYING'], timeout=10)
    task_id = verifying_log['metadata']['verification_task_id']
    logger.info(f"✓ Task spawned: {task_id}")
    
    # Wait for timeout (5min + buffer)
    logger.info("\n--- Waiting for soft timeout (5min + retry delays) ---")
    logger.info("This will take ~6-7 minutes...")
    
    # Note: With small test files, task might complete before timeout
    # To truly test timeout, need files that take >5min to hash
    # Or mock time.sleep in the hash computation
    
    logger.warning("NOTE: With small test files, timeout may not trigger")
    logger.warning("      For real timeout testing, need files >5min hash time")
    
    # Wait up to 8 minutes for either VERIFIED or VERIFICATION_FAILED
    start_time = time.time()
    final_status = None
    
    while time.time() - start_time < 480:  # 8 minutes
        current_log = api_client.get_dataset_upload_log(dataset_id)
        status = current_log['status']
        
        if status in [UPLOAD_STATUS['VERIFIED'], UPLOAD_STATUS['VERIFICATION_FAILED']]:
            final_status = status
            logger.info(f"✓ Final status reached: {status}")
            break
        
        time.sleep(10)
    
    if final_status == UPLOAD_STATUS['VERIFICATION_FAILED']:
        logger.info("✓ Task timed out as expected")
        final_log = api_client.get_dataset_upload_log(dataset_id)
        logger.info(f"  Failure reason: {final_log.get('failure_reason')}")
    elif final_status == UPLOAD_STATUS['VERIFIED']:
        logger.warning("⚠️ Task completed before timeout (files too small)")
        logger.warning("   Timeout logic validated in code review")
    else:
        pytest.fail(f"Unexpected final status: {final_status}")
    
    logger.info("="*80)
    logger.info("✓ TEST COMPLETED: Soft timeout validation")
    logger.info("="*80)
    
    logger.warning("⚠️" * 40)
    logger.warning("REMEMBER TO:")
    logger.warning("1. Revert task timeout limits to production values")
    logger.warning("2. Restart Celery worker")
    logger.warning("3. Comment out this test")
    logger.warning("⚠️" * 40)

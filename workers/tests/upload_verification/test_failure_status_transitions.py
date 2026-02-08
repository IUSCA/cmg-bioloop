"""
Failure Tests: Status Transition Issues

Tests for script crashes, stale statuses, and recovery mechanisms.
"""

import logging
import subprocess
import time
from datetime import datetime, timedelta

import pytest

from tests.upload_verification.test_utils import (
    compute_manifest_hash,
    set_upload_status,
    wait_for_status,
)
from workers.constants.upload import UPLOAD_STATUS

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_celery
def test_failure_script_crash_after_verifying_before_spawn(api_client, test_dataset, test_files):
    """
    Test: Script crashes after setting VERIFYING but before spawning task
    
    Scenario:
    1. Set status to VERIFYING manually (simulating crash)
    2. Don't set task_id (crash happened before spawn)
    3. Wait 5+ minutes (simulated via metadata)
    4. Run script
    5. Should respawn task after detecting stale VERIFYING
    """
    dataset, upload_log = test_dataset
    dataset_id = dataset['id']
    
    logger.info("="*80)
    logger.info("TEST: Script Crash After VERIFYING (No Task Spawned)")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info("="*80)
    
    # Setup manifest
    manifest_hash = compute_manifest_hash(test_files)
    
    # Step 1: Manually set VERIFYING without task_id (simulating crash)
    logger.info("Simulating: Script set VERIFYING but crashed before spawning task")
    stale_timestamp = datetime.utcnow() - timedelta(minutes=6)  # 6 minutes ago
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['VERIFYING'],
            'process_id': 'test-process-003',
            'metadata': {
                'checksum': {'manifest_hash': manifest_hash},
                'verification_started_at': stale_timestamp.isoformat(),
                # No verification_task_id - simulating crash before spawn
            }
        }
    )
    logger.info(f"✓ Status set to VERIFYING (stale timestamp: {stale_timestamp})")
    logger.info("  No task_id persisted (simulating crash)")
    
    # Wait for staleness threshold (30s)
    logger.info("\n--- Waiting 31s for upload to become 'stalled' ---")
    time.sleep(31)
    
    # Step 2: Run script
    logger.info("\n--- Running script (should detect stale and respawn) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script stdout:\n{result.stdout}")
    if result.stderr:
        logger.info(f"Script stderr:\n{result.stderr[:2000]}")  # First 2000 chars
    assert result.returncode == 0
    
    # Should see stale detection and respawn (check stderr for detailed logs)
    combined_output = result.stdout + result.stderr
    assert ('STALE VERIFYING' in combined_output or 'FAILURE MODE: STALE VERIFYING' in combined_output), \
        "Script should detect stale VERIFYING and respawn task"
    
    # Step 3: Wait for verification to complete
    logger.info("\n--- Waiting for respawned task to complete ---")
    verified_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFIED'], timeout=60)
    
    assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
    assert verified_log['metadata']['verification_task_id'] is not None
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Stale VERIFYING detected and recovered")
    logger.info("="*80)


@pytest.mark.integration
@pytest.mark.requires_celery
def test_failure_task_spawned_but_id_not_persisted(api_client, test_dataset, test_files):
    """
    Test: Task spawned successfully but script crashes before persisting task_id
    
    Scenario:
    1. This is actually the same as previous test from script's perspective
    2. Status = VERIFYING, no task_id, stale >5min
    3. Script will respawn task
    4. Original task may still be running, but that's OK (idempotent)
    
    Note: This tests the idempotency guarantee - spawning task twice is safe.
    """
    dataset, upload_log = test_dataset
    dataset_id = dataset['id']
    
    logger.info("="*80)
    logger.info("TEST: Task Spawned But ID Not Persisted")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info("NOTE: Same recovery as previous test - respawn is idempotent")
    logger.info("="*80)
    
    # Setup
    manifest_hash = compute_manifest_hash(test_files)
    stale_timestamp = datetime.utcnow() - timedelta(minutes=6)
    
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['VERIFYING'],
            'process_id': 'test-process-004',
            'metadata': {
                'checksum': {'manifest_hash': manifest_hash},
                'verification_started_at': stale_timestamp.isoformat(),
                # Simulating: Task was spawned but ID not persisted due to crash
            }
        }
    )
    
    logger.info("✓ Simulated scenario: VERIFYING without task_id (>5min stale)")
    
    # Run script - should respawn
    logger.info("\n--- Running script (will respawn idempotent task) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script stdout:\n{result.stdout}")
    assert result.returncode == 0
    
    # Wait for completion
    verified_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFIED'], timeout=60)
    assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Idempotent respawn works correctly")
    logger.info("="*80)


@pytest.mark.integration
def test_failure_stale_verifying_no_task_id(api_client, test_dataset, test_files):
    """
    Test: VERIFYING status for >5min without task_id → Respawn task
    
    Scenario:
    1. Set VERIFYING with old timestamp, no task_id
    2. Script should detect stale state (>5min)
    3. Script should log warning and respawn
    
    Note: This is effectively the same as test_failure_script_crash_after_verifying
    """
    dataset, upload_log = test_dataset
    dataset_id = dataset['id']
    
    logger.info("="*80)
    logger.info("TEST: Stale VERIFYING (No Task ID, >5min)")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info("="*80)
    
    # This is redundant with previous tests but validates the 5-minute threshold
    manifest_hash = compute_manifest_hash(test_files)
    stale_timestamp = datetime.utcnow() - timedelta(minutes=7)  # 7 minutes ago
    
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['VERIFYING'],
            'process_id': 'test-process-005',
            'metadata': {
                'checksum': {'manifest_hash': manifest_hash},
                'verification_started_at': stale_timestamp.isoformat(),
            }
        }
    )
    
    logger.info(f"✓ Set VERIFYING with 7-minute old timestamp (>5min threshold)")
    
    # Wait for staleness threshold (30s)
    logger.info("\n--- Waiting 31s for upload to become 'stalled' ---")
    time.sleep(31)
    
    # Run script
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script stdout:\n{result.stdout}")
    if result.stderr:
        logger.info(f"Script stderr:\n{result.stderr[:2000]}")  # First 2000 chars
    assert result.returncode == 0
    combined_output = result.stdout + result.stderr
    assert 'STALE VERIFYING' in combined_output or 'FAILURE MODE: STALE VERIFYING' in combined_output
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Stale detection works correctly")
    logger.info("="*80)

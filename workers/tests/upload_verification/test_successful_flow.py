"""
Successful Flow Tests for Upload Verification

Tests the successful flow: UPLOADED → VERIFYING → VERIFIED → COMPLETE
"""

import logging
import subprocess
import time

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
def test_upload_verification_with_checksum_enabled(api_client, test_dataset, test_files):
    """
    Test: UPLOADED → VERIFYING → VERIFIED → COMPLETE (checksum verification enabled)
    
    Flow:
    1. Create dataset with uploaded files
    2. Compute manifest hash and store in upload_log metadata
    3. Run manage_upload_workflows.py script
    4. Script should:
       - Set status to VERIFYING
       - Spawn Celery task
       - Persist task_id
    5. Wait for task to complete (status → VERIFIED)
    6. Run script again
    7. Script should trigger integrated workflow (status → COMPLETE)
    """
    dataset, upload_log = test_dataset
    dataset_id = dataset['id']
    dataset_name = dataset['name']
    
    logger.info("="*80)
    logger.info("TEST: Happy Path with Checksum Enabled")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info(f"Dataset name: {dataset_name}")
    logger.info(f"Files: {list(test_files.keys())}")
    logger.info("="*80)
    
    # Step 1: Compute manifest hash and create upload_log via /complete endpoint simulation
    # In production, /complete creates upload_log. For tests, we call update_dataset_upload_log directly.
    manifest_hash = compute_manifest_hash(test_files)
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['UPLOADED'],
            'process_id': 'test-process-001',
            'metadata': {
                'checksum': {
                    'manifest_hash': manifest_hash,
                    'files': [
                        {
                            'relativePath': filename,
                            'size': info['size'],
                            'hash': info['hash'],
                        }
                        for filename, info in test_files.items()
                    ]
                }
            }
        }
    )
    logger.info("✓ Upload log configured with checksum metadata")
    
    # Step 2: Run script (first run: spawn verification task)
    logger.info("\n--- Running manage_upload_workflows.py (spawn verification) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script exit code: {result.returncode}")
    logger.info(f"Script stdout:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"Script stderr:\n{result.stderr}")
    
    assert result.returncode == 0, "Script should exit successfully"
    assert 'Spawning verification task' in result.stdout or 'verification_spawned' in result.stdout
    
    # Step 3: Wait for verification to complete (status → VERIFIED)
    logger.info("\n--- Waiting for verification task to complete ---")
    verified_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFIED'], timeout=60)
    
    assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
    assert verified_log['metadata']['verification_task_id'] is not None
    
    task_id = verified_log['metadata']['verification_task_id']
    logger.info(f"✓ Verification completed, task_id: {task_id}")
    
    # Wait for stalled threshold (30s) before script picks up VERIFIED upload
    logger.info("\n--- Waiting 31s for upload to become 'stalled' (updated_at > 30s ago) ---")
    time.sleep(31)
    
    # Step 4: Run script again (second run: trigger workflow)
    logger.info("\n--- Running manage_upload_workflows.py (trigger workflow) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script exit code: {result.returncode}")
    logger.info(f"Script stdout:\n{result.stdout}")
    
    assert result.returncode == 0
    assert 'verified_triggered' in result.stdout or 'Starting integrated workflow' in result.stdout
    
    # Step 5: Verify final status is COMPLETE
    logger.info("\n--- Verifying final status ---")
    final_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['COMPLETE'], timeout=10)
    
    assert final_log['status'] == UPLOAD_STATUS['COMPLETE']
    logger.info("✓ Upload reached COMPLETE status")
    
    # Step 6: Verify workflow was created
    dataset = api_client.get_dataset(dataset_id=dataset_id, workflows=True)
    workflows = dataset.get('workflows', [])
    integrated_wf = [wf for wf in workflows if wf['name'] == 'integrated']
    
    assert len(integrated_wf) > 0, "Integrated workflow should be created"
    wf_id = integrated_wf[0].get('_id') or integrated_wf[0].get('id')
    logger.info(f"✓ Integrated workflow created: {wf_id}")
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Upload verification with checksum enabled")
    logger.info("="*80)


@pytest.mark.integration
@pytest.mark.requires_celery
def test_upload_verification_with_checksum_disabled(api_client, test_dataset, test_files):
    """
    Test: UPLOADED → VERIFYING → VERIFIED → COMPLETE (checksum disabled, file existence only)
    
    Flow:
    1. Create dataset with uploaded files (no checksum metadata)
    2. Run manage_upload_workflows.py script
    3. Verification should use fallback _verify_files_exist()
    4. Should complete successfully
    """
    dataset, upload_log = test_dataset
    dataset_id = dataset['id']
    dataset_name = dataset['name']
    
    logger.info("="*80)
    logger.info("TEST: Checksum Disabled (File Existence Only)")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info(f"Dataset name: {dataset_name}")
    logger.info("="*80)
    
    # Step 1: Set upload status to UPLOADED (no checksum metadata)
    # When metadata is empty/missing, verification will use fallback file existence check
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['UPLOADED'],
            'process_id': 'test-process-002',
            'metadata': {},  # Empty metadata = no checksum = fallback verification
        }
    )
    logger.info("✓ Upload log configured WITHOUT checksum (fallback mode)")
    
    # Wait for stalled threshold before script picks it up
    logger.info("\n--- Waiting 31s for upload to become 'stalled' ---")
    time.sleep(31)
    
    # Step 2: Run script (spawn verification)
    logger.info("\n--- Running manage_upload_workflows.py ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script stdout:\n{result.stdout}")
    assert result.returncode == 0
    
    # Step 3: Wait for verification (should be fast - just file existence check)
    logger.info("\n--- Waiting for verification (file existence check) ---")
    verified_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFIED'], timeout=30)
    
    assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
    logger.info("✓ Verification passed (file existence check)")
    
    # Wait for stalled threshold
    logger.info("\n--- Waiting 31s for upload to become 'stalled' ---")
    time.sleep(31)
    
    # Step 4: Run script again to trigger workflow
    logger.info("\n--- Running script again (trigger workflow) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    assert result.returncode == 0
    
    # Step 5: Verify final status
    final_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['COMPLETE'], timeout=10)
    assert final_log['status'] == UPLOAD_STATUS['COMPLETE']
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Upload verification with checksum disabled")
    logger.info("="*80)

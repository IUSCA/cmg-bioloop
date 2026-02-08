"""
Edge Case Tests for Upload Verification

Tests for multiple files, concurrent uploads, and other edge scenarios.
"""

import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests.upload_verification.test_utils import (
    compute_manifest_hash,
    wait_for_status,
)
from workers.constants.upload import UPLOAD_STATUS

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_celery
def test_edge_case_multiple_files_manifest(api_client, test_data_dir):
    """
    Test: Upload with many files (manifest with 10+ entries)
    
    Scenario:
    1. Create 10 files of varying sizes
    2. Compute manifest hash
    3. Spawn verification
    4. Verify all files are checked correctly
    """
    logger.info("="*80)
    logger.info("TEST: Multiple Files Manifest (10 files)")
    logger.info("="*80)
    
    # Create dataset with multiple files
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    dataset_name = f'test-multi-files-{timestamp}'
    upload_dir = test_data_dir / f'upload_{timestamp}'
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Create 10 files
    import blake3
    files_dict = {}
    
    for i in range(10):
        filename = f'file_{i:02d}.dat'
        filepath = upload_dir / filename
        
        # Vary file sizes (1KB to 10KB)
        size = (i + 1) * 1024
        content = os.urandom(size)
        filepath.write_bytes(content)
        
        file_hash = blake3.blake3(content).hexdigest()
        files_dict[filename] = {
            'path': filepath,
            'size': size,
            'hash': file_hash,
            'content': content,
        }
    
    logger.info(f"✓ Created {len(files_dict)} test files")
    total_size = sum(f['size'] for f in files_dict.values())
    logger.info(f"  Total size: {total_size / 1024:.1f} KB")
    
    # Create dataset with upload_log via /datasets/uploads endpoint
    import requests
    token = os.environ.get('APP_API_TOKEN')
    api_url = os.environ.get('API_BASE_URL', 'http://api:3030')
    
    response = requests.post(
        f"{api_url}/datasets/uploads",
        json={
            'name': dataset_name,
            'type': 'DATA_PRODUCT',
        },
        headers={'Authorization': f'Bearer {token}'}
    )
    response.raise_for_status()
    upload_log = response.json()
    dataset = upload_log['audit_log']['dataset']
    dataset_id = dataset['id']
    
    # Update origin_path to point to our test directory
    requests.patch(
        f"{api_url}/datasets/{dataset_id}",
        json={'origin_path': str(upload_dir)},
        headers={'Authorization': f'Bearer {token}'}
    )
    logger.info(f"✓ Dataset + upload_log created: {dataset_id}")
    
    # Compute manifest
    manifest_hash = compute_manifest_hash(files_dict)
    
    # Set upload log
    api_client.update_dataset_upload_log(
        dataset_id=dataset_id,
        log_data={
            'status': UPLOAD_STATUS['UPLOADED'],
            'process_id': 'test-multi-files',
            'metadata': {
                'checksum': {
                    'manifest_hash': manifest_hash,
                    'files': [
                        {
                            'relativePath': filename,
                            'size': info['size'],
                            'hash': info['hash'],
                        }
                        for filename, info in files_dict.items()
                    ]
                }
            }
        }
    )
    logger.info("✓ Upload log configured")
    
    # Wait for staleness threshold
    logger.info("\n--- Waiting 31s for upload to become 'stalled' ---")
    time.sleep(31)
    
    # Run script
    logger.info("\n--- Running verification script ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    assert result.returncode == 0
    
    # Wait for verification
    logger.info("\n--- Waiting for verification (10 files) ---")
    verified_log = wait_for_status(api_client, dataset_id, UPLOAD_STATUS['VERIFIED'], timeout=120)
    
    assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
    logger.info("✓ Verification passed for all files")
    
    # Cleanup
    api_client.delete_dataset(dataset_id)
    import shutil
    shutil.rmtree(upload_dir)
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Multiple files verified correctly")
    logger.info("="*80)


@pytest.mark.integration
@pytest.mark.requires_celery
@pytest.mark.slow
def test_edge_case_concurrent_uploads_processed(api_client, test_data_dir):
    """
    Test: Multiple concurrent uploads processed independently
    
    Scenario:
    1. Create 3 uploads simultaneously
    2. Run script
    3. Script should spawn 3 verification tasks
    4. All should complete independently
    5. No race conditions or conflicts
    """
    logger.info("="*80)
    logger.info("TEST: Concurrent Uploads Processing")
    logger.info("="*80)
    
    # Create 3 concurrent uploads
    from datetime import datetime
    import blake3
    
    datasets = []
    
    for i in range(3):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        dataset_name = f'test-concurrent-{i}-{timestamp}'
        upload_dir = test_data_dir / f'upload_{i}_{timestamp}'
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        # Create small file
        filename = f'file_{i}.txt'
        filepath = upload_dir / filename
        content = f'Concurrent upload test {i}\n'.encode() * 100
        filepath.write_bytes(content)
        file_hash = blake3.blake3(content).hexdigest()
        
        # Create dataset with upload_log via /datasets/uploads endpoint
        import requests
        token = os.environ.get('APP_API_TOKEN')
        api_url = os.environ.get('API_BASE_URL', 'http://api:3030')
        
        response = requests.post(
            f"{api_url}/datasets/uploads",
            json={
                'name': dataset_name,
                'type': 'DATA_PRODUCT',
            },
            headers={'Authorization': f'Bearer {token}'}
        )
        response.raise_for_status()
        upload_log = response.json()
        dataset = upload_log['audit_log']['dataset']
        
        # Update origin_path to point to our test directory
        requests.patch(
            f"{api_url}/datasets/{dataset['id']}",
            json={'origin_path': str(upload_dir)},
            headers={'Authorization': f'Bearer {token}'}
        )
        
        # Compute manifest
        files_dict = {
            filename: {
                'path': filepath,
                'size': len(content),
                'hash': file_hash,
            }
        }
        manifest_hash = compute_manifest_hash(files_dict)
        
        # Set upload log
        api_client.update_dataset_upload_log(
            dataset_id=dataset['id'],
            log_data={
                'status': UPLOAD_STATUS['UPLOADED'],
                'process_id': f'test-concurrent-{i}',
                'metadata': {
                    'checksum': {
                        'manifest_hash': manifest_hash,
                        'files': [{
                            'relativePath': filename,
                            'size': len(content),
                            'hash': file_hash,
                        }]
                    }
                }
            }
        )
        
        datasets.append({
            'id': dataset['id'],
            'name': dataset_name,
            'dir': upload_dir,
        })
        
        logger.info(f"✓ Created upload {i+1}/3: {dataset['id']}")
        time.sleep(0.5)  # Small delay to ensure different timestamps
    
    # Wait for staleness threshold
    logger.info("\n--- Waiting 31s for uploads to become 'stalled' ---")
    time.sleep(31)
    
    # Run script once - should handle all 3
    logger.info("\n--- Running script (should process 3 uploads) ---")
    result = subprocess.run(
        ['python', '-m', 'workers.scripts.manage_upload_workflows', '--dry-run=False'],
        capture_output=True,
        text=True,
        cwd='/opt/sca/app'
    )
    
    logger.info(f"Script stdout:\n{result.stdout}")
    assert result.returncode == 0
    
    # Wait for all 3 to reach VERIFIED
    logger.info("\n--- Waiting for all 3 uploads to verify ---")
    
    for i, ds in enumerate(datasets):
        logger.info(f"\nChecking upload {i+1}/3: {ds['id']}")
        verified_log = wait_for_status(
            api_client,
            ds['id'],
            UPLOAD_STATUS['VERIFIED'],
            timeout=90
        )
        assert verified_log['status'] == UPLOAD_STATUS['VERIFIED']
        logger.info(f"✓ Upload {i+1} verified")
    
    # Cleanup
    logger.info("\n--- Cleaning up ---")
    for ds in datasets:
        api_client.delete_dataset(ds['id'])
        import shutil
        shutil.rmtree(ds['dir'])
    
    logger.info("="*80)
    logger.info("✓ TEST PASSED: Concurrent uploads processed independently")
    logger.info("="*80)

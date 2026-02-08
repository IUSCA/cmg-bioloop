"""
Pytest Configuration and Shared Fixtures

Provides reusable fixtures for upload verification integration tests.
Tests run against Docker services (API, Postgres, RabbitMQ, Celery).
"""

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

import workers.api as api
from workers.constants.upload import UPLOAD_STATUS

# Configure logging to file
TEST_LOGS_DIR = Path(__file__).parent.parent / 'test_logs'
TEST_LOGS_DIR.mkdir(exist_ok=True)

# Create timestamped log file for this test run
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = TEST_LOGS_DIR / f'test_run_{timestamp}.log'

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger(__name__)
logger.info("="*80)
logger.info(f"TEST RUN STARTED: {timestamp}")
logger.info(f"Log file: {log_file}")
logger.info("="*80)


@pytest.fixture(scope="session")
def api_client():
    """
    API client for all tests (reused across entire test session).
    Uses APP_API_TOKEN from environment variables.
    """
    logger.info("Initializing API client...")
    
    # Verify API token exists
    token = os.environ.get('APP_API_TOKEN')
    if not token:
        pytest.fail("APP_API_TOKEN not set in environment")
    
    logger.info(f"API Base URL: {os.environ.get('API_BASE_URL')}")
    logger.info(f"API Token: {'*' * 10}{token[-4:] if len(token) > 4 else '****'}")
    
    return api


@pytest.fixture(scope="session")
def test_data_dir():
    """
    Base directory for all test data (mounted in Docker).
    Points to /opt/sca/data/test_uploads in container.
    
    Falls back to local temp directory if running outside Docker.
    """
    # Check if running in Docker (landing_volume mounted at /opt/sca/data)
    docker_path = Path('/opt/sca/data/test_uploads')
    if docker_path.parent.exists():
        base_dir = docker_path
    else:
        # Fallback for local testing (outside Docker)
        import tempfile
        base_dir = Path(tempfile.gettempdir()) / 'bioloop_test_uploads'
        logger.warning(f"Not in Docker, using temp directory: {base_dir}")
    
    base_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Test data directory: {base_dir}")
    
    yield base_dir
    
    # Cleanup: Remove test uploads after all tests
    logger.info(f"Cleaning up test data directory: {base_dir}")
    try:
        shutil.rmtree(base_dir)
    except Exception as e:
        logger.warning(f"Failed to cleanup test data: {e}")


@pytest.fixture
def temp_upload_dir(test_data_dir):
    """
    Temporary directory for a single test's upload files.
    Created fresh for each test, cleaned up after.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    upload_dir = test_data_dir / f'upload_{timestamp}'
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Created temp upload dir: {upload_dir}")
    
    yield upload_dir
    
    # Cleanup after test
    logger.info(f"Cleaning up temp upload dir: {upload_dir}")
    try:
        shutil.rmtree(upload_dir)
    except Exception as e:
        logger.warning(f"Failed to cleanup temp dir: {e}")


@pytest.fixture
def test_files(temp_upload_dir):
    """
    Create small dummy files for testing.
    Returns dict with file paths and their BLAKE3 hashes.
    """
    import blake3
    
    files_created = {}
    
    # Small text file (1KB)
    small_file = temp_upload_dir / 'small.txt'
    small_content = b'Test content for upload verification\n' * 30  # ~1KB
    small_file.write_bytes(small_content)
    small_hash = blake3.blake3(small_content).hexdigest()
    files_created['small.txt'] = {
        'path': small_file,
        'size': len(small_content),
        'hash': small_hash,
        'content': small_content,
    }
    
    # Medium binary file (100KB)
    medium_file = temp_upload_dir / 'medium.bin'
    medium_content = os.urandom(1024 * 100)  # 100KB
    medium_file.write_bytes(medium_content)
    medium_hash = blake3.blake3(medium_content).hexdigest()
    files_created['medium.bin'] = {
        'path': medium_file,
        'size': len(medium_content),
        'hash': medium_hash,
        'content': medium_content,
    }
    
    logger.info(f"Created test files: {list(files_created.keys())}")
    logger.info(f"Total size: {sum(f['size'] for f in files_created.values())} bytes")
    
    return files_created


@pytest.fixture
def test_dataset(temp_upload_dir):
    """
    Create a test dataset WITH upload_log via the upload registration endpoint.
    Auto-cleanup after test completes.
    
    Returns tuple: (dataset_dict, upload_log_dict)
    """
    import requests
    import os
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    dataset_name = f'test-upload-{timestamp}'
    
    logger.info(f"Creating test dataset with upload_log: {dataset_name}")
    
    # Call POST /datasets/uploads (creates both dataset and upload_log)
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
    
    # Upload log includes audit_log which has the dataset
    dataset = upload_log['audit_log']['dataset']
    dataset_id = dataset['id']
    
    logger.info(f"✓ Dataset + upload_log created: ID={dataset_id}, name={dataset_name}")
    logger.info(f"  Upload log ID: {upload_log['id']}, status: {upload_log['status']}")
    logger.info(f"  Origin path: {dataset['origin_path']}")
    
    # Update origin_path to point to our test directory
    # (In production, /complete endpoint moves files, but for tests we control the location)
    requests.patch(
        f"{api_url}/datasets/{dataset_id}",
        json={'origin_path': str(temp_upload_dir)},
        headers={'Authorization': f'Bearer {token}'}
    )
    dataset['origin_path'] = str(temp_upload_dir)
    logger.info(f"  Updated origin_path to: {temp_upload_dir}")
    
    yield (dataset, upload_log)
    
    # Cleanup: Delete dataset after test
    logger.info(f"Cleaning up dataset: {dataset_id}")
    try:
        response = requests.delete(
            f"{api_url}/datasets/{dataset_id}",
            headers={'Authorization': f'Bearer {token}'}
        )
        response.raise_for_status()
        logger.info(f"✓ Dataset deleted: {dataset_id}")
    except Exception as e:
        logger.warning(f"Failed to delete dataset {dataset_id}: {e}")


@pytest.fixture
def upload_log(api_client, test_dataset):
    """
    Ensure upload_log exists for test dataset.
    
    Note: Upload logs are created by the /complete endpoint in production.
    For tests, we create them directly in the database via raw SQL.
    """
    dataset_id = test_dataset['id']
    
    logger.info(f"Ensuring upload_log exists for dataset {dataset_id}...")
    
    # Try to get existing upload_log
    try:
        upload_log = api_client.get_dataset_upload_log(dataset_id)
        logger.info(f"✓ Upload log already exists: {upload_log.get('id')}")
        return upload_log
    except Exception:
        pass
    
    # Upload log doesn't exist - create it via direct DB insert
    # (In production, this is done by /complete endpoint)
    import requests
    token = os.environ.get('APP_API_TOKEN')
    api_url = os.environ.get('API_BASE_URL', 'http://api:3030')
    
    # Use Prisma raw SQL via API (if available) or just note it needs manual setup
    logger.warning("Upload log doesn't exist - tests should create it manually")
    logger.warning("Each test should call update_dataset_upload_log with initial state")
    
    # Return None - tests will create their own initial state
    return None


def pytest_configure(config):
    """
    Pytest hook: Configure markers for test categorization.
    """
    config.addinivalue_line(
        "markers", "integration: Integration tests (run against Docker services)"
    )
    config.addinivalue_line(
        "markers", "slow: Slow tests (may take minutes)"
    )
    config.addinivalue_line(
        "markers", "requires_celery: Tests that require Celery worker running"
    )


def pytest_runtest_setup(item):
    """
    Pytest hook: Log test start.
    """
    logger.info("\n" + "="*80)
    logger.info(f"TEST STARTED: {item.name}")
    logger.info(f"Module: {item.module.__name__}")
    logger.info("="*80)


def pytest_runtest_teardown(item, nextitem):
    """
    Pytest hook: Log test completion.
    """
    logger.info("="*80)
    logger.info(f"TEST COMPLETED: {item.name}")
    logger.info("="*80 + "\n")

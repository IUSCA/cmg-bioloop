# Upload Verification Integration Tests

Comprehensive integration tests for the async upload verification system (Celery task + management script).

## Overview

These tests validate:
- ✅ Happy path flows (checksum enabled/disabled)
- ❌ Failure modes (crashes, retries, timeouts)
- 🔄 Recovery mechanisms (stale detection, respawning)
- 🎯 Edge cases (multiple files, concurrent uploads)

Tests run against **live Docker services**: API, Postgres, RabbitMQ, Celery worker.

## Test Structure

```
tests/upload_verification/
├── conftest.py                          # Shared fixtures and logging
├── test_utils.py                        # Helper functions
├── test_happy_path.py                   # Happy path scenarios
├── test_failure_status_transitions.py   # Script crashes, stale states
├── test_failure_task_execution.py       # Task failures, retries
├── test_failure_timeouts.py             # Hung tasks, RabbitMQ issues
├── test_timeout_limits.py               # Soft timeout test (COMMENT OUT AFTER USE)
├── test_edge_cases.py                   # Multiple files, concurrent uploads
└── README.md                            # This file
```

## Prerequisites

### 1. Install Dependencies

```bash
cd workers
poetry install  # Installs pytest and related packages
```

### 2. Start Docker Services

```bash
# From repo root
docker compose up -d

# Verify services are running
docker compose ps
```

Required services:
- `api` - Bioloop API (port 3030)
- `db` - PostgreSQL database (port 5432)
- `queue` - RabbitMQ (port 5672)
- `celery_worker` - Celery worker for tasks

### 3. Environment Variables

Ensure `workers/.env` has:

```bash
APP_API_TOKEN=<your-api-token>
API_BASE_URL=http://api:3030  # In docker, use service name
```

## Running Tests

### Run All Tests

```bash
cd workers
poetry run pytest tests/upload_verification/ -v
```

### Run Specific Test File

```bash
poetry run pytest tests/upload_verification/test_happy_path.py -v
```

### Run Single Test

```bash
poetry run pytest tests/upload_verification/test_happy_path.py::test_happy_path_with_checksum_enabled -v
```

### Run with Specific Markers

```bash
# Only integration tests
poetry run pytest tests/upload_verification/ -m integration -v

# Skip slow tests
poetry run pytest tests/upload_verification/ -m "not slow" -v

# Only tests requiring Celery
poetry run pytest tests/upload_verification/ -m requires_celery -v
```

### Run from Docker Container

```bash
# Exec into celery_worker container
docker compose exec celery_worker bash

# Inside container
cd /opt/sca/app
poetry run pytest tests/upload_verification/ -v
```

## Test Logs

### Log Location

All test logs are saved to `workers/test_logs/`:

```
workers/test_logs/
└── test_run_20260205_143022.log  # Timestamped per test run
```

**Note**: This directory is gitignored and mounted in Docker for host access.

### Log Contents

Each log file includes:
- Test execution timeline
- Fixture setup/teardown
- API interactions (dataset creation, upload log updates)
- Script output (manage_upload_workflows.py)
- Celery task states
- Detailed failure information

### Viewing Logs

```bash
# Tail latest log
tail -f workers/test_logs/test_run_*.log | head -100

# Search logs for errors
grep -i "error\|failed" workers/test_logs/test_run_*.log

# View logs for specific test
grep "TEST: Happy Path" workers/test_logs/test_run_*.log -A 50
```

## Test Categories

### Happy Path Tests (`test_happy_path.py`)

**File**: `test_happy_path.py`

1. **`test_happy_path_with_checksum_enabled`**
   - Flow: UPLOADED → VERIFYING → VERIFIED → COMPLETE
   - Validates full checksum verification (manifest hash)
   - Checks workflow trigger after verification

2. **`test_happy_path_with_checksum_disabled`**
   - Flow: UPLOADED → VERIFYING → VERIFIED → COMPLETE
   - Uses fallback `_verify_files_exist()` (no checksums)
   - Validates file existence check is sufficient

### Status Transition Failures (`test_failure_status_transitions.py`)

**File**: `test_failure_status_transitions.py`

3. **`test_failure_script_crash_after_verifying_before_spawn`**
   - Simulates: Script sets VERIFYING but crashes before spawning task
   - Recovery: Script detects stale VERIFYING (>5min), respawns task

4. **`test_failure_task_spawned_but_id_not_persisted`**
   - Simulates: Task spawned but script crashes before persisting task_id
   - Recovery: Same as above (idempotent respawn)

5. **`test_failure_stale_verifying_no_task_id`**
   - Validates 5-minute staleness threshold
   - Recovery: Respawn task

### Task Execution Failures (`test_failure_task_execution.py`)

**File**: `test_failure_task_execution.py`

6. **`test_failure_checksum_mismatch_retries_then_fails`**
   - Simulates: Corrupted manifest hash (tampering)
   - Celery retries 3 times (60s delays)
   - Final status: VERIFICATION_FAILED
   - Admin notification sent
   - **Duration**: ~5-6 minutes (due to retries)

7. **`test_failure_worker_crash_celery_failure_state`**
   - Simulates: Worker crash mid-verification
   - Celery marks task as FAILURE (no heartbeat)
   - Script detects FAILURE state, sets VERIFICATION_FAILED
   - **Note**: Full crash simulation is complex, test validates handling

8. **`test_failure_task_success_but_status_not_updated`**
   - Simulates: Task completes (Celery state=SUCCESS) but crashes before updating DB
   - Recovery: Script polls Celery state, updates to VERIFIED

### Timeout & Infrastructure Failures (`test_failure_timeouts.py`)

**File**: `test_failure_timeouts.py`

9. **`test_failure_task_hung_timeout_simulation`**
   - Simulates: Task VERIFYING for >24h
   - Script detects timeout, sets VERIFICATION_FAILED
   - **Note**: Uses timestamp simulation (can't wait 24h)

10. **`test_failure_rabbitmq_unavailable`**
    - Simulates: RabbitMQ connection failure when spawning task
    - Script logs error, status remains UPLOADED
    - Retries on next script run
    - **Note**: Cannot safely stop RabbitMQ in tests, validates error handling exists

### Timeout Limit Test (`test_timeout_limits.py`)

**File**: `test_timeout_limits.py`

11. **`test_soft_timeout_with_5min_limit`** ⚠️ SPECIAL TEST ⚠️
    - **BEFORE RUNNING**:
      1. Edit `workers/workers/tasks/declarations.py`:
         ```python
         soft_time_limit=300,  # 5 min (was 43200)
         time_limit=600,       # 10 min (was 86400)
         ```
      2. Restart Celery worker: `docker compose restart celery_worker`
    - **TEST**: Validates soft timeout behavior with shortened limit
    - **AFTER RUNNING**:
      1. **REVERT** `declarations.py` to production values
      2. Restart Celery worker
      3. **COMMENT OUT** this test (or mark with `@pytest.mark.skip`)
    - **Duration**: 6-8 minutes
    - **Note**: Test is already marked with `@pytest.mark.skip` by default

### Edge Cases (`test_edge_cases.py`)

**File**: `test_edge_cases.py`

12. **`test_edge_case_multiple_files_manifest`**
    - Creates 10 files (1KB-10KB each)
    - Validates manifest with multiple entries
    - Checks all files are verified correctly

13. **`test_edge_case_concurrent_uploads_processed`**
    - Creates 3 simultaneous uploads
    - Script processes all 3 independently
    - Validates no race conditions or conflicts
    - **Duration**: ~3-4 minutes

## Fixtures

### Core Fixtures (`conftest.py`)

- **`api_client`**: API module instance (uses `APP_API_TOKEN`)
- **`test_data_dir`**: Base directory for test uploads (`/opt/sca/data/test_uploads`)
- **`temp_upload_dir`**: Unique directory per test (auto-cleanup)
- **`test_files`**: Dict of dummy files with BLAKE3 hashes
- **`test_dataset`**: Dataset created via API (auto-cleanup)
- **`upload_log`**: Upload log record for test dataset

### Test Utilities (`test_utils.py`)

- `compute_manifest_hash(files_dict)` - Compute BLAKE3 manifest
- `wait_for_status(api, dataset_id, status, timeout)` - Poll until status reached
- `wait_for_celery_task_state(celery_app, task_id, state, timeout)` - Poll Celery task
- `set_upload_status(api, dataset_id, status, metadata)` - Manually update status
- `corrupt_file_hash(test_files, filename)` - Corrupt hash for testing

## Markers

Defined in `conftest.py` → `pytest_configure()`:

- `@pytest.mark.integration` - Integration test (runs against Docker services)
- `@pytest.mark.slow` - Slow test (>1 minute)
- `@pytest.mark.requires_celery` - Requires Celery worker running

## Troubleshooting

### Common Issues

#### 1. API Connection Refused

**Error**: `ConnectionError: API not reachable`

**Fix**:
```bash
# Check API is running
docker compose ps api

# Check logs
docker compose logs api

# Restart if needed
docker compose restart api
```

#### 2. Celery Worker Not Running

**Error**: `Task not found` or `No workers available`

**Fix**:
```bash
# Check worker
docker compose ps celery_worker

# Check logs
docker compose logs celery_worker

# Restart
docker compose restart celery_worker
```

#### 3. Database Connection Issues

**Error**: `psycopg2.OperationalError: could not connect`

**Fix**:
```bash
# Check Postgres
docker compose ps db

# Check API can connect
docker compose logs api | grep -i postgres
```

#### 4. RabbitMQ Not Available

**Error**: `kombu.exceptions.OperationalError`

**Fix**:
```bash
# Check RabbitMQ
docker compose ps queue

# Check Celery can connect
docker compose logs celery_worker | grep -i rabbitmq
```

#### 5. Test Files Not Found

**Error**: `FileNotFoundError: /opt/sca/data/test_uploads/...`

**Fix**:
```bash
# Ensure landing_volume is mounted
docker compose config | grep landing_volume

# Check directory exists
docker compose exec celery_worker ls -la /opt/sca/data/
```

### Debug Mode

Run tests with verbose output and immediate printing:

```bash
poetry run pytest tests/upload_verification/ -v -s
```

- `-v`: Verbose test names
- `-s`: Don't capture output (print immediately)

### Capture Logs

Run with full logging output:

```bash
poetry run pytest tests/upload_verification/ -v --log-cli-level=DEBUG
```

## Cleanup

### Manual Cleanup

If tests crash or are interrupted:

```bash
# List test datasets
docker compose exec api sh -c 'npx prisma studio'  # Open Prisma Studio, search "test-upload-"

# Delete via API
curl -X DELETE http://localhost:3030/datasets/<dataset-id> \
  -H "Authorization: Bearer $APP_API_TOKEN"

# Clean test data directory
docker compose exec celery_worker rm -rf /opt/sca/data/test_uploads/
```

### Reset Test Environment

```bash
# Stop all services
docker compose down

# Remove volumes (WARNING: Deletes all data)
docker compose down -v

# Restart fresh
docker compose up -d
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Upload Verification Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start Docker services
        run: docker compose up -d
      
      - name: Wait for services
        run: sleep 30
      
      - name: Run tests
        run: |
          docker compose exec -T celery_worker bash -c "
            cd /opt/sca/app
            poetry run pytest tests/upload_verification/ -v -m 'not slow'
          "
      
      - name: Upload test logs
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-logs
          path: workers/test_logs/
```

## Contributing

### Adding New Tests

1. Create test in appropriate file (or new file if new category)
2. Use fixtures from `conftest.py`
3. Add detailed logging (see existing tests)
4. Mark with appropriate markers (`@pytest.mark.integration`, etc.)
5. Update this README with test description

### Test Naming Convention

- File: `test_<category>.py`
- Function: `test_<scenario>_<expected_outcome>`
- Examples:
  - `test_happy_path_with_checksum_enabled`
  - `test_failure_checksum_mismatch_retries_then_fails`
  - `test_edge_case_concurrent_uploads_processed`

### Logging Best Practices

```python
logger.info("="*80)
logger.info("TEST: <Test Name>")
logger.info(f"Dataset ID: {dataset_id}")
logger.info("="*80)

# ... test steps with log statements ...

logger.info("✓ TEST PASSED: <Summary>")
logger.info("="*80)
```

## Maintenance

### Update Timeouts

If verification takes longer due to larger test files:

```python
# In test_utils.py
wait_for_status(..., timeout=120)  # Increase from 60s to 120s
```

### Update Test Data Sizes

If you want to test with larger files:

```python
# In conftest.py → test_files fixture
large_content = os.urandom(1024 * 1024 * 100)  # 100MB instead of 100KB
```

### Archive Old Logs

Test logs can accumulate over time:

```bash
# Archive logs older than 30 days
find workers/test_logs -name "*.log" -mtime +30 -exec gzip {} \;

# Delete archived logs older than 90 days
find workers/test_logs -name "*.log.gz" -mtime +90 -delete
```

## Questions?

See also:
- `.ai/bioloop/features/uploads.md` - Upload feature documentation
- `workers/workers/scripts/manage_upload_workflows.py` - Management script
- `workers/workers/tasks/verify_upload.py` - Celery task implementation

For issues, contact the development team or file a bug report.

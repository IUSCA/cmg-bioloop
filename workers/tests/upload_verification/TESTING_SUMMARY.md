# Upload Verification Testing - Quick Summary

## What Was Implemented

Comprehensive pytest-based integration tests for the async upload verification system (Celery task + management script).

## Test Count

- **Total**: 13 tests
- **Happy Path**: 2 tests
- **Failure Modes**: 8 tests
- **Edge Cases**: 2 tests
- **Special Timeout Test**: 1 test (commented out by default)

## Test Files

```
tests/upload_verification/
├── conftest.py                          # Fixtures (API, datasets, files)
├── test_utils.py                        # Helpers (manifest, polling)
├── test_happy_path.py                   # 2 tests
├── test_failure_status_transitions.py   # 3 tests
├── test_failure_task_execution.py       # 3 tests
├── test_failure_timeouts.py             # 2 tests
├── test_timeout_limits.py               # 1 test (skip by default)
├── test_edge_cases.py                   # 2 tests
├── run_tests.sh                         # Helper script
└── README.md                            # Full documentation
```

## Quick Start

```bash
# Install dependencies
cd workers
poetry install

# Ensure Docker services are running
docker compose up -d

# Run all tests (excluding slow/skipped)
poetry run pytest tests/upload_verification/ -v

# Or use helper script
./tests/upload_verification/run_tests.sh
```

## Test Scenarios

### Happy Path
1. **Checksum enabled**: UPLOADED → VERIFYING → VERIFIED → COMPLETE
2. **Checksum disabled**: File existence check fallback

### Failure Modes
3. **Script crash after VERIFYING**: No task spawned → Respawn after 5min
4. **Task spawned but ID not persisted**: Idempotent respawn
5. **Stale VERIFYING**: No task_id for >5min → Respawn
6. **Checksum mismatch**: 3 retries → VERIFICATION_FAILED (~5min test)
7. **Worker crash**: Celery FAILURE state → Admin notified
8. **Task SUCCESS but status not updated**: Script recovers
9. **Task hung >24h**: Timeout detection → VERIFICATION_FAILED
10. **RabbitMQ unavailable**: Error handling validation

### Edge Cases
11. **Multiple files**: 10 files manifest verification
12. **Concurrent uploads**: 3 simultaneous uploads processed

### Special Test (Commented Out)
13. **Soft timeout**: 5min limit test (requires manual config changes)

## Key Features

- **Integration tests**: Run against live Docker services (no mocks)
- **Comprehensive logging**: Persisted to `workers/test_logs/` (gitignored)
- **Auto-cleanup**: Datasets, files, logs cleaned up after tests
- **Modular design**: One test per scenario, reusable fixtures
- **Real failure simulation**: Crashes, stale states, timeouts

## Dependencies Added

```toml
[tool.poetry.group.dev.dependencies]
pytest = "^8.0.0"
pytest-asyncio = "^0.23.0"
pytest-timeout = "^2.2.0"
blake3 = "^0.4.1"
```

## Files Changed

**Added:**
- `workers/tests/conftest.py`
- `workers/tests/upload_verification/*.py` (8 files)
- `workers/pytest.ini`

**Modified:**
- `workers/pyproject.toml` (added pytest dependencies)
- `.gitignore` (added test_logs/)

## Running Tests

### All Tests (Fast Only)
```bash
poetry run pytest tests/upload_verification/ -m "not slow" -v
```

### All Tests (Including Slow)
```bash
poetry run pytest tests/upload_verification/ --all -v
# Or
./tests/upload_verification/run_tests.sh --all
```

### Specific Test File
```bash
poetry run pytest tests/upload_verification/test_happy_path.py -v
```

### Single Test
```bash
poetry run pytest tests/upload_verification/test_happy_path.py::test_happy_path_with_checksum_enabled -v
```

## Test Logs

Logs saved to: `workers/test_logs/test_run_YYYYMMDD_HHMMSS.log`

View logs:
```bash
tail -f workers/test_logs/test_run_*.log
```

## Documentation

See `workers/tests/upload_verification/README.md` for:
- Detailed prerequisites
- Troubleshooting guide
- CI/CD integration examples
- Maintenance guidelines
- Test descriptions

## Notes

- Tests use `APP_API_TOKEN` from environment (no mocks)
- Test data created in `/opt/sca/data/test_uploads` (auto-cleanup)
- Checksum mismatch test takes ~5min (3 retries with delays)
- Timeout limit test is commented out (requires manual config changes)
- All tests are idempotent and can be run multiple times

## Next Steps

1. Run tests: `./tests/upload_verification/run_tests.sh`
2. Review logs: `tail -f workers/test_logs/test_run_*.log`
3. Fix any failures
4. Integrate into CI/CD pipeline (see README)

---

**For detailed information, see `workers/tests/upload_verification/README.md`**

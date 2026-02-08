# Dataset Upload Feature

**Feature Scope:** Browser-based dataset upload with TUS resumable uploads and optional BLAKE3 checksum verification.

**Status:** Core Platform Feature (TUS-based implementation as of 2026-02)

---

## Overview

The upload feature allows users to upload datasets directly from their web browser using the TUS (resumable upload) protocol. Files are uploaded via TUS with automatic resume capability, and an async polling job triggers the integrated workflow.

---

## Architecture Components

### Services Involved

1. **UI Client** - File selection, TUS upload, optional BLAKE3 checksum computation
2. **API** - TUS server, upload metadata, dataset records, `/complete` endpoint
3. **Workers** - Polling job verifies uploads and triggers integrated workflow
4. **Rhythm API** - Orchestrates integrated workflow

### Database Tables

- `dataset` - Dataset record
- `dataset_upload_log` - Upload session metadata with fields:
  - `process_id` - TUS upload ID (formerly `tus_id`)
  - `status` - UPLOADING, UPLOADED, COMPLETE, VERIFICATION_FAILED, etc.
  - `metadata` - JSON field for checksums, failure reasons
- `dataset_audit` - Audit trail for dataset creation

---

## Upload Flow (TUS-based)

### 1. Dataset Creation
1. UI calls `POST /datasets/uploads` with dataset name, type
2. API creates `dataset`, `dataset_audit`, `dataset_upload_log` records
3. API generates `origin_path`: `/uploads/{type}/{id}/{name}`
4. Status set to `UPLOADING`

### 2. TUS Upload Phase
1. For each file, TUS client sends:
   - `POST /uploads/files` - Creates upload slot, returns upload ID
   - `PATCH /uploads/files/{id}` - Sends actual file bytes (resumable)
2. TUS metadata includes: `entity_type`, `entity_id`, `filename`, `relative_path`
3. Files stored temporarily in TUS upload directory

### 3. Completion Phase
1. UI calls `POST /datasets/uploads/:id/complete` with:
   - `process_id` - TUS upload ID
   - `metadata` - Optional BLAKE3 checksum data
2. API moves file from TUS temp to dataset's `origin_path`
3. Status set to `UPLOADED`
4. UI shows success (or retry button on failure)

### 4. Async Processing (Polling Job)
1. `manage_upload_workflows.py` runs every 30s (via entrypoint.sh in dev, PM2 in prod)
2. Finds uploads with status `UPLOADED` and `process_id` set
3. Verifies upload integrity (checksum or file existence)
4. On success: triggers `integrated` workflow directly, sets status to `COMPLETE`
5. On failure: sets status to `VERIFICATION_FAILED`

---

## Key Files

### API
- `api/src/routes/datasets/uploads.js` - Upload endpoints including `/complete`
- `api/src/services/upload.js` - TUS server configuration
- `api/src/services/dataset.js` - `getUploadedDatasetPath()` function
- `api/src/app.js` - TUS server mounting
- `api/src/constants.js` - `UPLOAD_STATUSES` enum

### Workers
- `workers/workers/scripts/manage_upload_workflows.py` - Polling job
- `workers/workers/upload.py` - `verify_upload_integrity()` for checksum verification
- `workers/bin/entrypoint.sh` - Spawns polling job in dev

### UI
- `ui/src/components/dataset/upload/UploadDatasetStepper.vue` - Upload UI
- `ui/src/services/upload/checksum.js` - BLAKE3 manifest hash computation (uses `hash-wasm`)
- `ui/src/views/DatasetUploadsPage.vue` - Upload logs table

---

## Path Construction

### API Side (api/src/services/dataset.js)

```javascript
const getUploadedDatasetPath = ({ datasetId, datasetType }) => path.join(
  config.upload.path,        // From UPLOAD_HOST_DIR env var
  datasetType.toLowerCase(), // 'raw_data' or 'data_product'
  `${datasetId}`,
);
```

### TUS FileStore (api/src/services/upload.js)

```javascript
const uploadPath = config.get('upload.path');  // From UPLOAD_HOST_DIR env var

this.tusServer = new Server({
  path: '/api/uploads/files',
  maxSize: 100 * 1024 * 1024 * 1024,  // 100 GB max file size
  datastore: new FileStore({
    directory: uploadPath,
    expirationPeriodInMilliseconds: 7 * 24 * 60 * 60 * 1000,  // 7 days
  }),
});
```

---

## Upload Statuses

| Status | Meaning |
|--------|---------|
| `UPLOADING` | Upload in progress |
| `UPLOADED` | TUS upload complete, waiting for async processing |
| `COMPLETE` | Integrated workflow triggered successfully |
| `VERIFICATION_FAILED` | Checksum mismatch or file not found |
| `PROCESSING_FAILED` | Workflow failed |
| `PERMANENTLY_FAILED` | Max retries exceeded |

---

## Checksum Verification (Optional)

When enabled (`config.upload.verify_checksums`):

1. **UI computes BLAKE3 manifest hash** before upload:
   - Hash each file
   - Create manifest: `blake3-manifest-v1\npath\tsize\thash`
   - Hash the manifest
2. **Stored in `metadata.checksum`** via `/complete` endpoint
3. **Worker verifies** by recomputing manifest hash from files at `origin_path`

**Feature Flag:**
- API: `config.get('upload.verify_checksums')` 
- Workers: `config['upload']['verify_checksums']` in `workers/config/common.py`
- UI: `config.enabledFeatures.upload_verify_checksums`

Currently **disabled by default**.

---

## Configuration

### Environment Variable

| Variable | Location | Purpose |
|----------|----------|---------|
| `UPLOAD_HOST_DIR` | `api/.env` (and root `.env` for docker-compose) | Path where API stores/reads uploaded files |

**Config mapping:** `api/config/custom-environment-variables.json` maps `UPLOAD_HOST_DIR` to `config.upload.path`

### Docker Environment

In `docker-compose-prod.yml`, the API service mounts the upload directory:

```yaml
api:
  environment:
    - UPLOAD_HOST_DIR=${UPLOAD_HOST_DIR}
  volumes:
    - ${UPLOAD_HOST_DIR}:${UPLOAD_HOST_DIR}
```

The same path is used on host and in container for simplicity.

### Localhost/Docker Development

```bash
# api/.env.default
UPLOAD_HOST_DIR=/opt/sca/data/uploads
```

The `init_data_dirs` container creates `/opt/sca/data/uploads` on startup.
The `landing_volume` Docker volume mounts at `/opt/sca/data`.

### Production

```bash
# Root .env (for docker-compose volume mount)
UPLOAD_HOST_DIR=/N/scratch/cmguser/cmg-bioloop/uploads

# api/.env (also needs this, or passed via docker-compose environment)
UPLOAD_HOST_DIR=/N/scratch/cmguser/cmg-bioloop/uploads
```

---

## Production Deployment Checklist

### Service Host (cmg-new-service1.sca.iu.edu)

1. **Root `.env`** - Set docker-compose variable:
   ```bash
   UPLOAD_HOST_DIR=/N/scratch/cmguser/cmg-bioloop/uploads
   ```

2. **`docker-compose-prod.yml`** - Volume mount and environment configured:
   ```yaml
   api:
     environment:
       - UPLOAD_HOST_DIR=${UPLOAD_HOST_DIR}
     volumes:
       - ${UPLOAD_HOST_DIR}:${UPLOAD_HOST_DIR}
   ```

3. **Host directory exists** with proper permissions:
   ```bash
   mkdir -p /N/scratch/cmguser/cmg-bioloop/uploads
   ```

---

## API Endpoints

### Upload Management
- `POST /datasets/uploads` - Create dataset and upload log
- `POST /datasets/uploads/:id/complete` - Register TUS completion, move files
- `PATCH /datasets/uploads/:id/upload-log` - Update upload metadata
- `GET /datasets/uploads` - List uploads (filters out those without `process_id`)
- `GET /datasets/uploads/:username` - User's uploads

### TUS Endpoints (mounted at /uploads/files)
- `POST /uploads/files` - Create TUS upload
- `PATCH /uploads/files/:id` - Send file data
- `HEAD /uploads/files/:id` - Check upload status

---

## Polling Job

**Script:** `workers/workers/scripts/manage_upload_workflows.py`

**Schedule:**
- Dev: Every 30s via background loop in `entrypoint.sh`
- Prod: Every 1 min via PM2 cron in `ecosystem.config.js`

**Process:**
1. Fetch uploads with `UPLOADED` status (older than 30s threshold)
2. Verify integrity (checksum or file existence)
3. Start `integrated` workflow directly (no `process_dataset_upload` step)
4. Update status to `COMPLETE` or `VERIFICATION_FAILED`

---

## UI Behavior

### Success Flow
1. TUS upload completes → UI calls `/complete`
2. `/complete` succeeds → Green success message, button disabled

### Failure Flow
1. `/complete` fails → Warning message, "Retry Registration" button appears
2. User clicks Retry → Calls `/complete` again

### Upload Logs Table
- Only shows uploads with `process_id` (hides incomplete/orphaned uploads)
- Status column shows icons like Import Log table (spinner/check/warning/error)
- Polls for status updates on uploads with active workflows or pending processing

---

## Removed Components (2026-02)

The `process_dataset_upload` workflow was removed. Previously:
- UI → API → `process_dataset_upload` workflow → `integrated` workflow

Now:
- UI → API → Polling job → `integrated` workflow directly

Removed from: `workers/tasks/`, `workers/api.py`, `api/constants.js`, `api/routes/`, `ui/services/`

---

## Migration from Chunk-Based Upload

The previous chunk-based upload system used secure_download service. Key changes:

| Aspect | Old (Chunk-based) | New (TUS) |
|--------|-------------------|-----------|
| Upload Handler | secure_download service | API service (TUS server) |
| Protocol | Custom chunk API | TUS 1.0 protocol |
| Resume Support | Manual chunk tracking | Built-in TUS resumable |
| File Assembly | Worker merges chunks | TUS handles directly |
| Max File Size | Limited by chunk handling | 100 GB |

---

## Changelog

### 2026-02-05 - Async Upload Verification Implementation

**Major Feature:** Implemented asynchronous upload integrity verification to prevent blocking PM2 cron script.

**Problem:**
- `manage_upload_workflows.py` script runs every 1 minute via PM2 `cron_restart`
- PM2 `cron_restart` kills and restarts processes, interrupting long-running BLAKE3 verification
- BLAKE3 checksum verification of large files (200GB+) can take hours
- Blocking verification in script prevented timely workflow triggering for other uploads

**Solution:**
- Offloaded verification to standalone Celery task with robust failure handling
- Script now spawns async task and tracks state via `VERIFYING` status
- Implemented comprehensive failure recovery for all edge cases

**Implementation Details:**

**New Upload States:**
- `VERIFYING`: Integrity verification in progress (async Celery task)
- `VERIFIED`: Integrity verified, ready to trigger workflow
- `PERMANENTLY_FAILED`: Max retries exceeded

**Celery Task (`verify_upload_integrity`):**
- Standalone task (not WorkflowTask) - fire-and-forget with status tracking
- Streaming BLAKE3 hash with 16MB chunks (Lustre-optimized)
- 24-hour timeout (soft: 23h 55m, hard: 24h)
- Auto-retry (max 3 attempts, 60s delay)
- Worker process integration for log tracking
- Detailed failure logging with resolution guidance

**Script Flow (`manage_upload_workflows.py`):**
1. `UPLOADED` → Set `VERIFYING` → Spawn task → Persist task ID
2. `VERIFYING` → Check task state → Handle all failure modes
3. `VERIFIED` → Trigger integrated workflow → Set `COMPLETE`

**Failure Modes Handled:**
1. Script crashes after `VERIFYING` but before spawning task → Respawn after 5min
2. Task spawned but crashes before persisting task ID → Respawn after 5min
3. Task throws catchable exception → Auto-retry (3x), then `VERIFICATION_FAILED`
4. Worker crashes mid-hash (uncatchable) → Celery marks as `FAILURE`
5. Task succeeds but crashes before status update → Script detects and updates
6. Task hung (>24h) → Script marks as `VERIFICATION_FAILED`
7. Stale `VERIFYING` (no task ID, >5min) → Respawn task
8. RabbitMQ/Celery unavailable → Script retries on next run
9. Admin notification sent for permanent failures

**UI Changes:**
- Added manifest hash progress indicator during client-side computation
- Created `/uploads/:id` admin page with:
  - Upload overview (dataset link, status, type)
  - Status icon and chip display
  - Worker process logs with 10-second auto-refresh
  - Only accessible by admins

**Files Modified:**
- `api/src/constants.js`: Added `VERIFYING`, `VERIFIED`, `PERMANENTLY_FAILED`
- `workers/workers/constants/upload.py`: Added new statuses
- `ui/src/constants.js`: Added new statuses
- `workers/workers/upload.py`: Streaming BLAKE3 hash (16MB chunks)
- `workers/workers/tasks/declarations.py`: New `verify_upload_integrity` task
- `workers/workers/tasks/verify_upload.py`: Task implementation with logging
- `workers/workers/scripts/manage_upload_workflows.py`: Rewritten with async flow
- `api/src/routes/uploads.js`: Updated `/stalled` to include `VERIFYING`/`VERIFIED`
- `ui/src/components/dataset/upload/UploadDatasetStepper.vue`: Progress callback
- `ui/src/pages/uploads/[id].vue`: New admin upload details page

**Performance:**
- Script runs in <1 second (non-blocking)
- Verification runs in background (up to 24 hours for very large files)
- 16MB chunks optimal for Lustre filesystem (avoids MDS bottleneck)

**Design Principles:**
- Uploads decoupled from Datasets (generic entity handlers)
- Verification is idempotent and resumable
- Task ID acts as distributed lock to prevent duplicate verification
- Comprehensive logging for debugging and admin troubleshooting

### 2026-02-05 - Upload Verification Integration Tests

**Comprehensive pytest-based integration tests for async upload verification system.**

**Test Coverage:**
- ✅ 2 Happy path tests (checksum enabled/disabled)
- ❌ 8 Failure mode tests (crashes, retries, timeouts, infrastructure)
- 🔄 2 Edge case tests (multiple files, concurrent uploads)
- ⚠️ 1 Special timeout test (5min limit, comment out after validation)

**Test Structure:**
```
tests/upload_verification/
├── conftest.py                          # Fixtures (api_client, test_files, datasets)
├── test_utils.py                        # Helpers (manifest hash, status polling)
├── test_happy_path.py                   # Happy paths
├── test_failure_status_transitions.py   # Script crashes, stale states
├── test_failure_task_execution.py       # Task failures, retries
├── test_failure_timeouts.py             # Hung tasks, RabbitMQ issues
├── test_timeout_limits.py               # Soft timeout (5min test, skip by default)
├── test_edge_cases.py                   # Multiple files, concurrent uploads
├── run_tests.sh                         # Helper script
└── README.md                            # Comprehensive guide
```

**Test Environment:**
- Runs against Docker services (API, Postgres, RabbitMQ, Celery)
- Uses `APP_API_TOKEN` for API authentication (no mocks)
- Test data in `/opt/sca/data/test_uploads` (auto-cleanup)
- Logs persisted to `workers/test_logs/` (gitignored, docker-mounted)

**Key Features:**
- One test function per failure scenario (isolation)
- Detailed logging with timestamps and context
- Automatic cleanup (datasets, files, logs)
- Pytest markers: `@pytest.mark.integration`, `@pytest.mark.slow`, `@pytest.mark.requires_celery`
- Helper script: `run_tests.sh --all` or `run_tests.sh --slow`

**Running Tests:**
```bash
# From workers directory
poetry install                           # Install pytest dependencies
poetry run pytest tests/upload_verification/ -v

# Or use helper script
./tests/upload_verification/run_tests.sh --all
```

**Test Scenarios Covered:**
1. ✅ Happy path: UPLOADED → VERIFYING → VERIFIED → COMPLETE (checksum enabled)
2. ✅ Happy path: File existence fallback (checksum disabled)
3. ❌ Script crash after VERIFYING before spawning task
4. ❌ Task spawned but ID not persisted
5. ❌ Stale VERIFYING (no task_id, >5min)
6. ❌ Checksum mismatch → 3 retries → VERIFICATION_FAILED (~5min)
7. ❌ Worker crash → Celery FAILURE state
8. ❌ Task SUCCESS but status not updated
9. ❌ Task hung >24h timeout (simulated)
10. ❌ RabbitMQ unavailable (error handling validation)
11. ⚠️ Soft timeout with 5min limit (requires manual timeout config changes)
12. 🎯 Multiple files manifest (10 files)
13. 🎯 Concurrent uploads (3 simultaneous)

**Files Added:**
- `workers/tests/conftest.py`: Session-scoped fixtures
- `workers/tests/upload_verification/*.py`: 6 test modules
- `workers/tests/upload_verification/README.md`: 400+ line test guide
- `workers/tests/upload_verification/run_tests.sh`: Helper script
- `workers/pytest.ini`: Pytest configuration

**Files Modified:**
- `workers/pyproject.toml`: Added pytest dependencies
- `.gitignore`: Added `workers/test_logs/`, `workers/tests/fixtures/temp_uploads/`
- `docker-compose.yml`: Added comment about test_logs mount (already included via workers/ mount)

**Dependencies Added:**
- `pytest ^8.0.0`
- `pytest-asyncio ^0.23.0`
- `pytest-timeout ^2.2.0`
- `blake3 ^0.4.1` (for test manifest computation)

**Design Principles:**
- Tests are modular, decoupled, and reusable
- Each test file focuses on one failure category
- Extensive logging for debugging (persisted to gitignored dir)
- Auto-cleanup prevents test pollution
- Tests simulate real-world failure modes

**Documentation:**
- `workers/tests/upload_verification/README.md`: Complete guide with:
  - Prerequisites and setup
  - Running tests (various modes)
  - Test descriptions and scenarios
  - Troubleshooting section
  - CI/CD integration examples
  - Maintenance guidelines

### 2026-02-05 - Code and Documentation Cleanup
- **Configuration Simplified:** Now uses single `UPLOAD_HOST_DIR` env var (removed `UPLOAD_DIR`, `UPLOAD_MOUNT_DIR`)
- **Upload Log Status Icons:** Replaced status chips with icons matching Import Log table pattern
- **TUS Implementation Complete:** Full TUS-based upload flow working
- **Removed `process_dataset_upload`:** Polling job triggers `integrated` directly
- **Polling job in dev:** Added to `entrypoint.sh` (runs every 30s)
- **BLAKE3 checksums:** Optional verification using `hash-wasm` (UI) and `blake3` (workers)
- **UI retry logic:** Shows retry button if `/complete` API fails
- **Upload logs filter:** Hides uploads without `process_id`
- **Code cleanup:** Removed dead TUS hook methods from `api/src/services/upload.js`
- **Code cleanup:** Removed legacy chunk upload route from `secure_download/src/routes/upload.js`
- **Code cleanup:** Removed ~300 lines of dead chunk-based upload code from `UploadDatasetStepper.vue`
- **Code cleanup:** Removed dead `ui/src/services/upload/index.js` and `uploadApi.js`
- **Code cleanup:** Removed `spark-md5` npm dependency (replaced by BLAKE3)
- **Code cleanup:** Removed `upload_scope` from `secure_download/config/default.json`
- **Docs cleanup:** Deleted obsolete `docs/features/dataset_upload.md` and upload diagrams
- **Docs cleanup:** Deleted migration tracking files (`TUS_*.md`, `UPLOAD_CHECKSUM_*.md`)
- **Docs cleanup:** Fixed `.ai/bioloop/features/imports-downloads.md` (removed incorrect upload references)
- **Docs cleanup:** Removed `process_dataset_upload` and `cancel_dataset_upload` from `BIOLOOP_WORKFLOW_ARCHITECTURE.md`
- **Architecture doc:** Created `docs/features/tus-upload-architecture.md` for tech lead overview

### 2026-02-04
- **Schema changes:** Renamed `tus_id` → `process_id`, added `metadata` JSON field
- **Added `VERIFICATION_FAILED` status**

### 2026-02-03
- **Major Update:** Migrated from chunk-based upload to TUS protocol
- **Architecture Change:** TUS server now runs in API service (not secure_download)
- **docker-compose-prod.yml:** Added volume mount for uploads

### 2026-01-18
- **Issue Identified:** secure_download container missing `/N/...` mount in production
- **Status:** Superseded by TUS implementation

---

## Pending Work

1. **Orphan Detection:** TUS uploads that complete but fail to register (no `process_id` in DB) need detection/cleanup mechanism.

---

**Last Updated:** 2026-02-05

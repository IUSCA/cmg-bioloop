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

### API (api/.env)
```bash
UPLOAD_DIR=/opt/sca/data/uploads  # TUS upload directory
```

### Workers (workers/workers/config/common.py)
```python
config = {
    'upload': {
        'verify_checksums': False  # Enable BLAKE3 verification
    }
}
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

## Changelog

### 2026-02-05
- **Upload Log Status Icons:** Replaced status chips with icons matching Import Log table pattern:
  - Spinner (primary) for uploads in progress
  - Spinner (warning) for uploads pending processing or active integrated workflow
  - Green check for successful integrated workflow
  - Warning/error icons for failures (verification, processing, registration)
  - Added polling for datasets with active workflows or pending processing
- **TUS Implementation Complete:** Full TUS-based upload flow working
- **Removed `process_dataset_upload`:** Polling job triggers `integrated` directly
- **Polling job in dev:** Added to `entrypoint.sh` (runs every 30s)
- **BLAKE3 checksums:** Optional verification using `hash-wasm` (UI) and `blake3` (workers)
- **UI retry logic:** Shows retry button if `/complete` API fails
- **Upload logs filter:** Hides uploads without `process_id`

### 2026-02-04
- **Schema changes:** Renamed `tus_id` → `process_id`, added `metadata` JSON field
- **Added `VERIFICATION_FAILED` status**

### 2026-01-18
- **Issue Identified:** secure_download container missing `/N/...` mount in production

---

## Pending Work

1. **Orphan Detection:** TUS uploads that complete but fail to register (no `process_id` in DB) need detection/cleanup mechanism.

---

**Last Updated:** 2026-02-05


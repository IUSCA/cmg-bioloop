# Dataset Upload Feature

**Feature Scope:** Browser-based dataset upload with chunked file transfer and OAuth-based access control.

**Status:** Core Platform Feature

---

## Overview

The upload feature allows users to upload datasets directly from their web browser. Files are uploaded in 2MB chunks to handle large files, network interruptions, and provide granular progress tracking.

---

## Architecture Components

### Services Involved

1. **UI Client** - Initiates upload, chunks files, sends chunks
2. **API** - Manages upload metadata, creates dataset records
3. **secure_download Service** - Receives file chunks, writes to filesystem
4. **Signet (OAuth)** - Issues upload tokens (client credentials flow)
5. **Rhythm API** - Orchestrates upload processing workflow
6. **Workers** - Merges chunks, processes uploaded datasets

### Database Tables

- `dataset` - Dataset record
- `dataset_upload_log` - Upload session metadata
- `file_upload_log` - Per-file upload tracking (chunks, checksums)
- `dataset_audit` - Audit trail for dataset creation

---

## Upload Flow

### 1. Pre-Upload Phase
1. UI evaluates MD5 checksums for files and chunks
2. UI posts metadata to API:
   - File names, checksums, relative paths
   - Optional: source raw data, project, instrument
3. API creates `dataset`, `dataset_upload_log`, `file_upload_log` records
4. API generates `origin_path` using `getUploadedDatasetPath()`
5. UI requests OAuth upload token from Signet via API

### 2. Upload Phase
1. UI uploads file chunks sequentially to secure_download `/upload` endpoint
2. Each chunk includes:
   - `upload_path` - Dataset's origin path (from API)
   - `file_upload_log_id` - File identifier
   - `checksum` - File's overall MD5
   - `chunk_checksum` - This chunk's MD5
   - `index` - Chunk position
3. secure_download verifies OAuth scope and chunk checksum
4. Chunks written to: `{upload_path}/uploaded_chunks/{file_upload_log_id}/{checksum}-{index}`

### 3. Post-Upload Phase
1. UI initiates `process_dataset_upload` workflow via Rhythm
2. Worker merges chunks into complete files
3. Worker validates file checksums
4. Dataset becomes available for staging/use

---

## Path Construction

### API Side (api/src/services/dataset.js)

```javascript
const getUploadedDatasetPath = ({ datasetId, datasetType }) => path.join(
  config.upload.path,        // From UPLOAD_DIR env var
  datasetType.toLowerCase(),  // 'raw_data' or 'data_product'
  `${datasetId}`,
  'processed',
);
```

**Example:** `/N/scratch/cmguser/cmg-bioloop/uploads/data_product/123/processed`

### secure_download Side (secure_download/src/routes/upload.js)

```javascript
const getFileChunksStorageDir = ({ uploadPath, fileUploadLogId }) => path.join(
  uploadPath,           // Passed from API in request body
  'uploaded_chunks',
  fileUploadLogId,
);
```

**Example:** `/N/scratch/cmguser/cmg-bioloop/uploads/data_product/123/processed/uploaded_chunks/456/`

---

## Configuration

### API Service (api/.env)

```bash
UPLOAD_DIR=/N/scratch/cmguser/cmg-bioloop/uploads  # Production
# UPLOAD_DIR=/opt/sca/data  # Docker/local development
```

### secure_download Service

**No specific config needed** - upload path comes from API in request body.

However, the service MUST have the upload directory mounted in its Docker container.

---

## Production Deployment

### Service Host (cmg-new-service1.sca.iu.edu)

**API Container:**
- Repository: `/opt/sca/cmg`
- Config: `api/.env` sets `UPLOAD_DIR=/N/scratch/cmguser/cmg-bioloop/uploads`
- The API sends this path to secure_download in the request body

### Worker/Download Hosts (colo, mmge2, etc.)

**secure_download Container:**
- Deployment: `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml`
- Repository: `/opt/sca/cmg-bioloop/secure_download` (mounted as volume)
- **CRITICAL:** Container MUST have upload directory mounted with write permissions

---

## Known Issues & Solutions

### Issue: Upload Permission Denied (2026-01-18)

**Error:**
```
Error: EACCES: permission denied, mkdir '/N'
```

**Root Cause:**
The `secure_download` api container tries to create directories under `/N/scratch/cmguser/cmg-bioloop/uploads` but doesn't have that path mounted.

**Current State:**
In `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml`:
- The `api` service has NO `/N/...` mounts
- Only the `nginx-cmg-bioloop` service has `/N/scratch/cmguser/cmg-bioloop/:ro` (read-only)

**Solution:**
Add to the `api` service in `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml`:

```yaml
api:
  volumes:
    - /opt/sca/cmg-bioloop/secure_download/:/opt/sca/app
    - api_modules:/opt/sca/app/node_modules
    - /N/scratch/cmguser/cmg-bioloop/uploads:/N/scratch/cmguser/cmg-bioloop/uploads  # ADD THIS
```

Then restart:
```bash
cd /opt/sca/docker/scripts/cmg-new-download
docker compose -f docker-compose-prod.yml restart
```

**Note:** Agent does NOT have write access to that compose file. User must make this change.

---

## API Endpoints

### Upload Management
- `POST /datasets/uploads` - Register new upload, create dataset
- `PATCH /datasets/uploads/:id` - Update upload status
- `GET /datasets/uploads/:id` - Get upload details
- `POST /upload/token` - Request OAuth upload token

### secure_download Endpoints
- `POST /upload` - Accept file chunk (with OAuth token)

---

## OAuth Scopes

Upload tokens use file-specific scopes:
- Pattern: `upload_file:{hyphen-delimited-filename}`
- Example: `upload_file:my-data-file-txt`
- Issued by: Signet OAuth server
- Flow: Client credentials

---

## Worker Tasks

### process_dataset_upload
1. Verifies upload directory exists
2. Creates `{dataset_path}/processed` directory
3. For each file:
   - Reads chunks: `{dataset_path}/uploaded_chunks/{file_upload_log_id}/{checksum}-{i}`
   - Merges chunks sequentially into final file
   - Validates file checksum
   - Updates `file_upload_log` status
4. Updates `dataset_upload_log` status to complete

### cancel_dataset_upload
1. Marks upload as cancelled in database
2. Optionally cleans up uploaded chunks

---

## Key Patterns

### Error Handling
- Chunk checksum mismatches → reject chunk
- Missing OAuth scope → 403 Forbidden
- Network interruption → UI retries failed chunks
- File merge failure → mark upload as failed, notify user

### Security
- Each file requires unique OAuth token with file-specific scope
- Tokens expire after use
- upload_path validated to prevent directory traversal
- Checksum validation at chunk and file level

### Performance
- 2MB chunk size balances memory usage and network efficiency
- Sequential chunk upload simplifies merge logic
- Chunks stored separately allows resumable uploads

---

## Related Documentation

- [Detailed Upload Documentation](../../docs/features/dataset_upload.md)
- [secure_download Architecture](../../docs/features/secure_download.md)
- [Production Deployment](.ai/PRODUCTION_DEPLOYMENT.md)

---

## Changelog

### 2026-01-18
- **Issue Identified:** secure_download container missing `/N/...` mount in production
- **Impact:** Upload requests fail with `EACCES: permission denied, mkdir '/N'`
- **Status:** Documented solution, requires user to update production compose file

---

**Last Updated:** 2026-01-18


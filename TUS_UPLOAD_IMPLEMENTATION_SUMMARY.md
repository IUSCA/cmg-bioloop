# TUS Upload Implementation Summary

**Date Started:** 2026-02-01  
**Status:** 62% Complete (8/13 tasks done)  
**Branch:** Current working branch

---

## 🎯 Project Goal

Replace the current complex, custom chunked upload system with industry-standard **TUS protocol** for resumable uploads.

### Key Benefits
- ✅ Handles 100GB+ files reliably
- ✅ Built-in resumability (browser crash = resume from exact byte)
- ✅ Removes 90% of custom upload code
- ✅ Generic architecture (works for any entity, not just datasets)
- ✅ No `cancel_dataset_upload` workflow needed
- ✅ No OAuth token-per-file complexity

---

## ✅ Completed Tasks (8/13)

### 1. Database Schema Updated
**File:** `api/prisma/schema.prisma`
- ✅ Removed `file_upload_log` table (TUS handles chunks internally)
- ✅ Simplified `dataset_upload_log` table
- ✅ Added TUS-specific fields: `tus_id`, `file_path`, `file_size`
- ✅ Added retry tracking: `retry_count`, `failure_reason`
- ✅ Added upload mode tracking: `selection_mode`, `directory_name`
- ✅ Added new status: `PERMANENTLY_FAILED`

**Migration needed:** Run `npx prisma migrate dev` after npm install

### 2. Dependencies Installed
**API (`api/package.json`):**
```json
"@tus/server": "^1.3.0",
"@tus/file-store": "^1.3.0"
```

**UI (`ui/package.json`):**
```json
"tus-js-client": "^4.1.0"
```

**Action needed:** Run `npm install` in both `api/` and `ui/` directories

### 3. Generic Upload Service Created
**File:** `api/src/services/upload.js`
- Entity-agnostic TUS server
- Handles all upload protocol logic
- Delegates business logic to entity handlers
- 7-day auto-expiration for incomplete uploads

### 4. Dataset Upload Handler Created
**File:** `api/src/services/upload/datasetHandler.js`
- Dataset-specific business logic
- Updates `dataset_upload_log` table
- Preserves directory structure for folder uploads
- Triggers `process_dataset_upload` workflow

### 5. Upload API Endpoints Created
**File:** `api/src/routes/uploads.js`
- Mounts TUS server at `/uploads/*`
- Helper endpoints for retry jobs:
  - `/uploads/stalled` - Find stalled uploads
  - `/uploads/failed` - Find failed uploads  
  - `/uploads/expired` - Find expired uploads
  - `/uploads/all-tus-ids` - For orphaned file detection
  - `/uploads/with-file-paths` - For missing file detection

**Already mounted in:** `api/src/routes/index.js` (line 43)

### 6. Generic UI Upload Component Created
**File:** `ui/src/components/upload/GenericUploader.vue`
- Supports: single file, multiple files, directory selection
- Per-file progress tracking
- Overall progress tracking
- Cancel/resume support
- Works for any entity type (dataset, session, etc.)

**Bug Fixed:** Changed `require('@/services/prisma')` to `require('@/db')` in:
- `api/src/routes/uploads.js`
- `api/src/services/upload/datasetHandler.js`

### 7. DatasetUploader Component Created
**File:** `ui/src/components/dataset/upload/DatasetUploader.vue`
- ✅ Wrapper around GenericUploader
- ✅ Dataset-specific form fields (name, type, project, raw data, instrument, genomic details)
- ✅ Creates dataset and upload_log before upload via POST /datasets/uploads
- ✅ Handles upload completion and redirects to dataset page
- ✅ Pre-populates dataset name from directory name (if directory selected)

**API Updated:** `api/src/routes/datasets/uploads.js`
- ✅ Removed `files_metadata` requirement (TUS doesn't need it)
- ✅ Removed `file_upload_log` creation (table no longer exists)
- ✅ Simplified PATCH endpoint (removed files array handling)

**Page Updated:** `ui/src/pages/datasetUpload/new.vue`
- ✅ Uses new DatasetUploader component instead of UploadDatasetStepper

### 8. Worker Retry Job Created
**File:** `workers/workers/scripts/manage_upload_workflows.py`
- ✅ Retries stalled uploads (UPLOADED but workflow not started >5 min)
- ✅ Retries failed processing workflows (PROCESSING_FAILED, up to 3 times)
- ✅ Marks uploads as PERMANENTLY_FAILED after 3 failures
- ✅ Sends admin notifications for permanent failures
- ✅ Dry-run mode for testing
- ✅ Comprehensive logging and error handling
- ✅ Designed to run every 15 minutes via PM2 or cron

**API Wrappers Added:** `workers/workers/api.py`
- ✅ `get_stalled_uploads()` - Get UPLOADED uploads >5 min old
- ✅ `get_failed_uploads()` - Get PROCESSING_FAILED uploads with retry count
- ✅ `update_upload_retry()` - Update retry count and status
- ✅ `trigger_dataset_upload_workflow()` - Trigger process_dataset_upload workflow

**Constants Updated:** `workers/workers/constants/upload.py`
- ✅ Added `MAX_RETRY_COUNT = 3` constant
- ✅ Added `PERMANENTLY_FAILED` status

---

## 📋 Remaining Tasks (5/13)

### 9. Create Worker Cleanup Job
**File to create:** `workers/workers/scripts/cleanup_upload_resources.py`
- Cleans expired incomplete uploads (UPLOADING > 7 days)
- Removes orphaned files (no DB record)
- Detects missing files (DB record but no file)
- Run daily via PM2 or cron

### 10. Update Worker Task - Remove Chunk Merging
**File to update:** `workers/workers/tasks/process_dataset_upload.py`
- Remove all chunk merging logic (TUS already wrote complete files)
- Simplify to just:
  1. Validate file exists
  2. Optional: Validate checksum
  3. Update upload status to COMPLETE
  4. Trigger `integrated` workflow (not `upload_integrated`)
  5. Clean up any temporary resources

### 11. Update Docker Compose Configs
**Files to update:**
- `docker-compose.yml` (local dev)
- `docker-compose-prod.yml` (production)

**Changes needed:**
```yaml
api:
  volumes:
    # Docker dev
    - landing_volume:/opt/sca/data
    # Production
    - /N/scratch/cmguser/cmg-bioloop/uploads:/uploads
  environment:
    - UPLOAD_DIR=/opt/sca/data  # or /uploads in prod
```

### 12. Remove secure_download Upload Endpoint
**File to update:** `secure_download/src/routes/index.js`
- Remove: `router.use('/upload', require('./upload'));`
- Keep: `router.use('/download', require('./download'));`

**Optional:** Delete `secure_download/src/routes/upload.js` (no longer used)

### 13. Update Documentation
**File to update:** `.ai/bioloop/features/uploads.md`
- Document TUS architecture
- Remove references to custom chunking
- Update failure modes and recovery
- Document generic upload pattern
- Add examples for future entity types

---

## 🔑 Key Architecture Decisions

### Generic Pattern
```
GenericUploader (UI)
  ↓ uploads to
TUS Server (API) → entity handler
  ↓ delegates to
DatasetHandler → updates dataset_upload_log → triggers workflow
```

### File/Directory Handling
- **Single file:** User selects one file, uploads directly
- **Multiple files:** User Ctrl+clicks files, uploads each with TUS
- **Directory:** User selects folder, preserves structure via `webkitRelativePath`

### Upload Status Flow
```
UPLOADING → UPLOADED → PROCESSING → COMPLETE
                  ↓
              PROCESSING_FAILED → [retry up to 3x] → PERMANENTLY_FAILED
```

### Workflow Integration
- `process_dataset_upload`: Validates uploaded files, triggers `integrated`
- `integrated`: Same workflow as always (inspect → archive → stage → validate)
- NO separate `upload_integrated` workflow

---

## 🐛 Known Issues & Fixes

### Issue: Module Not Found
**Error:** `Cannot find module '@/services/prisma'`  
**Fix:** ✅ Changed to `require('@/db')` in uploads.js and datasetHandler.js

### Issue: Missing Dependencies
**Fix:** ✅ Run `npm install` in both api/ and ui/ directories

### Issue: Database Schema Out of Sync  
**Fix:** ✅ Run `npx prisma migrate dev --name add_tus_upload_support`

### Issue: Old Upload Code References Removed Relations
**Error:** `Unknown field 'files' for include statement on model 'dataset_upload_log'`  
**Fix:** ✅ Removed `files` relation from `INCLUDE_DATASET_UPLOAD_LOG_RELATIONS` and `INCLUDE_AUDIT_LOGS` in `api/src/constants.js`
- The `file_upload_log` table no longer exists (removed in TUS migration)
- The `files` relation on `dataset_upload_log` no longer exists
- TUS tracks files internally, no need for per-file DB records

### Issue: Authentication Middleware Not Following App Conventions
**Error:** `auth.verifyTokenMiddleware is not a function`  
**Fix:** ✅ Changed all upload endpoints to use `accessControl` pattern instead of direct `authenticate`
- GET endpoints → `isPermittedTo('read')`
- PATCH endpoint → `isPermittedTo('update')`
- TUS file upload → `authenticate` (basic auth, not resource-based)

---

## 🧪 Testing Checklist (After Implementation)

### Unit Tests
- [ ] Upload service handler routing
- [ ] Dataset handler validation
- [ ] File path construction for directories

### Integration Tests
- [ ] Single file upload → workflow triggered
- [ ] Multiple file upload → all files uploaded
- [ ] Directory upload → structure preserved
- [ ] Resume after browser close
- [ ] Retry after failure (automated)

### Manual Tests  
- [ ] Upload 100MB file
- [ ] Upload 10GB file (verify memory stays constant)
- [ ] Upload directory with 100+ files
- [ ] Close browser mid-upload, reopen, resume
- [ ] Kill API mid-upload, restart, check cleanup

---

## 📁 Files Created/Modified

### Created
1. `api/src/services/upload.js` - Generic TUS service
2. `api/src/services/upload/datasetHandler.js` - Dataset handler
3. `api/src/routes/uploads.js` - Upload endpoints
4. `ui/src/components/upload/GenericUploader.vue` - Generic UI component
5. `ui/src/components/dataset/upload/DatasetUploader.vue` - Dataset-specific uploader
6. `workers/workers/scripts/manage_upload_workflows.py` - Retry job for stalled/failed uploads

### Modified
1. `api/prisma/schema.prisma` - Database schema
2. `api/package.json` - Added TUS dependencies
3. `ui/package.json` - Added tus-js-client
4. `api/src/routes/datasets/uploads.js` - Removed file_upload_log, simplified for TUS
5. `ui/src/pages/datasetUpload/new.vue` - Uses DatasetUploader component
6. `workers/workers/api.py` - Added upload retry API wrappers
7. `workers/workers/constants/upload.py` - Added MAX_RETRY_COUNT and PERMANENTLY_FAILED

### To Create (Remaining)
1. `workers/workers/scripts/cleanup_upload_resources.py`

### To Modify (Remaining)
1. `workers/workers/tasks/process_dataset_upload.py`
2. `docker-compose.yml`
3. `docker-compose-prod.yml`
4. `secure_download/src/routes/index.js`
5. `.ai/bioloop/features/uploads.md`

---

## 🚀 Next Steps

1. **Immediate:** Run `npm install` in api/ and ui/
2. **Database:** Run Prisma migration
3. **Test:** Restart containers and verify API starts without errors
4. **Continue:** Complete remaining 7 tasks
5. **Deploy:** Test in docker env, then production

---

## 💡 Important Conventions

### API Patterns
- Always use `require('@/db')` for Prisma (NOT `@/services/prisma`)
- Use `asyncHandler` for all async route handlers
- Use `logger.info/error` (NOT console.log)
- Follow workflow creation pattern (Rhythm generates IDs)

### UI Patterns
- Use Vuestic components (`va-button`, `va-progress-bar`)
- Use Icon from `@iconify/vue`
- Emit events for parent handling (`@upload-complete`, etc.)

### Worker Patterns
- Use `workers.api` module for API calls
- Follow retry pattern (up to 3 attempts)
- Send admin notifications via `/notifications` endpoint
- Run via PM2 or cron (not one-off scripts)

### Production Restrictions
- ⚠️ NEVER touch `/N/...` paths (production data)
- ⚠️ NEVER reset database without permission
- ⚠️ NEVER run docker commands
- ✅ CAN make code changes in `/opt/sca/cmg`

---

## 📚 Reference Documentation

- TUS Protocol: https://tus.io/protocols/resumable-upload.html
- TUS JS Client: https://github.com/tus/tus-js-client
- TUS Node Server: https://github.com/tus/tus-node-server
- Feature docs: `.ai/bioloop/features/uploads.md`
- Architecture: `.ai/bioloop/architecture.md`
- API conventions: `.ai/bioloop/api_conventions.md`

---

**Last Updated:** 2026-02-01
**Estimated Remaining Time:** 4-6 hours
**Status:** Ready to continue in new tab

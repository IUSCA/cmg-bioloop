# Upload Feature Code Cleanup - 2026-02-11

**Status:** ✅ Complete  
**Context:** Final cleanup after TUS upload rewrite in CMG

---

## Summary

This document lists all dead code and outdated configuration that was **removed from CMG** after migrating from the old chunk-based upload system to TUS resumable uploads.

**Purpose for other Bioloop forks:**
- This is a checklist of what to DELETE from the target fork if it still has the old upload implementation
- Everything listed here has already been cleaned up in CMG
- Use this as a reference when migrating another Bioloop fork to the TUS implementation

---

## Files Deleted (9 files)

### UI
1. **`ui/src/services/upload/token.js`** - Unused OAuth upload token service
2. **`ui/src/pages/datasetUpload/index.vue`** - Duplicate old upload list page (replaced by `/datasets/uploads/index.vue`)
3. **`ui/src/pages/datasetUpload/`** - Empty directory removed

### Documentation
4. **`MERGE_COMPARISON_REPORT.md`** - Outdated merge comparison file (contained `cancel_dataset_upload` references)

---

## Code Removed (11 files modified)

### Authentication & OAuth
**`api/src/services/auth.js`:**
- Removed `get_upload_token()` function (9 lines)
- Removed `get_upload_token` from exports

**`api/.env.default`:**
- Removed `OAUTH_UPLOAD_CLIENT_ID=xxx`
- Removed `OAUTH_UPLOAD_CLIENT_SECRET=xxx`

**`api/config/custom-environment-variables.json`:**
- Removed `oauth.upload.client_id` mapping
- Removed `oauth.upload.client_secret` mapping

**`api/config/default.json`:**
- Removed `oauth.upload.scope_prefix` config

**`api/bin/entrypoint.sh`:**
- Removed OAuth upload client generation logic (~10 lines)
- Removed `OAUTH_UPLOAD_CLIENT_ID` and `OAUTH_UPLOAD_CLIENT_SECRET` from environment variable check

### UI Token Mechanism
**`ui/src/stores/auth.js`:**
- Removed `uploadToken` ref
- Removed `refreshUploadToken()` method (34 lines)
- Removed `uploadTokenService` import
- Removed `refreshUploadToken` from exports

**`ui/src/components/dataset/upload/UploadDatasetStepper.vue`:**
- Removed unused `uploadToken` ref declaration

**`ui/src/config.js`:**
- Removed `refreshTokenTMinusSeconds.uploadToken: 20`

### Dependencies
**`api/package.json`:**
- Removed `"spark-md5": "^3.0.2"` (replaced by BLAKE3)

**`secure_download/package.json`:**
- Removed `"spark-md5": "^3.0.2"`

**`secure_download/config/default.json`:**
- Removed `"upload_scope": "upload_file"`

### Workflow Configuration
**`api/config/default.json`:**
- Removed `process_dataset_upload` workflow definition (7 lines)

### Route Cleanup
**`api/src/routes/index.js`:**
- Removed broken `/uploads` route reference (was already deleted during refactor)

### Documentation
**`.ai/bioloop/features/uploads.md`:**
- Removed obsolete `cancel_dataset_upload` reference in changelog

---

## Verification Results

✅ **No remaining references found for:**
- `uploadToken`, `refreshUploadToken`, `getUploadToken`
- `OAUTH_UPLOAD_CLIENT_ID`, `OAUTH_UPLOAD_CLIENT_SECRET`
- `spark-md5`, `SparkMD5`
- `process_dataset_upload`, `cancel_dataset_upload`
- `chunk_upload`, `ChunkUpload`
- `entity_type`, `entity_id` (in upload context)
- `audit_log_id` (in upload context)
- `tus_id` (renamed to `process_id`)
- `upload_scope`

✅ **All upload statuses in use:**
- `UPLOADING`, `UPLOAD_FAILED`, `UPLOADED`
- `VERIFYING`, `VERIFIED`, `VERIFICATION_FAILED`
- `PROCESSING`, `PROCESSING_FAILED`
- `COMPLETE`, `PERMANENTLY_FAILED`

✅ **No orphaned files or directories**

---

## Current Upload Authentication

**TUS Implementation uses:**
- Standard JWT Bearer token (same as all other API endpoints)
- Token from `localStorage.getItem('token')`
- Standard `authenticate` middleware in `api/src/middleware/tus.js`
- No separate OAuth upload credentials needed

**Flow:**
```
UI → localStorage.getItem('token') → TUS client headers
    → Authorization: Bearer <token>
    → api/src/middleware/tus.js → authenticate()
    → Standard JWT validation
```

---

## Documentation Updated

1. **`.ai/bioloop/features/uploads.md`** - Added comprehensive cleanup list
2. **`UPLOAD_DOCS_STATUS_REPORT.md`** - Updated with cleanup status
3. **`.ai/UPLOAD_CLEANUP_2026_02_11.md`** - This file (cleanup summary)

---

## Impact

**No functional changes** - All removed code was unused/dead code.

**Benefits:**
- Cleaner codebase (removed ~150+ lines of dead code)
- Simplified authentication (one less OAuth client to manage)
- Removed deprecated npm dependencies
- Eliminated duplicate UI pages
- Consistent with platform authentication patterns

---

**Cleanup Completed:** 2026-02-11  
**Verified By:** AI Agent comprehensive scan

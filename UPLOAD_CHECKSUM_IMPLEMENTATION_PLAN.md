# Upload Checksum Verification Implementation Plan

## Overview
Implement optional BLAKE3 manifest-based checksum verification for TUS uploads to detect corrupt/incomplete uploads before processing.

**Feature Flag:** `upload_verify_checksums` (default: `false`)

---

## Phase 1: Database Schema Changes

### 1.1 Update Prisma Schema
**File:** `api/prisma/schema.prisma`

**Changes:**
```prisma
model dataset_upload_log {
  id               Int           @id @default(autoincrement())
  status           upload_status
  
  // RENAME: tus_id → process_id (generic upload identifier)
  process_id       String?       // Generic identifier for any upload tech (TUS, S3, etc.)
  
  // KEEP: Upload metadata
  selection_mode   String?       // 'files' or 'directory'
  directory_name   String?       // Original directory name for directory uploads
  retry_count      Int           @default(0)  // Track workflow retry attempts
  
  // KEEP: Flexible metadata JSON
  metadata         Json?         // { checksum?: {...}, failure_reason?: "..." }
  
  updated_at       DateTime      @default(now()) @updatedAt @db.Timestamp(6)
  audit_log        dataset_audit @relation(fields: [audit_log_id], references: [id], onDelete: Cascade)
  audit_log_id     Int           @unique
  
  // REMOVE these columns:
  // - tus_id (renamed to process_id)
  // - file_path (only last file, useless for multi-file)
  // - file_size (only last file, useless for multi-file)
  // - failure_reason (moved to metadata.failure_reason)
}
```

### 1.2 Create Migration
```bash
cd api
npx prisma migrate dev --name rename_tus_id_to_process_id_and_cleanup_columns
```

**Migration should:**
- Rename `tus_id` → `process_id`
- Drop `file_path`, `file_size`, `failure_reason` columns
- Preserve existing data in `process_id` (copy from `tus_id`)

---

## Phase 2: Feature Flag Configuration

### 2.1 API Configuration
**File:** `api/config/default.json`

Add under `enabledFeatures`:
```json
{
  "enabledFeatures": {
    "upload_verify_checksums": false
  }
}
```

### 2.2 UI Configuration
**File:** `ui/src/config.js`

Add under `enabledFeatures`:
```javascript
export default {
  enabledFeatures: {
    upload_verify_checksums: false,
  }
}
```

### 2.3 Worker Configuration
**File:** `workers/workers/config/common.py`

Add:
```python
# Upload verification settings
UPLOAD_VERIFY_CHECKSUMS = os.environ.get('UPLOAD_VERIFY_CHECKSUMS', 'false').lower() == 'true'
```

**File:** `workers/.env.default`

Add:
```bash
# Upload checksum verification (default: false for performance)
UPLOAD_VERIFY_CHECKSUMS=false
```

---

## Phase 3: UI Implementation - Manifest Computation

### 3.1 Create Upload Service
**NEW FILE:** `ui/src/services/upload.js`

**Dependencies to add:**
```bash
cd ui
npm install blake3-wasm
```

**Implementation:**
```javascript
import * as blake3 from 'blake3-wasm';
import config from '@/config';

/**
 * Compute BLAKE3 manifest hash for uploaded files
 * Public API - follows naming convention (no underscore prefix)
 */
export async function computeManifestHash(files) {
  if (!config.enabledFeatures.upload_verify_checksums) {
    return null;  // Feature disabled
  }
  
  if (!files || files.length === 0) {
    return null;
  }
  
  return _computeBlake3Manifest(files);
}

/**
 * Internal helper to compute BLAKE3 manifest
 * Private - follows naming convention (underscore prefix)
 */
async function _computeBlake3Manifest(files) {
  // Initialize BLAKE3 WASM
  await blake3.load();
  
  const manifest = [];
  
  // 1. Hash each file
  for (const file of files) {
    const fileBytes = await file.arrayBuffer();
    const fileHash = blake3.hash(new Uint8Array(fileBytes));
    
    manifest.push({
      path: _normalizePath(file.webkitRelativePath || file.name),
      size: file.size,
      hash: fileHash.toString('hex'),
    });
  }
  
  // 2. Sort by path (deterministic order)
  manifest.sort((a, b) => a.path.localeCompare(b.path));
  
  // 3. Create canonical manifest string
  const manifestStr = [
    'blake3-manifest-v1',
    ...manifest.map(f => `${f.path}\t${f.size}\t${f.hash}`)
  ].join('\n');
  
  // 4. Hash the manifest
  const manifestBytes = new TextEncoder().encode(manifestStr);
  const manifestHash = blake3.hash(manifestBytes);
  
  return {
    algorithm: 'blake3',
    mode: files.length === 1 ? 'single' : 'manifest-v1',
    manifest_hash: manifestHash.toString('hex'),
    file_count: files.length,
    total_size: manifest.reduce((sum, f) => sum + f.size, 0),
    computed_at: new Date().toISOString(),
  };
}

/**
 * Normalize file path for cross-platform consistency
 * Private helper
 */
function _normalizePath(path) {
  // Use forward slashes, no leading ./
  return path.replace(/\\/g, '/').replace(/^\.\//, '');
}
```

### 3.2 Update Upload Component
**File:** `ui/src/components/dataset/upload/UploadDatasetStepper.vue`

**Add import:**
```javascript
import { computeManifestHash } from '@/services/upload';
```

**Update `handleTusComplete` function:**
```javascript
const handleTusComplete = async () => {
  // Compute manifest hash (if feature enabled)
  const manifestHash = await computeManifestHash(selectedFiles.value);
  
  if (manifestHash) {
    console.log('Computed upload manifest hash:', manifestHash.manifest_hash);
    
    // Store in database
    try {
      await datasetService.updateDatasetUploadLog(
        datasetUploadLog.value.audit_log.dataset.id,
        { metadata: { checksum: manifestHash } }
      );
      console.log('Manifest hash saved to database');
    } catch (error) {
      console.error('Failed to save manifest hash:', error);
      // Don't fail upload - async process will use TUS fallback
    }
  }
  
  submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOADED;
  statusChipColor.value = "success";
  submissionAlert.value = "All files have been uploaded successfully!";
  submissionAlertColor.value = "success";
  isSubmissionAlertVisible.value = true;
  submissionSuccess.value = true;
  
  // Workflow will be triggered by Python monitoring process
};
```

### 3.3 Add Dataset Service Method
**File:** `ui/src/services/dataset.js`

**Add method:**
```javascript
export function updateDatasetUploadLog(datasetId, data) {
  return api.patch(`/datasets/${datasetId}/upload-log`, data);
}
```

---

## Phase 4: API Implementation - Update Endpoints

### 4.1 Add Update Upload Log Endpoint
**File:** `api/src/routes/datasets/uploads.js`

**Add new route:**
```javascript
// Update upload log metadata (e.g., checksum)
router.patch(
  '/:id/upload-log',
  authenticate,
  checkOwnership({ idPath: 'params.id' }),
  asyncHandler(async (req, res) => {
    // #swagger.tags = ['datasets']
    // #swagger.summary = 'Update dataset upload log metadata'
    
    const datasetId = parseInt(req.params.id, 10);
    const { metadata } = req.body;
    
    // Find upload log for this dataset
    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
          create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        },
      },
    });
    
    if (!uploadLog) {
      return res.status(404).json({ error: 'Upload log not found' });
    }
    
    // Merge metadata (preserve existing fields)
    const existingMetadata = uploadLog.metadata || {};
    const updatedMetadata = { ...existingMetadata, ...metadata };
    
    const updated = await prisma.dataset_upload_log.update({
      where: { id: uploadLog.id },
      data: { metadata: updatedMetadata },
    });
    
    res.json({ success: true, upload_log: updated });
  }),
);
```

### 4.2 Update Code References
**Files to update:** All files referencing `tus_id` field

**Search and replace:**
```bash
# Find all occurrences
cd /Users/ripandey/dev/cmg-bioloop
grep -r "tus_id" api/src --include="*.js"
grep -r "\.tus_id" workers/workers --include="*.py"

# Replace programmatically or manually
```

**Update patterns:**
- `tus_id` → `process_id`
- `uploadLog.tus_id` → `uploadLog.process_id`
- `dataset_upload_log.tus_id` → `dataset_upload_log.process_id`

---

## Phase 5: Worker Implementation - Verification

### 5.1 Create Upload Verification Module
**NEW FILE:** `workers/workers/upload.py`

**Dependencies to add:**
```bash
cd workers
poetry add blake3
```

**Implementation:**
```python
"""Upload verification utilities"""
import json
from pathlib import Path
import blake3

from workers.config import config
from workers import api


def verify_upload_integrity(dataset):
    """
    Verify upload integrity using manifest hash or TUS fallback.
    
    Args:
        dataset (dict): Dataset with upload log
    
    Returns:
        bool: True if upload is verified
    
    Raises:
        Exception: If verification fails
    """
    # Check feature flag
    if not config.UPLOAD_VERIFY_CHECKSUMS:
        # Default: Use TUS Offset == Size check
        print(f"Checksum verification disabled, using TUS fallback for dataset {dataset['id']}")
        return _verify_tus_completion(dataset['id'])
    
    # Get stored manifest
    upload_log = _get_upload_log(dataset)
    manifest_data = upload_log.get('metadata', {}).get('checksum')
    
    if not manifest_data:
        print(f"No manifest hash found for dataset {dataset['id']}, falling back to TUS check")
        return _verify_tus_completion(dataset['id'])
    
    # Recompute manifest from uploaded files
    origin_path = Path(dataset['origin_path'])
    if not origin_path.exists():
        raise Exception(f"Origin path does not exist: {origin_path}")
    
    print(f"Verifying manifest hash for dataset {dataset['id']}...")
    computed_hash = _compute_manifest_hash(origin_path)
    stored_hash = manifest_data['manifest_hash']
    
    # Verify match
    if computed_hash != stored_hash:
        raise Exception(
            f"Manifest hash mismatch for dataset {dataset['id']}: "
            f"expected {stored_hash}, got {computed_hash}"
        )
    
    print(f"✓ Manifest hash verified for dataset {dataset['id']}")
    return True


def _compute_manifest_hash(origin_path):
    """
    Compute BLAKE3 manifest hash from directory.
    Matches client-side algorithm exactly.
    """
    files = sorted([f for f in origin_path.rglob('*') if f.is_file()])
    
    if not files:
        raise Exception(f"No files found at {origin_path}")
    
    manifest_lines = ['blake3-manifest-v1']
    
    for file_path in files:
        # Hash file content
        with open(file_path, 'rb') as f:
            file_hash = blake3.blake3(f.read()).hexdigest()
        
        # Relative path from origin_path
        rel_path = file_path.relative_to(origin_path)
        rel_path_str = str(rel_path).replace('\\', '/')  # Normalize to forward slashes
        
        manifest_lines.append(
            f"{rel_path_str}\t{file_path.stat().st_size}\t{file_hash}"
        )
    
    # Hash the manifest
    manifest_str = '\n'.join(manifest_lines)
    return blake3.blake3(manifest_str.encode('utf-8')).hexdigest()


def _verify_tus_completion(dataset_id):
    """
    Fallback verification: Use TUS Offset == Size check.
    
    Args:
        dataset_id (int): Dataset ID
    
    Returns:
        bool: True if TUS uploads are complete
    
    Raises:
        Exception: If no completed TUS uploads found
    """
    # Query TUS directory for completed uploads
    upload_path = Path(config.UPLOAD_PATH)
    completed_count = 0
    
    for info_file in upload_path.glob('*.info'):
        try:
            metadata = json.loads(info_file.read_text())
            
            if metadata.get('MetaData', {}).get('dataset_id') == str(dataset_id):
                # Check if complete (Offset == Size)
                if metadata['Offset'] == metadata['Size']:
                    completed_count += 1
        except Exception as e:
            print(f"Warning: Failed to read TUS info file {info_file}: {e}")
            continue
    
    if completed_count == 0:
        raise Exception(f"No completed TUS uploads found for dataset {dataset_id}")
    
    print(f"✓ Found {completed_count} completed TUS upload(s) for dataset {dataset_id}")
    return True


def _get_upload_log(dataset):
    """Helper to get upload log from dataset dict"""
    # Assuming dataset includes upload log via API join
    return dataset.get('upload_log', {})


def cleanup_upload_metadata(dataset_id):
    """
    Remove manifest data after successful integrated workflow.
    Saves database space.
    
    Args:
        dataset_id (int): Dataset ID
    """
    try:
        # Get current metadata
        upload_log = api.get_dataset_upload_log(dataset_id)
        metadata = upload_log.get('metadata', {})
        
        # Remove checksum field
        if 'checksum' in metadata:
            metadata.pop('checksum')
            
            api.update_dataset_upload_log(
                dataset_id,
                {'metadata': metadata}
            )
            
            print(f"Cleaned up manifest metadata for dataset {dataset_id}")
    except Exception as e:
        print(f"Warning: Failed to cleanup manifest for dataset {dataset_id}: {e}")
        # Don't fail the workflow for cleanup errors
```

### 5.2 Update Upload Management Script
**File:** `workers/workers/scripts/manage_upload_workflows.py`

**Add import:**
```python
from workers import upload
```

**Update `process_stalled_uploads` function:**
```python
def process_stalled_uploads(dry_run=True):
    """Process uploads that are UPLOADED but workflow hasn't started"""
    logger.info("\n--- Processing Stalled Uploads ---")
    
    summary = {'retried': 0, 'errors': 0}
    
    try:
        response = api.get_stalled_uploads()
        stalled_uploads = response.get('uploads', [])
        
        logger.info(f"Found {len(stalled_uploads)} stalled uploads")
        
        for upload_record in stalled_uploads:
            dataset_id = upload_record['dataset_id']
            dataset_name = upload_record['dataset_name']
            
            logger.info(f"\nStalled upload:")
            logger.info(f"  Dataset ID: {dataset_id}")
            logger.info(f"  Dataset Name: {dataset_name}")
            
            try:
                # Get full dataset with upload log
                dataset = api.get_dataset(dataset_id=dataset_id, workflows=True)
                
                # Verify integrity before triggering workflow
                logger.info(f"  Verifying upload integrity...")
                upload.verify_upload_integrity(dataset)
                
                # Trigger workflow if verification passes
                if dry_run:
                    logger.info(f"  [DRY RUN] Would trigger workflow for dataset {dataset_id}")
                else:
                    logger.info(f"  Triggering process_dataset_upload workflow...")
                    workflow = api.trigger_dataset_upload_workflow(
                        dataset_id=dataset_id,
                        workflow_name=WORKFLOWS['PROCESS_DATASET_UPLOAD']
                    )
                    logger.info(f"  ✓ Workflow started: {workflow.get('workflow_id', 'unknown')}")
                    summary['retried'] += 1
                    
            except Exception as e:
                logger.error(f"  ✗ Verification/trigger failed for dataset {dataset_id}: {e}")
                
                # Mark as PROCESSING_FAILED
                if not dry_run:
                    try:
                        api.update_dataset_upload(
                            uploaded_dataset_id=dataset_id,
                            log_data={
                                'status': UPLOAD_STATUS['PROCESSING_FAILED'],
                                'metadata': {'failure_reason': str(e)}
                            }
                        )
                    except Exception as update_err:
                        logger.error(f"  ✗ Failed to update status: {update_err}")
                
                summary['errors'] += 1
    
    except Exception as e:
        logger.error(f"Failed to fetch stalled uploads: {e}", exc_info=True)
        summary['errors'] += 1
    
    logger.info(f"\nStalled uploads processed: {summary['retried']} retried, {summary['errors']} errors")
    return summary
```

### 5.3 Update Process Workflow
**File:** `workers/workers/tasks/process_dataset_upload.py`

**Add cleanup after integrated workflow succeeds:**
```python
# At the end of process() function, after integrated workflow starts
from workers import upload

def process(celery_task, dataset_id, **kwargs):
    # ... existing code ...
    
    # After integrated workflow starts successfully
    if int_wf:
        # Cleanup manifest metadata to save space
        try:
            upload.cleanup_upload_metadata(dataset_id)
        except Exception as e:
            print(f"Warning: Failed to cleanup upload metadata: {e}")
            # Don't fail workflow for cleanup errors
    
    return dataset_id,
```

---

## Phase 6: API Updates for Worker Access

### 6.1 Add Worker API Methods
**File:** `workers/workers/api.py`

**Add methods:**
```python
def get_dataset_upload_log(dataset_id):
    """Get upload log for a dataset"""
    with APIServerSession() as s:
        r = s.get(f'datasets/{dataset_id}/upload-log')
        r.raise_for_status()
        return r.json()


def update_dataset_upload_log(dataset_id, log_data):
    """Update upload log metadata"""
    with APIServerSession() as s:
        r = s.patch(f'datasets/{dataset_id}/upload-log', json=log_data)
        r.raise_for_status()
        return r.json()
```

### 6.2 Add API Endpoint for Upload Log
**File:** `api/src/routes/datasets/uploads.js`

**Add GET endpoint:**
```javascript
// Get upload log for a dataset
router.get(
  '/:id/upload-log',
  authenticate,  // Worker endpoint
  asyncHandler(async (req, res) => {
    const datasetId = parseInt(req.params.id, 10);
    
    const uploadLog = await prisma.dataset_upload_log.findFirst({
      where: {
        audit_log: {
          dataset_id: datasetId,
          create_method: CONSTANTS.DATASET_CREATE_METHODS.UPLOAD,
        },
      },
    });
    
    if (!uploadLog) {
      return res.status(404).json({ error: 'Upload log not found' });
    }
    
    res.json(uploadLog);
  }),
);
```

---

## Phase 7: Documentation

### 7.1 Create Enabled Features Guide
**NEW FILE:** `.ai/bioloop/features/enabled-features.md`

```markdown
# Enabled Features Configuration

System for toggling optional features across the platform.

## Configuration Files

- **API**: `api/config/default.json` → `enabledFeatures` object
- **UI**: `ui/src/config.js` → `enabledFeatures` object  
- **Workers**: Environment variables in `workers/.env`

## Available Features

### `upload_verify_checksums`

**Default**: `false`  
**Type**: Performance optimization vs integrity verification

**Purpose**: Enable BLAKE3 manifest-based checksum verification for uploaded files.

#### When Enabled (`true`)

**Client Side:**
- Computes BLAKE3 hash for each uploaded file
- Creates canonical manifest with file paths + hashes
- Hashes the manifest → stores in database

**Server Side:**
- Recomputes manifest from uploaded files
- Verifies manifest hash matches client
- Detects corrupt/tampered/incomplete uploads
- Fails workflow if mismatch detected

**Performance Impact:**
- Adds ~1-5 seconds per GB (BLAKE3 is fast)
- Doubles I/O (client reads for hash, server reads for verification)

#### When Disabled (`false`) - Default

**Client Side:**
- No hashing performed
- Faster uploads

**Server Side:**
- Uses TUS protocol's native `Offset == Size` check
- Verifies bytes received == bytes expected
- Sufficient for detecting network errors

**Performance Impact:**
- Minimal overhead
- Recommended for most use cases

#### When to Enable

Enable if you need to detect:
- File tampering during upload
- Storage corruption after upload
- Man-in-the-middle attacks
- Specific compliance requirements

#### Configuration Examples

**Enable globally (all environments):**
```json
// api/config/default.json
{
  "enabledFeatures": {
    "upload_verify_checksums": true
  }
}
```

**Enable per environment:**
```bash
# workers/.env (production)
UPLOAD_VERIFY_CHECKSUMS=true
```

**Enable for testing:**
```javascript
// ui/src/config.js
export default {
  enabledFeatures: {
    upload_verify_checksums: process.env.NODE_ENV === 'test',
  }
}
```

## Adding New Features

1. Add to all three config files with same key
2. Default to `false` (opt-in for safety)
3. Document purpose, performance impact, use cases
4. Add feature detection in code
5. Update this guide
```

### 7.2 Update Feature Documentation
**File:** `.ai/bioloop/features/uploads.md`

**Add section at end:**
```markdown
## Checksum Verification (Optional)

**Feature Flag**: `upload_verify_checksums` (default: `false`)

### Algorithm

Uses BLAKE3 manifest-based verification:

1. **Per-file hash**: `hash_i = BLAKE3(file_bytes)`
2. **Canonical manifest**:
   ```
   blake3-manifest-v1
   path/to/file1.txt\t1024\thash1
   path/to/file2.txt\t2048\thash2
   ```
3. **Manifest hash**: `BLAKE3(manifest_bytes)`

### Verification Flow

```
Client (Browser)
  ├─ Select files
  ├─ Upload via TUS
  ├─ Compute manifest hash  ← Only if feature enabled
  └─ Store in database

Server (Async Worker)
  ├─ Find UPLOADED datasets
  ├─ Verify:
  │   ├─ If feature enabled: Recompute manifest, compare hashes
  │   └─ If feature disabled: Check TUS Offset == Size
  └─ Trigger workflow if verified
```

### Database Schema

Stored in `dataset_upload_log.metadata`:
```json
{
  "checksum": {
    "algorithm": "blake3",
    "mode": "manifest-v1",
    "manifest_hash": "abc123...",
    "file_count": 5,
    "total_size": 1048576,
    "computed_at": "2026-02-04T12:00:00Z"
  }
}
```

Cleaned up after integrated workflow succeeds to save space.
```

---

## Phase 8: Testing

### 8.1 Manual Test Cases

**Test 1: Single File Upload (Checksum Disabled)**
```bash
# Expected: Uses TUS Offset == Size check
# Should complete in ~30 seconds (stalled timeout)
```

**Test 2: Single File Upload (Checksum Enabled)**
```bash
# Enable feature
# api/config/default.json: "upload_verify_checksums": true
# ui/src/config.js: upload_verify_checksums: true
# workers/.env: UPLOAD_VERIFY_CHECKSUMS=true

# Expected: Computes hash, verifies, completes
```

**Test 3: Multiple Files Upload**
```bash
# Upload 3 files
# Expected: Manifest with 3 entries, sorted by path
```

**Test 4: Directory Upload**
```bash
# Upload directory with nested structure
# Expected: Preserves relative paths in manifest
```

**Test 5: Corrupt Upload Detection**
```bash
# 1. Upload files
# 2. Manually modify a file at origin_path before verification
# Expected: Manifest mismatch, workflow fails with clear error
```

**Test 6: Partial Upload Handling**
```bash
# 1. Upload 2 of 5 files
# 2. Wait 30 seconds
# Expected: Processes 2 files (no failure)
```

### 8.2 Automated Tests

**NEW FILE:** `workers/workers/tests/test_upload_verification.py`

```python
import pytest
from pathlib import Path
from workers import upload

def test_manifest_computation():
    """Test BLAKE3 manifest hash computation"""
    # Create test files
    test_dir = Path('/tmp/test_upload')
    test_dir.mkdir(exist_ok=True)
    
    (test_dir / 'file1.txt').write_text('hello')
    (test_dir / 'file2.txt').write_text('world')
    
    # Compute hash
    hash1 = upload._compute_manifest_hash(test_dir)
    hash2 = upload._compute_manifest_hash(test_dir)
    
    # Should be deterministic
    assert hash1 == hash2
    
    # Modify file
    (test_dir / 'file1.txt').write_text('HELLO')
    hash3 = upload._compute_manifest_hash(test_dir)
    
    # Should change
    assert hash1 != hash3

def test_tus_fallback():
    """Test TUS Offset == Size fallback"""
    # Mock TUS info files
    # Test verification passes for complete uploads
    pass
```

---

## Phase 9: Deployment Checklist

### Pre-Deployment
- [ ] Review all code changes
- [ ] Test all 6 manual test cases
- [ ] Run automated tests
- [ ] Verify feature flag works (enable/disable)
- [ ] Check database migration is reversible
- [ ] Review performance impact (< 5% overhead)

### Deployment Steps
1. [ ] Stop workers (PM2)
2. [ ] Run database migration
3. [ ] Deploy API code
4. [ ] Deploy UI code
5. [ ] Deploy worker code
6. [ ] Restart services
7. [ ] Verify stalled upload timeout (30s for testing)
8. [ ] Monitor logs for errors

### Post-Deployment Verification
- [ ] Test single file upload
- [ ] Test multi-file upload
- [ ] Check async worker processes uploads
- [ ] Verify manifest stored in database
- [ ] Confirm cleanup after integrated workflow
- [ ] Monitor performance metrics

### Rollback Plan
If issues occur:
1. Set `upload_verify_checksums: false` in all configs
2. Restart services
3. Feature will use TUS fallback automatically
4. No database rollback needed (metadata column is optional)

---

## Phase 10: Production Tuning

After testing, adjust these values:

### Stalled Upload Timeout
**File:** `api/src/routes/uploads.js`

Change from 30 seconds back to 5 minutes:
```javascript
const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
```

### PM2 Monitoring Interval
**File:** `workers/ecosystem.config.js`

Current: Every 1 minute  
Recommended for production: Every 1-2 minutes (already optimal)

### Feature Flag Default
After successful testing, consider enabling by default:
```json
{
  "enabledFeatures": {
    "upload_verify_checksums": true  // Enable if corruption is a concern
  }
}
```

---

## Success Criteria

✅ **Functional Requirements Met:**
- Upload verification works with checksums enabled/disabled
- Manifest computation is deterministic
- Worker detects corrupt uploads
- Cleanup removes manifest after success
- Partial uploads handled gracefully

✅ **Non-Functional Requirements Met:**
- Performance impact < 5% with checksums disabled
- Performance impact < 10% with checksums enabled
- Feature flag works across all components
- Modular code (upload logic decoupled)
- Comprehensive documentation

✅ **Testing Complete:**
- All 6 manual test cases pass
- Automated tests pass
- Edge cases handled (partial uploads, errors)
- Rollback plan tested

---

## Notes for Implementation Agent

### Code Organization
- Follow existing naming conventions
- Use underscore prefix for private methods in UI services
- Use module-level functions in worker scripts
- Keep upload logic separate from dataset logic

### Error Handling
- Don't fail uploads if manifest computation fails
- Fall back to TUS check if manifest missing
- Log errors but continue processing
- Clean up gracefully on failures

### Performance
- BLAKE3 is fast but still adds overhead
- Compute hashes in background if possible
- Don't block UI during hash computation
- Clean up manifest data after use

### Security
- Validate manifest structure before processing
- Sanitize file paths (no `..`, absolute paths)
- Don't expose raw file contents in logs
- Use constant-time comparison for hashes

---

## Estimated Implementation Time

- Phase 1 (Schema): 30 minutes
- Phase 2 (Config): 15 minutes
- Phase 3 (UI): 2 hours
- Phase 4 (API): 1 hour
- Phase 5 (Worker): 2 hours
- Phase 6 (API Updates): 30 minutes
- Phase 7 (Documentation): 1 hour
- Phase 8 (Testing): 2 hours
- Phase 9 (Deployment): 1 hour

**Total**: ~10 hours

---

## Future Enhancements

1. **Parallel Hash Computation**: Use Web Workers in browser
2. **Incremental Hashing**: Hash during upload, not after
3. **Server-Side Caching**: Cache manifest hashes to avoid recomputation
4. **Admin Dashboard**: Show upload verification statistics
5. **Checksum Algorithms**: Support SHA-256 as alternative
6. **Corruption Recovery**: Auto-retry failed files instead of failing entire upload

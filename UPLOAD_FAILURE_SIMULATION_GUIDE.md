# Upload Failure Simulation - Developer Testing Guide

This guide explains how to simulate upload failures in the browser to test error handling, retry logic, and the resumable upload flow.

---

## Table of Contents

1. [How the Simulation System Works](#how-the-simulation-system-works)
2. [Prerequisites & Setup](#prerequisites--setup)
3. [Test Scenarios](#test-scenarios)
   - [Scenario 1: Successful Resume After Partial Write](#scenario-1-successful-resume-after-partial-write)
   - [Scenario 2: Client Timeout with Failure UI](#scenario-2-client-timeout-with-failure-ui)
   - [Scenario 3: Manual Retry After Timeout](#scenario-3-manual-retry-after-timeout)
4. [Verification Checklist](#verification-checklist)
5. [Troubleshooting](#troubleshooting)

---

## How the Simulation System Works

The failure simulation system has three components that work together:

### 1. **Client-Side Configuration** (`localStorage`)

Set two flags in the browser console to enable simulation:

```javascript
localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '6')
```

These flags are read by the UI (`UploadDatasetStepper.vue`) and sent as HTTP headers:
- `X-Simulate-Failure: mid-upload`
- `X-Simulate-Failure-Count: 6`

### 2. **API Middleware** (`api/src/middleware/tus.js`)

The TUS middleware (mounted in `app.js` via `createTusMiddleware`) intercepts TUS upload requests and tracks how many times each upload should fail:

```javascript
// CRITICAL: Only count PATCH requests with Content-Length (actual data uploads, not HEAD/OPTIONS)
const hasUploadData = req.headers['content-length'] && parseInt(req.headers['content-length'], 10) > 0;
const shouldSimulateFailure = req.headers['x-simulate-failure'] === 'mid-upload' && req.method === 'PATCH' && hasUploadData;

if (shouldSimulateFailure && uploadId !== 'files') {
  // Initialize tracking maps
  if (!global.tusFailureSimulation) {
    global.tusFailureSimulation = new Map();
  }
  if (!global.tusFailureSimulationCount) {
    global.tusFailureSimulationCount = new Map();
  }
  
  const MAX_FAILURES = parseInt(req.headers['x-simulate-failure-count'] || '1', 10);
  const currentFailCount = global.tusFailureSimulationCount.get(uploadId) || 0;
  
  if (currentFailCount < MAX_FAILURES) {
    logger.warn(`[TUS] Marking upload ${uploadId} for mid-upload failure simulation (attempt ${currentFailCount + 1}/${MAX_FAILURES})`);
    
    // Set flag for FileStore to trigger failure on this attempt
    global.tusFailureSimulation.set(uploadId, true);
    global.tusFailureSimulationCount.set(uploadId, currentFailCount + 1);
  } else {
    logger.info(`[TUS] Upload ${uploadId} has exhausted failure quota (${currentFailCount} failures), allowing retry to proceed`);
  }
}
```

**Key Points:**
- Only counts PATCH requests with `Content-Length > 0` (actual data uploads, not HEAD/OPTIONS)
- Tracks failure count per upload ID using `global.tusFailureSimulationCount` Map
- Sets flag (`global.tusFailureSimulation`) that FileStore checks on each write
- After quota exhausted, no flag is set → upload succeeds

### 3. **FileStore Simulation** (`api/src/services/upload.js`)

The `TestableFileStore` class extends TUS's `FileStore` and overrides the `write()` method to inject failures:

```javascript
async write(stream, id, offset) {
  const shouldFail = this._shouldSimulateFailure(id); // Checks global.tusFailureSimulation
  
  if (shouldFail) {
    const fs = require('fs');
    const path = require('path');
    const { pipeline, Transform } = require('stream');
    
    const failureThreshold = 1024 * 1024; // 1MB
    const filePath = path.join(this.directory, id);
    
    // Create write stream (same as parent FileStore)
    const writeable = fs.createWriteStream(filePath, {
      flags: offset > 0 ? 'r+' : 'w',  // 'r+' on retry to append at offset
      start: offset,
    });
    
    let bytesWritten = 0;
    let cutoffTriggered = false;
    
    const cutoffTransform = new Transform({
      transform(chunk, encoding, callback) {
        if (cutoffTriggered) {
          return callback(new Error('Simulated upload failure (test mode)'));
        }
        
        const remaining = failureThreshold - bytesWritten;
        
        if (chunk.length <= remaining) {
          bytesWritten += chunk.length;
          
          // If we exactly hit threshold, schedule failure AFTER passing this chunk
          if (bytesWritten === failureThreshold) {
            cutoffTriggered = true;
            setImmediate(() => {
              cutoffTransform.destroy(new Error('Simulated upload failure (test mode)'));
            });
          }
          
          return callback(null, chunk); // Pass entire chunk downstream
        }
        
        // Chunk would overshoot: forward only what we need to reach threshold
        const partial = chunk.subarray(0, remaining);
        bytesWritten += partial.length;
        cutoffTriggered = true;
        
        setImmediate(() => {
          cutoffTransform.destroy(new Error('Simulated upload failure (test mode)'));
        });
        
        return callback(null, partial); // Pass partial chunk downstream
      }
    });
    
    return new Promise((resolve, reject) => {
      pipeline(stream, cutoffTransform, writeable, (err) => {
        if (err) {
          // Expected: error occurred after writing threshold bytes
          const error = new Error('Simulated upload failure (test mode)');
          error.status_code = 500;
          reject(error);
        } else {
          resolve(offset + bytesWritten);
        }
      });
    });
  }
  
  // Normal operation - pass through to parent
  return super.write(stream, id, offset);
}

_shouldSimulateFailure(id) {
  if (!global.tusFailureSimulation) {
    global.tusFailureSimulation = new Map();
  }
  
  const shouldFail = global.tusFailureSimulation.get(id);
  
  // Clear the flag after checking (fail only once per write attempt)
  if (shouldFail) {
    global.tusFailureSimulation.delete(id);
    return true;
  }
  
  return false;
}
```

**Key Points:**
- **Writes partial data to disk** (~1MB) before failing
- Uses Transform stream to count bytes as they pass through
- Passes chunks downstream with `callback(null, chunk)` BEFORE destroying
- Calls `setImmediate()` to destroy AFTER chunk reaches file write stream
- On retry (offset > 0), opens file with 'r+' flag and writes at specified offset
- Flag is cleared after each check, so each PATCH attempt gets one chance to fail

### 4. **TUS Retry Configuration** (`ui/src/components/dataset/upload/UploadDatasetStepper.vue`)

The TUS client is configured with increased retries and a 30-second overall timeout:

```javascript
// Overall timeout for this upload (30 seconds)
const UPLOAD_TIMEOUT_MS = 30000;
let timeoutId = setTimeout(() => {
  console.error(`[TUS-CLIENT] ⏱️  Upload TIMEOUT after ${UPLOAD_TIMEOUT_MS / 1000}s for ${file.name}`);
  if (upload) {
    upload.abort(true); // Delete partial upload on server
  }
  reject(new Error(`Upload timeout after 30 seconds - retries exhausted or server not responding`));
}, UPLOAD_TIMEOUT_MS);

upload = new tus.Upload(file, {
  endpoint,
  // Increased retries: 15 attempts total (1 initial + 14 retries)
  retryDelays: [0, 1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000, 89000, 144000, 233000, 377000],
  // ... metadata, headers, callbacks
});
```

**Key Points:**
- **Up to 15 total attempts** (1 initial + 14 retries)
- **Fibonacci-like delays**: 0s, 1s, 2s, 3s, 5s, 8s, 13s, 21s, 34s, 55s...
- **Client timeout**: 30 seconds - aborts upload if not completed by then
- Timeout calls `upload.abort(true)` which sends DELETE to server for cleanup

---

## Prerequisites & Setup

### 1. **Start Services**

```bash
# Ensure API and UI are running
docker compose up -d api
# UI should be running with hot-reload (vite dev server)
```

### 2. **Open Browser DevTools**

- Navigate to upload page: `https://localhost/datasetUpload/new`
- Open DevTools (F12)
- Go to **Console** tab (for localStorage commands and logs)

### 3. **Prepare Test File**

**Upload mode**: Single file upload (not directory upload)
**Minimum file size**: 5MB
- Smaller files may complete too quickly
- Example test file: `testfile-5mb.zip` or any 5MB+ file
- Use the same file across tests for consistency

### 4. **Open API Logs Terminal**

```bash
docker compose logs -f api | grep -E "\[TUS\]|\[FILESTORE\]"
```

---

## Test Scenarios

### Scenario 1: Successful Resume After Partial Write

**Goal**: Verify that TUS correctly resumes from the last successful offset after a mid-upload failure.

#### Setup

```javascript
// Browser console
localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '2')
```

#### Steps

1. **Select file**: Choose a 5MB+ file (single file upload, NOT directory)
2. **Fill form**: Enter dataset name (e.g., "test-upload-1"), select dataset type
3. **Start upload**: Click "Upload" button
4. **Monitor**: Watch browser console and API logs

#### Expected UI Behavior

**Browser Console Logs:**

```javascript
// Checksum computation
✓ CHECKSUM COMPUTED (BEFORE UPLOAD): { manifest_hash: "...", file_count: 1, total_size: 5242880 }

// First attempt
[TUS-CLIENT] Starting upload for file 1/1: { name: "testfile-5mb.zip", size: 5242880 }
[TUS-CLIENT] Upload progress: 0.0%
// PATCH request fails with 500

// Second attempt (after ~1s delay)
[TUS-CLIENT] Upload progress: 0.0%
// PATCH request fails with 500

// Third attempt (after ~2s delay)
[TUS-CLIENT] Upload progress: 100.0%
[TUS-CLIENT] Upload SUCCESS

⏱️  FILE UPLOAD TIME: ~3.2s
✓ Checksum available: (uses cached checksum, NOT recomputed)
```

**API Logs:**

```
# First attempt
[TUS] PATCH /uploads/files/abc123
[TUS] Marking upload abc123 for mid-upload failure simulation (attempt 1/2)
[TUS-FILESTORE] Simulating mid-upload failure for upload abc123
[TUS-FILESTORE] Transform processing chunk (multiple times)
[TUS-FILESTORE] Partial chunk written to reach threshold
[TUS-FILESTORE] SIMULATED FAILURE after writing 1048576 bytes
[TUS] Response error: Simulated upload failure

# TUS checks offset
[TUS] HEAD /uploads/files/abc123
[TUS] HEAD completed

# Second attempt (resumes from ~1MB)
[TUS] PATCH /uploads/files/abc123
[TUS] Marking upload abc123 for mid-upload failure simulation (attempt 2/2)
[TUS-FILESTORE] SIMULATED FAILURE after writing 1048576 bytes

# TUS checks offset again
[TUS] HEAD /uploads/files/abc123

# Third attempt (quota exhausted, succeeds)
[TUS] PATCH /uploads/files/abc123
[TUS] Upload abc123 has exhausted failure quota (2 failures)
[TUS-FILESTORE] write() called (no simulation)
[TUS] PATCH completed
```

#### Verification

1. **Check Upload-Offset progression** (in browser Network tab → Response Headers):
   ```
   First HEAD response:  Upload-Offset: ~1048576  (1MB written)
   Second HEAD response: Upload-Offset: ~2097152  (2MB total written)
   Final PATCH success:  Upload-Offset: 5242880   (complete file)
   ```

2. **Verify partial file existed**:
   ```bash
   # During upload (before completion):
   docker compose exec -T api ls -lh /opt/sca/data/uploads/
   # Should show file with partial size (~1-2MB)
   # Example output: -rw-r--r-- 1 node node 2.0M Feb 10 17:19 abc123
   ```

3. **Verify upload completed**:
   - UI shows "Upload Complete" with green checkmark
   - Dataset appears in dashboard (e.g., "test-upload-1")
   - File moved to `/opt/sca/data/datasets/USERNAME/DATASET_NAME/testfile-5mb.zip`

#### Expected Timing

- **Total time**: ~3-4 seconds
- Retry delays: 0s + 1s + 2s = 3s cumulative

---

### Scenario 2: Client Timeout with Failure UI

**Goal**: Verify that the 30-second client timeout triggers and shows the failure UI with Retry button.

#### Setup

```javascript
// Browser console
localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '6')
```

**Why 6 failures?**
- TUS retry delays: 0s, 1s, 2s, 3s, 5s, 8s = 19s cumulative
- After 6th failure, 7th attempt starts at ~19s with 13s delay
- 19s + 13s = 32s > 30s timeout threshold

#### Steps

1. **Select file**: Choose a 5MB+ file (single file upload, NOT directory)
2. **Fill form**: Enter dataset name (e.g., "test-upload-2"), select dataset type
3. **Start upload**: Click "Upload" button
4. **Wait**: Let it run for 30+ seconds (do not interact with browser)
5. **Observe**: Timeout should trigger, UI should show failure

#### Expected UI Behavior

**Browser Console Logs:**

```javascript
// Checksum computation
✓ CHECKSUM COMPUTED (BEFORE UPLOAD): { manifest_hash: "...", total_size: 5242880 }

// Multiple failed attempts
[TUS-CLIENT] Starting upload for file 1/1: { name: "testfile-5mb.zip" }
[TUS-CLIENT] Upload progress: 0.0%
// PATCH fails (attempt 1) - 500 Internal Server Error

[TUS-CLIENT] Upload progress: 0.0%
// PATCH fails (attempt 2) - 500 Internal Server Error

// ... more failures (attempts 3, 4, 5, 6) ...

// After 30 seconds
[TUS-CLIENT] ⏱️  Upload TIMEOUT after 30s for testfile-5mb.zip
[TUS-CLIENT] Aborting upload due to timeout...
[TUS-CLIENT] One or more uploads failed: { 
  error_message: "Upload timeout after 30 seconds - retries exhausted or server not responding",
  total_files: 1,
  uploaded_count: 0 
}
```

**UI Display:**
- ❌ Status shows: "Upload Failed"
- 🔁 "Retry" button becomes visible
- ⏱️  Progress indicator stops

**API Logs:**

```
# Multiple failure cycles
[TUS] PATCH /uploads/files/xyz789
[TUS] Marking upload xyz789 (attempt 1/6)
[TUS-FILESTORE] SIMULATED FAILURE after writing 1048576 bytes

[TUS] HEAD /uploads/files/xyz789
[TUS] PATCH /uploads/files/xyz789
[TUS] Marking upload xyz789 (attempt 2/6)
[TUS-FILESTORE] SIMULATED FAILURE after writing 1048576 bytes

# ... continues until attempt 6/6 ...

# Client aborts after timeout
[TUS] DELETE /uploads/files/xyz789
[TUS] DELETE completed
```

#### Verification

1. **Check failure count**:
   ```bash
   # Count "SIMULATED FAILURE" lines in logs
   docker compose logs api --since 1m | grep "xyz789" | grep -c "SIMULATED FAILURE"
   # Should show: 6
   ```

2. **Check DELETE was called**:
   ```bash
   docker compose logs api --since 1m | grep "xyz789" | grep DELETE
   # Should show: DELETE /uploads/files/xyz789
   ```

3. **Verify UI state**:
   - "Upload Failed" message visible
   - "Retry" button is enabled
   - Upload progress is 0%
   - Status is NOT "Uploading"

#### Expected Timing

- **Total time**: ~30-32 seconds
- 6 failures with cumulative delays exceeding 30s

---

### Scenario 3: Manual Retry After Timeout

**Goal**: Verify that clicking "Retry" after a timeout succeeds without recomputing checksums.

#### Prerequisites

- Complete **Scenario 2** first (upload failed with timeout)
- UI showing "Upload Failed" with "Retry" button

#### Setup

**BEFORE clicking Retry, clear simulation flags:**

```javascript
// Browser console
localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')
localStorage.removeItem('SIMULATE_UPLOAD_FAILURE_COUNT')
```

**Why clear first?**
- The "Retry" button creates a NEW upload with a NEW upload ID
- Failure counters are tracked per upload ID
- If flags remain, the new upload will also fail 6 times
- Clearing flags ensures the retry succeeds

#### Steps

1. **Clear flags** (see above)
2. **Click "Retry"** button in UI
3. **Monitor**: Watch console and API logs

#### Expected UI Behavior

**Browser Console Logs:**

```javascript
// Pre-upload registration (UPDATE existing log)
[PRE-UPLOAD] Starting pre-upload registration { is_update: true, existing_log_id: 252 }
[PRE-UPLOAD] SUCCESS: Upload log created/updated

// Checksum verification
=== CHECKSUM VERIFICATION CHECK (BEFORE UPLOAD) ===
Feature enabled? true
Files to hash: 1
Already computed? YES  // ✅ NOT recomputed!
✓ Using previously computed checksum (skipping re-computation on retry)
Cached checksum: { manifest_hash: "...", file_count: 1, total_size: 5242880 }

// Upload starts (no simulation)
[TUS-CLIENT] Starting upload for file 1/1: { name: "testfile-5mb.zip", simulate_failure: "none" }
[TUS-CLIENT] Upload progress: 0.0%
[TUS-CLIENT] Upload progress: 100.0%
[TUS-CLIENT] Upload SUCCESS { process_id: "def456", file_size: 5242880 }

⏱️  FILE UPLOAD TIME: ~0.1s  // Fast! No retries
[UPLOAD-COMPLETE] API call SUCCESS
```

**API Logs:**

```
# Single successful attempt
[TUS] POST /uploads/files (create new upload resource)
[TUS] PATCH /uploads/files/def456
[TUS-FILESTORE] _shouldSimulateFailure check for def456
[TUS-FILESTORE] write() called for upload def456 (shouldFail: false)
[TUS] PATCH completed  // Success!

# Upload completion
[UPLOAD-COMPLETE] Starting upload completion
[UPLOAD-COMPLETE] TUS file found
[UPLOAD-COMPLETE] Moving file
[UPLOAD-COMPLETE] SUCCESS: Upload completed
```

#### Verification

1. **Checksum NOT recomputed**:
   ```javascript
   // Browser console should show:
   "Already computed? YES"
   "Using previously computed checksum"
   // NOT: "✓ STARTING checksum computation"
   ```

2. **No simulation triggered**:
   ```bash
   # API logs should NOT contain:
   docker compose logs api --since 1m | grep "def456" | grep "SIMULATED"
   # (Should return nothing)
   ```

3. **Upload completed**:
   - UI shows "Upload Complete" with green checkmark
   - File moved to dataset directory
   - Dataset visible in dashboard (e.g., "test-upload-2")
   - Upload log status: `UPLOADED`

4. **Database state**:
   ```bash
   docker compose exec -T postgres psql -U appuser -d app -c \
     "SELECT d.name, dul.status, dul.retry_count 
      FROM dataset_upload_log dul 
      JOIN dataset_audit da ON da.id = dul.audit_log_id 
      JOIN dataset d ON d.id = da.dataset_id 
      WHERE d.name = 'test-upload-2'
      ORDER BY dul.updated_at DESC LIMIT 1"
   # Should show: status = UPLOADED, retry_count = 0 or 1
   ```

#### Expected Timing

- **Total time**: ~0.1-0.2 seconds (single attempt, no retries)

---

## Verification Checklist

After running all scenarios, verify these behaviors:

### ✅ Partial Data Write

- [ ] API logs show "SIMULATED FAILURE after writing 1048576 bytes"
- [ ] Partial file exists on disk during upload (can check with `ls`)
- [ ] HEAD requests return `Upload-Offset` header with byte count
- [ ] Each retry uploads from last offset, not from zero

### ✅ TUS Resume Logic

- [ ] Upload-Offset increases after each failure: ~1MB → ~2MB → ~3MB
- [ ] Second PATCH has `Content-Length` less than first PATCH
- [ ] No duplicate data written (file size = total uploaded, not sum of attempts)

### ✅ Checksum Behavior

- [ ] Checksum computed BEFORE first upload attempt
- [ ] Checksum NOT recomputed on automatic TUS retries
- [ ] Checksum NOT recomputed on manual "Retry" button click
- [ ] Cached checksum used from `computedChecksum` reactive variable

### ✅ Timeout & Retry

- [ ] 30-second timeout triggers after multiple failures
- [ ] UI shows "Upload Failed" status
- [ ] "Retry" button appears and is clickable
- [ ] DELETE request sent to clean up failed upload
- [ ] Manual retry creates NEW upload ID and succeeds

### ✅ Error Handling

- [ ] No unhandled errors or process crashes
- [ ] No "Premature close" errors
- [ ] TUS errors properly caught and displayed
- [ ] Upload log status reflects failure state

---

## Troubleshooting

### Issue: "Upload succeeded but no failures occurred"

**Cause**: Middleware didn't detect the PATCH as a data upload.

**Fix**: Verify localStorage flags are set AND the PATCH has `Content-Length > 0`:
```javascript
// Check in Network tab:
// Request Headers should show:
X-Simulate-Failure: mid-upload
X-Simulate-Failure-Count: 6
Content-Length: 1048576
```

### Issue: "Premature close" errors in API logs

**Cause**: Stream destroyed before data written to disk.

**Fix**: Check FileStore implementation - ensure `callback(null, chunk)` is called BEFORE `setImmediate(() => destroy())`.

### Issue: "No partial file on disk"

**Cause**: Error thrown before `pipeline()` writes data.

**Fix**: Verify Transform stream is passing chunks downstream with `callback(null, chunk)`.

### Issue: "Upload timeout doesn't trigger"

**Cause**: Retry delays don't exceed 30 seconds.

**Fix**: Use `FAILURE_COUNT >= 6` to ensure cumulative delays exceed 30s.

### Issue: "Checksum recomputed on retry"

**Cause**: `computedChecksum` ref cleared or not persisted.

**Fix**: Check `computedChecksum.value` is set and retained across retry attempts. Should see "Already computed? YES" in logs.

---

## Cleanup After Testing

```javascript
// Browser console
localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')
localStorage.removeItem('SIMULATE_UPLOAD_FAILURE_COUNT')

// Verify
localStorage.getItem('SIMULATE_UPLOAD_FAILURE')  // Should return: null
```

```bash
# Clean up test uploads
docker compose exec -T api rm -rf /opt/sca/data/uploads/*

# Check test datasets (don't delete production data!)
docker compose exec -T postgres psql -U appuser -d app -c \
  "SELECT id, name, created_at FROM dataset WHERE name LIKE 'up%' ORDER BY created_at DESC LIMIT 10"
```

---

## Summary

This simulation system allows testing the full upload failure and recovery flow:

1. **Partial writes**: FileStore writes ~1MB before failing
2. **TUS resume**: HEAD checks offset, PATCH resumes from there
3. **Client timeout**: 30s limit triggers failure UI
4. **Manual retry**: Works without recomputing checksums
5. **Verification**: Logs and network tab show exact behavior

All tests confirm **resumable uploads work correctly** with proper error handling and user feedback.

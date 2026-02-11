# Upload Failure Testing Guide

This guide explains how to test various upload failure scenarios.

## Prerequisites

- API and UI services running
- Browser DevTools console open (F12 → Console)
- Terminal tailing API logs: `docker compose logs -tf api | grep -E "\[TUS\]|\[UPLOAD-"`

## Failure Scenarios

### ✅ Scenario 3: PATCH /uploads/files fails mid-upload (IMPLEMENTED) 🎯

**Description**: Simulates a failure that occurs AFTER some data has been written to disk. This properly tests TUS's resumable upload capability - on retry, TUS will resume from where it left off instead of starting over.

**When to use**: Test TUS resumable upload retry logic and verify that:
- Partial data is written to disk before failure
- On retry, upload resumes from last successful offset (not from beginning)
- UI shows proper error messages and retry behavior
- Checksum is NOT recomputed on retry

**How it works internally**:
1. Middleware detects `X-Simulate-Failure: mid-upload` header
2. Sets a flag for that specific upload ID
3. TUS starts receiving data normally
4. Custom `TestableFileStore` checks the flag during write operation
5. After writing ~1MB to disk, FileStore throws an error
6. TUS returns 500 error to client (with partial file on disk)
7. On retry, TUS detects partial file and sends HEAD request
8. Client resumes upload from last successful offset

**How to enable**:

1. **In browser console**, run:
   ```javascript
   // Test automatic retry success (quick test)
   localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
   localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '2')
   // Uploads will fail 2 times, then succeed on 3rd try (~3s total)
   
   // OR: Test 30-second timeout with failure UI (recommended)
   localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
   localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '6')
   // Uploads will fail 6 times (cumulative delays: 0+1+2+3+5+8 = 19s)
   // Each failure writes ~1MB, retries from offset
   // After 6th failure, TUS continues retrying but 30s timeout triggers
   // User will see "Upload Failed" with "Retry" button
   
   // TUS Configuration: Up to 15 attempts (1 initial + 14 retries)
   // Retry delays: 0s, 1s, 2s, 3s, 5s, 8s, 13s, 21s, 34s, 55s...
   ```

2. **Trigger**: The failure will occur automatically during the next PATCH request after ~1MB of data has been **written to disk**.

3. **Expected behavior with FAILURE_COUNT = 1** (default):
   - Upload starts normally (POST succeeds, PATCH begins)
   - ~1MB gets written to disk
   - Server returns 500 error: "Simulated upload failure (test mode)"
   - Browser console shows: `[TUS-CLIENT] Upload FAILED for <filename>`
   - **TUS client automatically retries** (5 attempts: 0s, 3s, 5s, 10s, 20s delays)
   - **Each retry resumes from 1MB offset** (not from 0)
   - API logs show:
     ```
     [TUS] Marking upload abc123 for mid-upload failure simulation
     [TUS-FILESTORE] SIMULATED FAILURE after writing 1048576 bytes
     [TUS] HEAD /uploads/files/abc123 (TUS checking current offset)
     [TUS] PATCH /uploads/files/abc123 (resume from offset 1048576)
     ```
   - Second PATCH succeeds (failure quota exhausted)
   - **Upload completes on 2nd attempt** (resumes from 1MB)
   - **Checksum is NOT recomputed on retry** (cached from initial computation)

4. **Expected behavior with FAILURE_COUNT = 2 or higher**:
   - Same as above, but multiple retries will fail
   - Each retry writes ~1MB, then fails
   - After multiple failures, if **30 seconds total elapsed**:
     - Console shows: `[TUS-CLIENT] ⏱️  Upload TIMEOUT after 30s`
     - Upload is aborted: `upload.abort(true)`
     - UI shows "Upload Failed" status
     - "Retry" button becomes available
   - On manual retry: Checksum is NOT recomputed (cached)

5. **To disable**:
   ```javascript
   localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')
   localStorage.removeItem('SIMULATE_UPLOAD_FAILURE_COUNT')
   ```

**Tuning failure point**:

To change when the failure occurs, edit `/api/src/services/upload.js` in `TestableFileStore`:
```javascript
const failureThreshold = 1024 * 1024; // Change this value
// Examples:
// 512 * 1024        → Fail after 512KB
// 5 * 1024 * 1024   → Fail after 5MB
// 100 * 1024        → Fail after 100KB
```

**Why this approach works**:
- ✅ Doesn't consume request stream (TUS can read it)
- ✅ Actually writes data to disk (partial file exists for resumption)
- ✅ Fails after real work is done (tests true resume capability)
- ✅ Fails only once per upload (subsequent retries succeed)
- ✅ Clean error handling (no "headers already sent" issues)

---

### ⏳ Scenario 1: POST /datasets/uploads fails (TODO)

**Description**: Initial dataset creation fails before any file upload begins.

**Implementation needed**:
- Add header check in `/api/src/routes/datasets/uploads.js` POST `/` endpoint
- Trigger with `X-Simulate-Failure: dataset-creation`
- Return 500 error before creating dataset

**Expected behavior**:
- User clicks "Upload" button
- API returns error immediately
- UI shows "Failed to create dataset. Please retry."
- No TUS upload initiated
- Dataset not created in database

---

### ⏳ Scenario 2: POST /uploads/files fails (TODO)

**Description**: TUS upload initiation fails (first request to create upload resource).

**Implementation needed**:
- Add header check in `/api/src/app.js` TUS middleware for POST requests
- Trigger with `X-Simulate-Failure: tus-initiation`
- Return 500 before TUS creates upload resource

**Expected behavior**:
- Dataset created successfully
- TUS POST request fails
- Browser console shows: `[TUS-CLIENT] Upload FAILED: Failed to create upload`
- UI shows "Upload initiation failed. Please retry."
- No upload file created on server

---

## Real-World Testing (Manual Network Interruption)

For more realistic testing without simulation flags:

1. **Browser DevTools Network Tab**:
   - Open DevTools → Network tab
   - Start upload
   - Right-click on an ongoing PATCH request → "Block request URL"
   - Or: Change throttling to "Offline" mid-upload

2. **Docker Network Disruption**:
   ```bash
   # Pause API container mid-upload
   docker compose pause api
   
   # Wait a few seconds, then resume
   docker compose unpause api
   ```

3. **Kill API Process**:
   ```bash
   # Stop API container abruptly during upload
   docker compose stop api
   
   # Restart after upload fails
   docker compose start api
   ```

---

## Monitoring & Debugging

### Browser Console Logs

Key prefixes to look for:
- `[TUS-CLIENT]` - Upload initiation, progress, errors
- `🧪 [TEST MODE]` - Failure simulation active
- `[PRE-UPLOAD]` - Dataset creation
- `[UPLOAD-COMPLETE]` - Completion API call

### API Logs

```bash
docker compose logs -tf api | grep -E "\[TUS\]|\[UPLOAD-"
```

Key prefixes:
- `[TUS]` - TUS protocol operations
- `[TUS] SIMULATING MID-UPLOAD FAILURE` - Simulation active
- `[UPLOAD-CREATE]` - Dataset creation
- `[UPLOAD-COMPLETE]` - Upload completion

### Database State

Check upload status:
```bash
docker compose exec -T postgres psql -U appuser -d app -c \
  "SELECT d.id, d.name, dul.status, dul.retry_count, dul.updated_at 
   FROM dataset_upload_log dul 
   JOIN dataset_audit da ON da.id = dul.audit_log_id 
   JOIN dataset d ON d.id = da.dataset_id 
   ORDER BY dul.updated_at DESC LIMIT 10"
```

---

## Success Criteria

After implementing all scenarios, verify:

### ✅ Checksum Behavior
- [ ] Checksum computed before upload starts
- [ ] **Checksum NOT recomputed on retry** (cached)
- [ ] Console shows: "Using previously computed checksum (skipping re-computation on retry)"

### ✅ TUS Retry Behavior
- [ ] TUS automatically retries (5 attempts with delays: 0s, 3s, 5s, 10s, 20s)
- [ ] Upload resumes from last successful offset
- [ ] Progress bar reflects resumption point

### ✅ UI/UX
- [ ] Clear error messages indicating failure type
- [ ] "Retry" button appears after all auto-retries exhausted
- [ ] Upload progress shows "Upload Failed" status
- [ ] Manual retry works without recomputing checksum

### ✅ API Error Handling
- [ ] Failed uploads logged with proper status
- [ ] Retry count incremented in database
- [ ] Partial upload files cleaned up (or kept for resume)

---

## Cleanup After Testing

```javascript
// Browser console
localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')
```

```bash
# Remove test uploads
docker compose exec -T api rm -rf /opt/sca/data/uploads/*
```

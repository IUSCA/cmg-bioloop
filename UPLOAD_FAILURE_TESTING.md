# Upload Failure Testing Guide

This guide explains how to test various upload failure scenarios.

## Prerequisites

- API and UI services running
- Browser DevTools console open (F12 → Console)
- Terminal tailing API logs: `docker compose logs -tf api | grep -E "\[TUS\]|\[UPLOAD-"`

## Failure Scenarios

### ✅ Scenario 3: PATCH /uploads/files fails mid-upload (IMPLEMENTED)

**Description**: Simulates a network error that occurs after upload has started but before it completes. The server receives partial data, then the connection is abruptly terminated.

**When to use**: Test TUS resumable upload retry logic and ensure the UI shows proper error messages and retry options.

**How to enable**:

1. **In browser console**, run:
   ```javascript
   localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload')
   ```

2. **Trigger**: The failure will occur automatically during the next PATCH request after 1MB of data has been received by the server.

3. **Expected behavior**:
   - Upload starts normally
   - After ~1MB transferred, connection is destroyed
   - Browser console shows: `[TUS-CLIENT] Upload FAILED for <filename>`
   - TUS client automatically retries (up to 5 times with exponential backoff)
   - API logs show: `[TUS] SIMULATED FAILURE: Destroying connection after X bytes`
   - If all retries fail: User sees "Upload Failed" with retry button
   - **Checksum should NOT be recomputed on retry** (cached from initial computation)

4. **To disable**:
   ```javascript
   localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')
   ```

**Tuning failure point**:

To change when the failure occurs, edit `/api/src/app.js`:
```javascript
const failureThreshold = 1024 * 1024; // Change this value
// Examples:
// 512 * 1024        → Fail after 512KB
// 5 * 1024 * 1024   → Fail after 5MB
// 100 * 1024        → Fail after 100KB
```

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

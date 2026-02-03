# TUS Upload Debugging Session - 2026-02-03

## Session Summary

**Duration:** ~2 hours  
**Status:** TUS server integrated, debugging response writing issue  
**Next Chat Should:** Fix TUS server response handling

---

## What Was Accomplished ✅

### 1. Fixed Authentication
- **Issue:** JWT token was being passed as a Vue ref object
- **Solution:** Use `localStorage.getItem('token')` directly in TUS client
- **File:** `ui/src/components/dataset/upload/UploadDatasetStepper.vue`

### 2. Fixed API Routing
- **Issue:** 404 errors on `/api/uploads/files`
- **Root Cause:** Test script included `/api` prefix, but API listens directly on `/uploads`
- **Solution:** Removed `/api` from test script endpoint (UI adds it via proxy config)
- **Files:** `api/src/scripts/test_tus_upload.js`

### 3. Fixed Constants References
- **Issue:** `Cannot read properties of undefined (reading 'UPLOAD')`
- **Root Cause:** Used `CONSTANTS.CREATE_METHODS.UPLOAD` but constant is `CONSTANTS.DATASET_CREATE_METHODS.UPLOAD`
- **Solution:** Updated all references in dataset handler
- **File:** `api/src/services/upload/datasetHandler.js`

### 4. Fixed Prisma Relations
- **Issue:** `Unknown field 'files' for include statement on model 'dataset_upload_log'`
- **Root Cause:** Removed `file_upload_log` table in TUS migration, but constants still referenced it
- **Solution:** Removed `files` relation from `INCLUDE_DATASET_UPLOAD_LOG_RELATIONS` and `INCLUDE_AUDIT_LOGS`
- **File:** `api/src/constants.js`

### 5. Integrated TUS into Existing UI
- **Preserved:** Original UploadDatasetStepper layout (user had hours of UI/validation work)
- **Added:** TUS client integration in step 3
- **Added:** Two-card layout (metadata + file list) with overall upload progress
- **Removed:** Individual file progress tracking (TUS handles internally)
- **File:** `ui/src/components/dataset/upload/UploadDatasetStepper.vue`

### 6. Created Test Script
- **Purpose:** Test TUS upload from inside API container
- **Location:** `api/src/scripts/test_tus_upload.js`
- **Usage:** `docker compose exec api node /opt/sca/app/src/scripts/test_tus_upload.js <dataset_id>`
- **Requires:** `TEMP_AGENT_ACCESS` env var with JWT token

---

## Current Error ❌

**Error Message:**
```
Cannot read properties of undefined (reading 'write')
```

**HTTP Status:** 500 Internal Server Error

**What's Happening:**
1. ✅ Request reaches `/uploads/files`
2. ✅ Authentication succeeds
3. ✅ TUS middleware executes
4. ✅ Dataset handler validation passes
5. ✅ Handler completes successfully
6. ❌ TUS server cannot write response back to client

**API Logs:**
```
[Router] POST /uploads/files (originalUrl: /uploads/files)
TUS handler: POST /files (originalUrl: /uploads/files)
2026-02-03 09:17:20 info: Upload create: type=dataset, id=7589, tus_id=976a20baca781749aa6ffe71f63ed78f 
2026-02-03 09:17:20 info: Dataset upload create: dataset_id=7589 
2026-02-03 09:17:21 info: Dataset 7589 upload validation passed 
2026-02-03 09:17:21 info: Upload create completed successfully for: dataset:7589 
POST /uploads/files 500 277.534 ms - 93
```

**Hypothesis:**
The TUS server's `handle()` method expects raw `req`/`res` objects, but Express middleware may have modified them or the response may have been partially sent.

---

## Files Modified

### API Files
1. **`api/src/routes/uploads.js`**
   - Fixed authentication (removed duplicate `authenticate` middleware)
   - Mounted TUS server at `/files` using `router.all()`
   - Added test endpoint `/test` (for debugging)
   - **Debug code to remove:** console.log statements

2. **`api/src/routes/index.js`**
   - Added debug logging middleware
   - **Debug code to remove:** console.log in router.use middleware

3. **`api/src/services/upload/datasetHandler.js`**
   - Fixed: `CONSTANTS.CREATE_METHODS.UPLOAD` → `CONSTANTS.DATASET_CREATE_METHODS.UPLOAD` (3 occurrences)

4. **`api/src/constants.js`**
   - Removed `files` relation from `INCLUDE_DATASET_UPLOAD_LOG_RELATIONS`
   - Removed `files` field from `INCLUDE_AUDIT_LOGS.audit_logs.include.upload.select`

5. **`api/src/scripts/test_tus_upload.js`** (NEW)
   - Test script for TUS uploads
   - Fixed endpoint: `/api/uploads/files` → `/uploads/files`

6. **`api/package.json`**
   - Added: `tus-js-client` (for test script)

### UI Files
1. **`ui/src/components/dataset/upload/UploadDatasetStepper.vue`**
   - Imported `tus-js-client`
   - Removed `GenericUploader` component usage
   - Added TUS progress state: `tusOverallProgress`, `tusFilesUploaded`, `tusTotalFiles`
   - Modified `onSubmit()` to call `uploadFilesWithTus()`
   - Added `uploadFilesWithTus()` function
   - Modified step 3 template to show two-card layout always
   - Fixed token retrieval: `auth.token` → `localStorage.getItem('token')`
   - Removed `files_metadata` from `uploadFormData`
   - Simplified `preUpload()` (removed checksum calculation)
   - Simplified `postSubmit()` (removed file status updates)
   - Added `handleTusComplete()`, `handleTusProgress()`, `handleTusError()`

2. **`ui/src/pages/datasetUpload/new.vue`**
   - Verified: Still uses `UploadDatasetStepper` (no changes needed)

---

## Next Steps to Fix TUS Response Issue

### Option 1: Mount TUS Server Differently
The TUS server's `handle()` method may need to be the ONLY handler for the route, not wrapped in Express routing.

**Try:**
```javascript
// In api/src/app.js, BEFORE mounting routes
const uploadService = require('./services/upload');
const tusServer = uploadService.getServer();

// Mount TUS directly, with custom authentication middleware
app.all(
  '/uploads/files/*',
  authenticate,
  (req, res) => tusServer.handle(req, res)
);
app.all(
  '/uploads/files',
  authenticate,
  (req, res) => tusServer.handle(req, res)
);

// Then mount other routes
app.use('/', indexRouter);
```

### Option 2: Check TUS Server Path Configuration
The TUS server path config might not match Express routing:

**Current:**
```javascript
// In api/src/services/upload.js
path: '/files',  // TUS server receives '/files' after Express strips '/uploads'
```

**Try:**
```javascript
path: '/uploads/files',  // Full path TUS should match
```

### Option 3: Remove Response Middleware
Express middleware (like `compression`, `morgan`) might interfere with TUS's raw response writing.

**Try:**
```javascript
// In api/src/app.js
app.use((req, res, next) => {
  if (req.path.startsWith('/uploads/files')) {
    // Skip compression and other middleware for TUS routes
    return next();
  }
  compression()(req, res, next);
});
```

### Option 4: Check @tus/server Documentation
Look for Express integration examples in [@tus/server docs](https://github.com/tus/tus-node-server).

### Option 5: Add Error Handler to TUS Server
```javascript
// In api/src/services/upload.js
this.tusServer = new Server({
  path: '/files',
  // ... other config
  onIncomingRequest: (req, res) => {
    console.log('TUS incoming:', req.method, req.url);
  },
  onResponseError: (req, res, err) => {
    console.error('TUS response error:', err);
    logger.error('TUS response error:', err);
  },
});
```

---

## Debug Commands

### Test TUS Upload
```bash
# Inside API container
docker compose exec api bash
export TEMP_AGENT_ACCESS="<jwt_token>"
node /opt/sca/app/src/scripts/test_tus_upload.js 7589
```

### Generate JWT Token
```bash
docker compose exec api node /opt/sca/app/src/scripts/issue_token.js ripandey
```

### Check API Logs
```bash
docker compose logs api --tail 50 --follow
```

### Restart API (if needed)
```bash
docker compose restart api
```

---

## Cleanup Tasks (After Fix)

1. Remove debug console.log from `api/src/routes/uploads.js`
2. Remove debug console.log from `api/src/routes/index.js`
3. Remove test endpoint `/uploads/test` from `api/src/routes/uploads.js`
4. Remove debug logging from `api/src/services/upload.js`
5. Update `TUS_UPLOAD_IMPLEMENTATION_SUMMARY.md` with completion status

---

## Important Notes

- **Database:** No database changes were made (user explicitly requested no DB resets)
- **UI Layout:** Preserved existing stepper layout (user had hours of work invested)
- **Production:** This is the production environment - be cautious with changes
- **Token Storage:** Always use `localStorage.getItem('token')` for TUS, not reactive Vue refs

---

## Success Criteria

When the fix is working:
1. Test script completes without errors
2. API logs show "Upload successful" or similar
3. File appears in `/opt/sca/data/uploads/`
4. Database `dataset_upload_log` record created with `tus_id`
5. TUS client receives 201 Created response with `Location` header

---

## References

- TUS Protocol Spec: https://tus.io/protocols/resumable-upload
- @tus/server GitHub: https://github.com/tus/tus-node-server
- tus-js-client GitHub: https://github.com/tus/tus-js-client
- Implementation Summary: `TUS_UPLOAD_IMPLEMENTATION_SUMMARY.md`

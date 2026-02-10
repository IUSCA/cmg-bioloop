# Concurrent Archival Implementation Summary

## ✅ Implementation Complete

Successfully implemented coordination between Bioloop and CMG for concurrent dataset archival to prevent SDA upload conflicts.

---

## 📝 Files Modified

### 1. `workers/workers/cmg_api.py`
**Added 3 new functions:**

- `get_dataset_by_origin_path(origin_path: str) -> dict | None`
  - Queries CMG API: `GET /api/legacy-migration/datasets?origin_path=...`
  - Returns dataset object or None if not found
  
- `get_dataproduct_by_origin_path(origin_path: str) -> dict | None`
  - Queries CMG API: `GET /api/legacy-migration/dataproducts?origin_path=...`
  - Returns dataproduct object or None if not found
  
- `is_dataset_archived_in_cmg(origin_path: str, dataset_type: str) -> bool`
  - Checks if CMG has completed archival
  - For RAW_DATA: checks `dataset.archived === true`
  - For DATA_PRODUCT: checks `dataproduct.paths.archive` is set

**API Response Format:**
```json
{
  "success": true,
  "found": true,
  "normalized_path": "/path/to/dataset",
  "dataset": {
    "_id": "69848419e62d88fe0f0b3766",
    "name": "dataset_name",
    "archived": false,
    "taken": null,
    ...
  }
}
```

### 2. `workers/workers/tasks/await_stability.py`
**Added CMG check at end of stability wait:**

```python
# After stability check completes:
1. Query CMG API by origin_path
2. If found, extract cmg_id (MongoDB _id)
3. Persist cmg_id to Bioloop database
4. If CMG check fails, log warning and continue
```

**Key features:**
- Non-blocking: failures don't stop workflow
- Logs detailed information about CMG status
- Handles both RAW_DATA and DATA_PRODUCT types

### 3. `workers/workers/tasks/archive.py`
**Added new function:** `wait_for_cmg_archival()`

```python
def wait_for_cmg_archival(
    dataset_name: str,
    origin_path: str,
    dataset_type: str,
    celery_task: WorkflowTask = None,
    poll_interval_seconds: int = 300,    # 5 minutes
    timeout_seconds: int = 86400         # 24 hours
) -> None:
    # Polls CMG every 5 minutes
    # Checks if archival is complete
    # Updates Celery progress
    # Raises TimeoutError if timeout exceeded
```

**Modified `archive()` function with STRICT VALIDATION:**

```python
# Decision flow:
if (cmg_id exists AND use_sda):
    # Dataset registered in CMG concurrently
    
    if (legacy_migration_incomplete):
        # STRICT VALIDATION MODE (Production Safety)
        verify_cmg_api_accessible()          # FAIL if CMG unreachable
        verify_dataset_in_cmg()              # FAIL if not found
    
    wait_for_cmg_archival(origin_path, dataset_type)
    
    # MANDATORY VERIFICATION
    verify_archive_exists_in_sda()           # FAIL if missing
    retrieve_hash_from_hsi()                 # FAIL if cannot get hash
    retrieve_size_from_sda()
    
    # Save bundle with HSI hash
    create_bundle_attrs(hash_from_hsi, size_from_sda)
    
elif (is_legacy AND migration_incomplete AND use_sda):
    # Existing legacy migration logic
    wait_for_sda_upload()  # (placeholder)
else:
    # Standard archival flow
    create_bundle()
    upload_to_sda()
```

**NEW: Production Safety Safeguards**

When `legacy_migration.completed = False` (both CMG and Bioloop active):

1. **CMG API Verification (MANDATORY)**
   - Verifies dataset can be found in CMG API
   - Fails if CMG unreachable or dataset not found

2. **SDA Archive Verification (MANDATORY)**
   - Verifies archive file exists in SDA using `sda.exists()`
   - Fails if file missing (even if CMG reports archived)

3. **HSI Hash Retrieval (MANDATORY)**
   - Retrieves MD5 hash from HSI using `sda.get_hash()`
   - Fails if hash unavailable
   - Hash is used for bundle table's `md5` attribute

4. **Bundle Metadata Persistence**
   - Bundle saved with hash from HSI (not locally computed)
   - Ensures database reflects actual SDA/HSI state

---

## 📚 Documentation

### Created Files:

1. **`workers/CONCURRENT_ARCHIVAL_SOLUTION.md`**
   - Comprehensive problem description
   - Implementation details
   - Timeline diagram
   - Configuration guide
   - Testing checklist
   - Edge cases

2. **`workers/PRODUCTION_SAFETY_SAFEGUARDS.md`** ⚠️ CRITICAL
   - Production safety requirements
   - Strict validation rules
   - When safeguards apply
   - Error scenarios and handling
   - Monitoring guidelines

3. **`workers/IMPLEMENTATION_SUMMARY.md`** (this file)
   - Quick reference
   - Files changed
   - How to test
   - Production deployment steps

4. **`workers/test_cmg_api_integration.py`**
   - Test script for CMG API functions
   - Usage examples
   - Demonstrates all API calls

---

## 🧪 How to Test

### 1. Test CMG API Functions

```bash
cd /Users/ripandey/dev/cmg-bioloop/workers

# Test RAW_DATA query
python test_cmg_api_integration.py \
  --origin-path /opt/sca/cmg/data/source/250117_M70445_3027_000000000-KLMNO5769 \
  --type RAW_DATA

# Test DATA_PRODUCT query
python test_cmg_api_integration.py \
  --origin-path /opt/sca/cmg/data/uploads/user_upload_123 \
  --type DATA_PRODUCT
```

### 2. Test Workflow Integration

**Setup:**
1. Create a test dataset in a location visible to both CMG and Bioloop
2. Let both systems register it concurrently

**Expected Behavior:**
```
Bioloop Workflow Steps:
1. await_stability completes
   → Checks CMG, finds dataset
   → Persists cmg_id to database
   
2. archive step starts
   → Detects cmg_id exists
   → Enters wait_for_cmg_archival()
   → Polls CMG every 5 minutes
   
3. CMG completes archival
   → Sets dataset.archived = true
   
4. Bioloop detects completion
   → Uses CMG's archive path
   → Proceeds to next step
   
Result: No concurrent SDA uploads! ✅
```

### 3. Verify Database

```sql
-- Check if cmg_id was persisted
SELECT id, name, cmg_id, archive_path 
FROM dataset 
WHERE name = 'your_test_dataset';

-- Expected result:
-- cmg_id should be populated with MongoDB ObjectId
-- archive_path should match CMG's path

-- Check bundle metadata (hash from HSI)
SELECT d.name, b.md5, b.size, b.name as bundle_name
FROM dataset d
JOIN bundle b ON b.dataset_id = d.id
WHERE d.name = 'your_test_dataset';

-- Expected result:
-- md5 should match hash from HSI (not locally computed)
-- size should match SDA file size
```

### 4. Test Production Safeguards

**⚠️ CRITICAL: Test these scenarios to ensure safeguards work:**

#### Test A: CMG API Unreachable
```bash
# Simulate CMG API down
# Stop CMG API or block network access

# Trigger archival for dataset with cmg_id
# Expected: Workflow FAILS with "cannot be found in CMG API"
```

#### Test B: Archive Missing in SDA
```bash
# Set dataset.archived = true in CMG manually
# But don't upload file to SDA

# Trigger archival
# Expected: Workflow FAILS with "archive does not exist in SDA"
```

#### Test C: Hash Not Available
```bash
# Upload to SDA without checksum
hsi -P "put dataset.tar : archive/raw/dataset.tar"

# Trigger archival
# Expected: Workflow FAILS with "cannot retrieve hash from SDA"
```

#### Test D: All Checks Pass (Normal Flow)
```bash
# Let CMG complete full archival
# Verify Bioloop successfully validates and saves bundle metadata

# Expected: Workflow succeeds
# Expected: Bundle table has hash from HSI
```

---

## 🚀 Production Deployment

### Prerequisites

1. **CMG API must be accessible**
   - Verify connection: `curl http://cmg-api:8080/api/legacy-migration/datasets?origin_path=/test`
   - Should return: `{"success": true, "found": false, ...}`

2. **Configuration in `workers/workers/config/common.py`:**
   ```python
   'cmg_api': {
       'base_url': 'https://cmg.iu.edu/',  # CMG API base URL
       'auth_token': 'Bearer <token>',      # Authentication token
       'conn_timeout': 30,                  # Connection timeout
       'read_timeout': 300,                 # Read timeout
   }
   ```

### Deployment Steps

1. **Code Deployment:**
   ```bash
   # On worker host
   cd /opt/sca/cmg-bioloop
   git pull origin main
   
   # Verify Python syntax
   python -m py_compile workers/workers/cmg_api.py
   python -m py_compile workers/workers/tasks/await_stability.py
   python -m py_compile workers/workers/tasks/archive.py
   ```

2. **Test CMG API Connectivity:**
   ```bash
   # Test from worker host
   poetry shell
   python test_cmg_api_integration.py \
     --origin-path /opt/sca/cmg/data/source/test_dataset \
     --type RAW_DATA
   ```

3. **Restart Workers:**
   ```bash
   # ONLY if code changes require it
   # Coordinate with operations team
   pm2 restart worker
   ```

4. **Monitor First Run:**
   ```bash
   # Watch logs for CMG integration
   pm2 logs worker | grep -i "cmg"
   
   # Expected log lines:
   # "checking if dataset exists in CMG"
   # "found in CMG with ID: ..."
   # "persisting to database"
   # "waiting for CMG to complete archival"
   # "CMG archival completed"
   ```

---

## 🔍 Monitoring & Debugging

### Key Log Messages

**Success case:**
```
[await_stability] checking if dataset exists in CMG by origin_path
[await_stability] found in CMG with ID: 69848419e62d88fe0f0b3766
[await_stability] persisting to database
[archive] detected CMG ID: 69848419e62d88fe0f0b3766
[archive] waiting for CMG to complete archival
[archive] CMG archival not yet complete (poll #1)
[archive] CMG archival not yet complete (poll #2)
[archive] CMG archival completed (elapsed: 614s, polls: 3)
[archive] using CMG archive path: archive/raw/dataset.tar
```

**Fallback case (CMG not found):**
```
[await_stability] checking if dataset exists in CMG by origin_path
[await_stability] not found in CMG, will proceed with standard archival
[archive] standard archival flow (no cmg_id)
[archive] creating tar bundle
[archive] uploading bundle to SDA
```

### Database Queries

```sql
-- Find datasets with CMG coordination
SELECT 
  id, 
  name, 
  cmg_id, 
  archive_path,
  created_at
FROM dataset 
WHERE cmg_id IS NOT NULL
ORDER BY created_at DESC
LIMIT 10;

-- Check workflow status
SELECT 
  d.name,
  d.cmg_id,
  w.name AS workflow_name,
  w.status,
  w.updated_at
FROM dataset d
JOIN workflow w ON w.dataset_id = d.id
WHERE d.cmg_id IS NOT NULL
ORDER BY w.updated_at DESC;
```

### Troubleshooting

**Issue: Timeout waiting for CMG archival**
```
Cause: CMG archival stuck or failed
Action:
1. Check CMG logs: Is archival worker running?
2. Check CMG database: What is dataset.taken status?
3. Check SDA: Is upload in progress?
```

**Issue: CMG API connection failed**
```
Cause: Network issues or CMG API down
Action:
1. Verify: curl http://cmg-api:8080/health
2. Check config: Is base_url correct?
3. Check auth: Is auth_token valid?
Note: Workflow will continue with standard archival
```

**Issue: cmg_id not persisted**
```
Cause: Dataset not in CMG yet
Action:
1. Check timing: Did CMG register it?
2. Check path: Does origin_path match exactly?
3. Check logs: Any CMG API errors?
Note: This is normal if only Bioloop registered it
```

---

## 🎯 Success Metrics

### What to Monitor

1. **Concurrent registrations avoided:**
   - Count: `SELECT COUNT(*) FROM dataset WHERE cmg_id IS NOT NULL`
   - These datasets were coordinated with CMG

2. **Average wait time:**
   - Parse logs: Time between "waiting for CMG" and "CMG archival completed"
   - Should be < 1 hour for typical datasets

3. **Timeout failures:**
   - Search logs: `"timeout waiting for CMG archival"`
   - Should be 0 (or investigate CMG issues)

4. **API errors:**
   - Search logs: `"error checking CMG archival status"`
   - Occasional errors are OK (retried)
   - Frequent errors indicate connectivity issues

---

## 📊 Performance Impact

- **Additional API calls:** 1 per dataset (in await_stability)
- **Additional polling:** Every 5 minutes during wait (in archive)
- **Database updates:** 1 additional field (cmg_id)
- **Memory impact:** Negligible
- **CPU impact:** Negligible (mostly waiting)

---

## 🔮 Future Enhancements

1. **Configurable polling:** Make interval/timeout configurable in config file
2. **Metrics dashboard:** Track wait times, success rates, timeouts
3. **Exponential backoff:** Reduce API calls for long waits
4. **Bundle info sync:** Fetch bundle metadata from CMG after archival
5. **Alert on long waits:** Notify if timeout approaches (e.g., >12 hours)
6. **Health check endpoint:** Add endpoint to verify CMG API connectivity

---

## ✅ Validation Checklist

Before deploying to production:

- [ ] All Python files compile without errors
- [ ] CMG API is accessible from worker host
- [ ] Configuration values are set correctly
- [ ] Test script runs successfully
- [ ] Logs show expected messages
- [ ] Database schema includes cmg_id field
- [ ] Operations team is aware of changes
- [ ] Monitoring is in place
- [ ] Rollback plan is documented

---

## 📞 Support

**Questions or Issues:**
- Check logs: `pm2 logs worker`
- Check documentation: `CONCURRENT_ARCHIVAL_SOLUTION.md`
- Test API: `python test_cmg_api_integration.py`
- Database: Check cmg_id field and workflow status

**Contact:**
- Implementation: See git blame for recent changes
- CMG API: Contact CMG team for API issues
- Operations: Coordinate for production changes

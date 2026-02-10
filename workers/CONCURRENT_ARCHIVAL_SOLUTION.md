# Concurrent Archival Solution

## Problem
When the same dataset is picked up by both CMG and Bioloop for registration, concurrent archival attempts to SDA could result in:
- One upload overwriting another mid-stream
- Data corruption
- Race conditions

## Solution Overview
Use CMG's database state as the source of truth for archival completion. Bioloop's archive workflow waits for CMG to complete archival before proceeding.

## Implementation

### 1. CMG API Functions (`workers/cmg_api.py`)

Added three new functions:

#### `get_dataset_by_origin_path(origin_path: str) -> dict | None`
- Queries CMG API: `/api/legacy-migration/datasets?origin_path=<origin_path>`
- Parses response: `{"success": true, "found": true, "dataset": {...}}`
- Returns `dataset` object from CMG's MongoDB `dataset` collection
- Returns `None` if not found or request unsuccessful

**Example response:**
```json
{
  "success": true,
  "found": true,
  "normalized_path": "/opt/sca/cmg/data/source/dataset_name",
  "dataset": {
    "_id": "69848419e62d88fe0f0b3766",
    "name": "dataset_name",
    "archived": false,
    "taken": null,
    ...
  }
}
```

#### `get_dataproduct_by_origin_path(origin_path: str) -> dict | None`
- Queries CMG API: `/api/legacy-migration/dataproducts?origin_path=<origin_path>`
- Parses response: `{"success": true, "found": true, "dataproduct": {...}}`
- Returns `dataproduct` object from CMG's MongoDB `dataproduct` collection
- Returns `None` if not found or request unsuccessful

**Example response:**
```json
{
  "success": true,
  "found": true,
  "normalized_path": "/opt/sca/cmg/data/uploads/upload_123",
  "dataproduct": {
    "_id": "507f191e810c19729de860eb",
    "name": "upload_123",
    "paths": {
      "archive": "archive/products/upload_123.tar",
      "staged": ""
    },
    ...
  }
}
```

#### `is_dataset_archived_in_cmg(origin_path: str, dataset_type: str) -> bool`
- Checks archival completion status in CMG database
- Uses `origin_path` to query CMG (leverages the functions above)
- For `RAW_DATA`: checks `dataset.archived === true`
- For `DATA_PRODUCT`: checks `dataproduct.paths.archive` exists and is not empty
- Returns `True` if archival is complete, `False` otherwise

### 2. Await Stability Step (`workers/tasks/await_stability.py`)

**Added at the end of the await_stability task:**

1. Check if dataset exists in CMG by querying CMG API with `origin_path`
2. If found, extract the `cmg_id` (MongoDB `_id`)
3. Persist `cmg_id` to Bioloop's PostgreSQL database using `api.update_dataset()`
4. If CMG check fails, log warning and continue (don't fail workflow)

**Why at the end?**
- Dataset must be stable before we check CMG
- Gives CMG time to register the dataset if it's processing concurrently
- Non-blocking: failures don't stop the workflow

### 3. Archive Step (`workers/tasks/archive.py`)

**Added new function:** `wait_for_cmg_archival()`
- Polls CMG database state using `cmg_api.is_dataset_archived_in_cmg(origin_path, dataset_type)`
- Queries CMG by origin_path to check archived status
- Default poll interval: 300 seconds (5 minutes)
- Default timeout: 86400 seconds (24 hours)
- Updates Celery progress tracking with time remaining
- Raises `TimeoutError` if CMG doesn't complete within timeout
- Logs detailed status on each poll

**Modified `archive()` function:**

Flow decision tree:
```
if (cmg_id exists AND use_sda):
    wait_for_cmg_archival()
    use CMG's archive path
    skip bundle creation
elif (is_legacy AND legacy_migration_incomplete AND use_sda):
    [existing placeholder logic]
else:
    [standard archival flow]
```

## How It Works

### Scenario: Concurrent Registration

**Timeline:**

```
T0: Dataset appears in filesystem
    ├─ CMG watch service detects it
    └─ Bioloop watch service detects it

T1: Both create database records
    ├─ CMG: creates in MongoDB
    └─ Bioloop: creates in PostgreSQL

T2: Both start integrated workflows
    ├─ CMG: starts workflow
    └─ Bioloop: starts workflow

T3: Both workflows reach await_stability
    ├─ CMG: checks stability
    └─ Bioloop: checks stability

T4: Bioloop's await_stability completes
    └─ Queries CMG API by origin_path
    └─ Finds CMG dataset, extracts cmg_id
    └─ Persists cmg_id to Bioloop database

T5: Bioloop's archive step starts
    └─ Detects cmg_id exists
    └─ Enters wait_for_cmg_archival()

T6: Bioloop polls CMG database (every 5 minutes)
    └─ Queries: GET /api/legacy-migration/datasets?origin_path=...
    └─ Checks if dataset.archived === true
    └─ Response: {"success": true, "found": true, "dataset": {"archived": false, "taken": {...}}}
    └─ Still false, continues waiting

T7: CMG's archive step completes
    └─ Uploads to SDA
    └─ Sets dataset.archived = true in MongoDB

T8: Bioloop's next poll detects archival complete
    └─ Queries: GET /api/legacy-migration/datasets?origin_path=...
    └─ Response: {"success": true, "found": true, "dataset": {"archived": true}}
    └─ Returns from wait_for_cmg_archival()

T9: Bioloop verifies CMG's archive (STRICT VALIDATION)
    └─ Checks: sda.exists(archive/raw/dataset.tar)
    └─ Gets hash: sda.get_hash() → "a3b5c7d9e1f3..."
    └─ Gets size: sda.get_size() → 12345678 bytes
    └─ Creates bundle_attrs with HSI hash

T10: Bioloop saves bundle metadata
    └─ Updates database: archive_path, bundle.md5, bundle.size
    └─ Bundle table now has hash from HSI
    └─ Proceeds to next workflow step

Result: Only CMG uploads to SDA, no conflict
```

## Configuration

### CMG API Configuration (`workers/workers/config/common.py`)

Requires these config values:
```python
'cmg_api': {
    'base_url': 'https://cmg.iu.edu/',  # CMG API base URL
    'auth_token': 'Bearer token',        # Authentication token
    'conn_timeout': 30,                  # Connection timeout
    'read_timeout': 300,                 # Read timeout
}
```

### Polling Configuration (defaults in code)

Can be overridden in config or as function parameters:
- `poll_interval_seconds`: 300 (5 minutes)
- `timeout_seconds`: 86400 (24 hours)

## Edge Cases Handled

1. **CMG API is down (await_stability)**: Logged as warning, workflow continues with standard archival
2. **Dataset not in CMG (await_stability)**: No cmg_id persisted, standard archival proceeds
3. **CMG archival timeout (archive)**: Raises TimeoutError, workflow fails (expected behavior)
4. **CMG ID exists but dataset type unknown**: ValueError raised with clear message
5. **Legacy datasets during migration**: Existing placeholder logic preserved

## Critical Safeguards (Production Safety)

### When `legacy_migration.completed = False` (Both CMG and Bioloop Active)

**STRICT VALIDATION in archive step when `cmg_id` exists:**

1. **CMG API Verification (MANDATORY)**
   - Must be able to query CMG API for the dataset
   - If query fails → **WORKFLOW FAILS** (Exception raised)
   - Prevents proceeding when data consistency is compromised

2. **SDA Archive Verification (MANDATORY)**
   - Must verify archive file exists in SDA using `sda.exists()`
   - If file not found → **WORKFLOW FAILS** (Exception raised)
   - Prevents data loss when CMG reports completion but file is missing

3. **HSI Hash Retrieval (MANDATORY)**
   - Must retrieve MD5 hash from HSI using `sda.get_hash()`
   - If hash cannot be retrieved → **WORKFLOW FAILS** (Exception raised)
   - Ensures data integrity verification is possible

4. **Bundle Metadata Persistence**
   - Hash retrieved from HSI is used for bundle table's `md5` attribute
   - Size retrieved from SDA via `sda.get_size()`
   - Bundle metadata saved to database for future verification

### Error Messages

```
FATAL: dataset has cmg_id but cannot be found in CMG API
→ Indicates data consistency issue between systems
→ Manual intervention required

FATAL: archive path does not exist in SDA
→ CMG reported archival complete but file not found
→ Potential CMG archival failure

FATAL: cannot retrieve hash from SDA
→ File exists but hash unavailable
→ HSI issue or incomplete upload
→ Cannot verify data integrity
```

### Why These Safeguards?

**Problem:** When both CMG and Bioloop are active, we rely on CMG to archive. If we can't verify CMG's work, we risk:
- Data inconsistency (different archives in different systems)
- Data loss (no archive exists but we think it does)
- Integrity issues (can't verify archive checksum)

**Solution:** Fail fast and loud. Better to halt the workflow than proceed with unverifiable archival.

## Testing Checklist

### Unit Tests Needed
- [ ] `get_dataset_by_origin_path()` with found dataset
- [ ] `get_dataset_by_origin_path()` with not found (returns None)
- [ ] `get_dataset_by_origin_path()` with API error handling
- [ ] `get_dataproduct_by_origin_path()` with found dataproduct
- [ ] `get_dataproduct_by_origin_path()` with not found (returns None)
- [ ] `is_dataset_archived_in_cmg()` for RAW_DATA with archived=true
- [ ] `is_dataset_archived_in_cmg()` for RAW_DATA with archived=false
- [ ] `is_dataset_archived_in_cmg()` for DATA_PRODUCT with archive path set
- [ ] `is_dataset_archived_in_cmg()` for DATA_PRODUCT with empty archive path
- [ ] `is_dataset_archived_in_cmg()` when dataset not found in CMG
- [ ] `wait_for_cmg_archival()` timeout behavior
- [ ] `wait_for_cmg_archival()` successful completion
- [ ] `wait_for_cmg_archival()` with CMG API errors (should log warning and retry)
- [ ] `archive()` with cmg_id present (waits for CMG)
- [ ] `archive()` without cmg_id (standard archival)
- [ ] `await_stability()` finds dataset in CMG and persists cmg_id
- [ ] `await_stability()` dataset not in CMG (continues normally)

### Integration Tests Needed
- [ ] End-to-end: Dataset registered in both systems
- [ ] CMG completes archival first
- [ ] Bioloop waits and detects completion
- [ ] Archive path matches CMG's path
- [ ] Workflow completes successfully

### Manual Testing
- [ ] Deploy to staging environment
- [ ] Create test dataset visible to both systems
- [ ] Verify cmg_id persisted in await_stability step
- [ ] Verify archive step waits for CMG
- [ ] Verify workflow completes without errors
- [ ] Verify no duplicate uploads to SDA

## Future Improvements

1. **Make polling configurable**: Add to `config/common.py`
2. **Add metrics**: Track how long workflows wait for CMG
3. **Optimize polling**: Use exponential backoff
4. **Add alerts**: Notify if timeout approaches
5. **Bundle info sync**: Fetch bundle metadata from CMG after archival

## Notes

- This solution **does not** prevent CMG from uploading to SDA
- It only prevents **Bioloop** from uploading when CMG is handling it
- Both systems can still create database records (safe, handled by unique constraints)
- Only one system will upload to SDA (CMG wins, Bioloop waits)
- If only Bioloop registers dataset (no cmg_id), standard archival proceeds
- **API Response Format:** All CMG API responses follow the pattern `{"success": bool, "found": bool, "dataset|dataproduct": {...}}`
- **Trailing Slashes:** CMG API automatically normalizes paths (removes trailing slashes)
- **Polling Strategy:** Uses origin_path for all lookups (consistent, reliable endpoint)

## Related Files

- `workers/workers/cmg_api.py` - CMG API client functions
- `workers/workers/tasks/await_stability.py` - Persistence of cmg_id
- `workers/workers/tasks/archive.py` - Wait logic and archival coordination
- `api/prisma/schema.prisma` - Database schema (cmg_id field already exists)

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
- Queries CMG API: `/api/legacy-migration/datasets/?origin_path=<origin_path>`
- Returns dataset from CMG's MongoDB `dataset` collection
- Returns `None` if not found

#### `get_dataproduct_by_origin_path(origin_path: str) -> dict | None`
- Queries CMG API: `/api/legacy-migration/dataproducts/?origin_path=<origin_path>`
- Returns dataproduct from CMG's MongoDB `dataproduct` collection
- Returns `None` if not found

#### `is_dataset_archived_in_cmg(cmg_id: str, dataset_type: str) -> bool`
- Checks archival completion status in CMG database
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
- Polls CMG database state using `cmg_api.is_dataset_archived_in_cmg()`
- Default poll interval: 300 seconds (5 minutes)
- Default timeout: 86400 seconds (24 hours)
- Updates Celery progress tracking
- Raises `TimeoutError` if CMG doesn't complete within timeout

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
    └─ Checks if dataset.archived === true
    └─ Still false, continues waiting

T7: CMG's archive step completes
    └─ Uploads to SDA
    └─ Sets dataset.archived = true

T8: Bioloop's next poll detects archival complete
    └─ Returns from wait_for_cmg_archival()
    └─ Uses CMG's archive path
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

1. **CMG API is down**: Logged as warning, workflow continues with standard archival
2. **Dataset not in CMG**: No cmg_id persisted, standard archival proceeds
3. **CMG archival timeout**: Raises TimeoutError, workflow fails (expected behavior)
4. **CMG ID exists but dataset type unknown**: ValueError raised with clear message
5. **Legacy datasets during migration**: Existing placeholder logic preserved

## Testing Checklist

### Unit Tests Needed
- [ ] `get_dataset_by_origin_path()` with valid/invalid origin paths
- [ ] `get_dataproduct_by_origin_path()` with valid/invalid origin paths
- [ ] `is_dataset_archived_in_cmg()` for RAW_DATA (archived=true/false)
- [ ] `is_dataset_archived_in_cmg()` for DATA_PRODUCT (with/without archive path)
- [ ] `wait_for_cmg_archival()` timeout behavior
- [ ] `wait_for_cmg_archival()` successful completion
- [ ] `archive()` with cmg_id present
- [ ] `archive()` without cmg_id

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

## Related Files

- `workers/workers/cmg_api.py` - CMG API client functions
- `workers/workers/tasks/await_stability.py` - Persistence of cmg_id
- `workers/workers/tasks/archive.py` - Wait logic and archival coordination
- `api/prisma/schema.prisma` - Database schema (cmg_id field already exists)

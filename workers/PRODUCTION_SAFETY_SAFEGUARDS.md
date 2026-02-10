# Production Safety Safeguards for Concurrent Archival

## Critical Context

When `legacy_migration.completed = False` in production config, **both CMG and Bioloop are actively registering datasets**. This creates a scenario where:

1. Both systems may detect the same dataset
2. Both systems may create database records
3. Only one system should archive to SDA (to prevent conflicts)
4. If Bioloop defers to CMG, it **MUST** verify CMG's work succeeded

## The Problem Without Safeguards

**Scenario:** Bioloop detects a dataset with `cmg_id`, meaning CMG also registered it.

**Without safeguards:**
```
✓ Bioloop: "CMG is handling this, I'll skip archival"
✓ Database: archive_path = "archive/raw/dataset.tar"
✗ Reality: CMG archival failed, no file in SDA
→ Result: Database says archived, but data is lost!
```

## The Solution: Strict Validation

### Validation Points in Archive Step

When `cmg_id` exists AND `legacy_migration` is enabled, the archive step performs **three mandatory checks**:

#### 1. CMG API Verification
```python
# Must be able to query CMG API
cmg_entity = cmg_api.get_dataset_by_origin_path(origin_path)

if not cmg_entity:
    raise Exception("FATAL: dataset has cmg_id but not found in CMG API")
```

**Why:** Verifies data consistency between systems. If we can't find the dataset in CMG, something is wrong.

**Fails when:**
- CMG API is down
- Dataset was deleted from CMG
- Origin path mismatch between systems
- CMG database corruption

#### 2. SDA Archive Existence
```python
# Must verify file exists in SDA
if not sda.exists(sda_bundle_path):
    raise Exception("FATAL: archive does not exist in SDA")
```

**Why:** CMG may report "archived=true" but the file might not exist due to:
- CMG archival process crashed
- Network failure during upload
- SDA storage issue
- Race condition in CMG's status update

**Fails when:**
- File not found in SDA
- Incorrect archive path
- SDA permissions issue

#### 3. HSI Hash Retrieval
```python
# Must retrieve MD5 hash from HSI
bundle_checksum = sda.get_hash(sda_bundle_path, missing_ok=False)

if not bundle_checksum:
    raise Exception("FATAL: cannot retrieve hash from SDA")
```

**Why:** Without the hash, we cannot:
- Verify data integrity later
- Validate the archive is complete
- Detect corruption or tampering

**Fails when:**
- Hash not computed by HSI
- HSI command failure
- Incomplete upload
- Storage system issue

### Bundle Metadata Persistence

Once all validations pass:

```python
# Retrieve metadata from SDA
bundle_size = sda.get_size(sda_bundle_path)
bundle_checksum = sda.get_hash(sda_bundle_path)

# Create bundle attributes with HSI hash
bundle_attrs = {
    'name': bundle_name,
    'size': bundle_size,
    'md5': bundle_checksum,  # ← Hash from HSI, not computed locally
}

# Will be saved to database by archive_dataset()
```

**Result:** Bundle table contains:
- Exact size from SDA
- Actual MD5 hash from HSI
- Verified archive path

## Implementation Details

### Code Location

File: `workers/workers/tasks/archive.py`  
Function: `archive()`  
Lines: ~142-235

### Configuration Check

```python
# safeguards only apply when both systems are active
legacy_migration_incomplete = not config.get('legacy_migration', {}).get('completed', True)

if cmg_id and use_sda and legacy_migration_incomplete:
    # STRICT VALIDATION MODE
```

### Error Handling

All validation failures raise exceptions with clear error messages:

```python
# Examples:
"FATAL: dataset has cmg_id but cannot be found in CMG API"
"FATAL: archive path does not exist in SDA: archive/raw/dataset.tar"
"FATAL: cannot retrieve hash from SDA"
```

These exceptions will:
- Fail the workflow
- Show in worker logs
- Trigger workflow retry mechanism
- Alert operators via monitoring

## Workflow Impact

### Normal Case (Validation Passes)

```
await_stability → archive (validate CMG) → archive (verify SDA) → next step
                  ✓ CMG API reachable     ✓ File exists
                  ✓ Dataset found         ✓ Hash retrieved
                                          ✓ Bundle saved
```

**Duration:** +10-30 seconds (API calls + HSI commands)

### Failure Case (Validation Fails)

```
await_stability → archive (validate CMG) → FAIL
                  ✗ CMG API unreachable
                  or dataset not found

await_stability → archive (verify SDA) → FAIL
                  ✓ CMG API OK
                  ✗ Archive missing in SDA

await_stability → archive (verify SDA) → FAIL
                  ✓ CMG API OK
                  ✓ Archive exists
                  ✗ Cannot get hash from HSI
```

**Result:** Workflow marked as FAILED, operator investigates

## When Safeguards Apply

| Condition | Safeguards Active? | Behavior |
|-----------|-------------------|----------|
| `cmg_id` exists + `legacy_migration` enabled + production | ✅ YES | STRICT validation |
| `cmg_id` exists + `legacy_migration` disabled | ❌ NO | Standard wait, no strict checks |
| `cmg_id` missing | ❌ NO | Standard archival (Bioloop creates bundle) |
| Development/Docker environment | ❌ NO | Standard archival |

## Testing the Safeguards

### Test 1: CMG API Down

```bash
# Simulate CMG API failure
# Stop CMG API temporarily
systemctl stop cmg-api

# Trigger Bioloop archival for dataset with cmg_id
# Expected: Workflow fails with "cannot be found in CMG API"
```

### Test 2: Archive Missing in SDA

```bash
# Manually set dataset.archived = true in CMG
# But don't upload file to SDA

# Trigger Bioloop archival
# Expected: Workflow fails with "archive does not exist in SDA"
```

### Test 3: Hash Not Available

```bash
# Upload file to SDA without checksum
hsi -P "put dataset.tar : archive/raw/dataset.tar"

# Trigger Bioloop archival
# Expected: Workflow fails with "cannot retrieve hash from SDA"
```

### Test 4: All Validations Pass

```bash
# Normal CMG archival flow
# Expected: Workflow succeeds, bundle saved with HSI hash
```

## Monitoring

### Key Log Messages

**Success:**
```
[archive] detected CMG ID: 507f1f77bcf86cd799439011
[archive] legacy_migration is enabled, strict validation required
[archive] verified dataset exists in CMG
[archive] CMG archival completed, verifying archive in SDA
[archive] archive exists in SDA, retrieving hash
[archive] retrieved hash from SDA: a3b5c7d9e1f3...
[archive] retrieved size from SDA: 12345678 bytes
[archive] successfully verified CMG archive with bundle metadata from SDA
[archive] bundle will be saved: size=12345678, md5=a3b5c7d9e1f3...
```

**Failure (CMG API):**
```
[archive] detected CMG ID: 507f1f77bcf86cd799439011
[archive] legacy_migration is enabled, strict validation required
[archive] FATAL: dataset has cmg_id but cannot be found in CMG API
ERROR: failed to verify dataset in CMG API: Connection refused
```

**Failure (SDA):**
```
[archive] CMG archival completed, verifying archive in SDA
[archive] FATAL: archive path does not exist in SDA: archive/raw/dataset.tar
ERROR: CMG reported archival complete but file not found
```

**Failure (Hash):**
```
[archive] archive exists in SDA, retrieving hash
[archive] FATAL: cannot retrieve hash from SDA
ERROR: File exists but hash is not available
```

### Metrics to Track

1. **Validation failures:** Count of workflows failed due to safeguards
2. **CMG API errors:** Frequency of CMG API unreachable
3. **SDA missing archives:** Count of missing files when CMG reports archived
4. **Hash retrieval failures:** Count of HSI hash errors
5. **Average validation time:** Time spent on CMG/SDA verification

### Alerts

**Critical alerts (immediate action required):**
- High rate of CMG API failures → CMG service down
- Missing archives in SDA → CMG archival broken
- Hash retrieval failures → HSI/SDA issue

**Warning alerts:**
- Occasional validation failures → Investigate specific datasets
- Slow validation times → Performance issue

## Production Deployment

### Before Deployment

- [ ] Verify CMG API is accessible
- [ ] Verify HSI commands work: `hsi -P "ls archive/raw/"`
- [ ] Verify SDA access permissions
- [ ] Test with one dataset (dry run)
- [ ] Review error handling in monitoring system

### After Deployment

- [ ] Monitor first 10 workflows with cmg_id
- [ ] Verify bundle table populated with HSI hashes
- [ ] Check validation times (should be < 30s)
- [ ] Confirm no false positives (legitimate failures)

### Rollback Plan

If safeguards cause issues:

1. **Disable strict validation:**
   ```python
   # In config/common.py
   'legacy_migration': {
       'completed': True,  # Disables strict validation
   }
   ```

2. **Or revert code:**
   ```bash
   git revert <commit-hash>
   pm2 restart worker
   ```

## FAQ

**Q: Why fail the workflow instead of continuing?**  
A: Better to fail fast than proceed with potentially lost data. Operators can investigate and retry.

**Q: What if CMG API is temporarily down?**  
A: Workflow will fail, but Celery retry mechanism will retry later. Add retry policy if needed.

**Q: Can we skip validation for specific datasets?**  
A: Yes, set `legacy_migration.completed = True` temporarily, but understand the risks.

**Q: What's the performance impact?**  
A: ~10-30 seconds per dataset for API calls + HSI commands. Acceptable for safety gain.

**Q: Will this affect existing archived datasets?**  
A: No, safeguards only apply to new archival operations during concurrent registration period.

---

**Last Updated:** 2026-02-09  
**Status:** Production-ready  
**Severity:** CRITICAL - Data Loss Prevention

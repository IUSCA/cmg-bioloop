# Bundle Population Script

Standalone script to populate the `bundle` table with MD5 checksums from HPSS for legacy archived datasets.

## Overview

This script queries HPSS (via HSI) to retrieve MD5 checksums for archived datasets and creates corresponding `bundle` records in the database. Bundles are required for staging legacy archived datasets.

**Important:** This script is NOT part of the bigbang/poller process. It's a separate utility that should be run directly on the remote host where HSI is available.

## Requirements

### Safety Guard

**IMPORTANT:** This script is a **standalone utility** that is NOT part of the automated migration process.

**Execution Model:**
- ✅ Can be run directly: `node populate_bundles.js`
- ❌ NOT called by bigbang or poller scripts
- ❌ NOT part of automated workflows
- ⚠️ Requires manual execution by operator with HSI access

### Technical Requirements

1. **Host Requirements:**
   - HSI binary must be installed and available in PATH
   - HSI must be authenticated (Kerberos ticket, HPSS credentials, etc.)
   - Network access to HPSS

2. **Database Requirements:**
   - Database connection configured (via `.env` or environment variables)
   - Prisma client must be installed (`npm install` already run)

3. **Permissions:**
   - Read access to HPSS archives
   - Write access to Bioloop database

## Usage

### Basic Usage

```bash
# Navigate to data_sync directory
cd /opt/sca/cmg/data_sync

# Dry run (see what would happen, no changes made)
node populate_bundles.js --dry-run

# Populate bundles in sandbox database (testing)
node populate_bundles.js --target-db=sandbox

# Populate bundles in production database
node populate_bundles.js --target-db=app
```

### Options

| Option | Description |
|--------|-------------|
| `--target-db=<target>` | Target database: `sandbox` (default), `app`, or `custom` |
| `--dry-run` | Show what would be done without making changes |
| `--limit=<n>` | Process only N datasets (useful for testing) |
| `--help`, `-h` | Show help message |

### Examples

```bash
# Dry run to see which datasets need bundles
node populate_bundles.js --dry-run

# Test with first 10 datasets
node populate_bundles.js --target-db=app --limit=10 --dry-run

# Populate all bundles in production
node populate_bundles.js --target-db=app

# Populate bundles in sandbox for testing
node populate_bundles.js --target-db=sandbox
```

## How It Works

1. **Query Database:** Finds all datasets with `archive_path` but no `bundle` record
2. **Check HSI:** Verifies HSI is available and authenticated
3. **For Each Dataset:**
   - Executes: `hsi -P 'hashlist /path/to/archive.tar'`
   - Parses MD5 checksum from output (format: `<md5> <size> <path>`)
   - Creates `bundle` record with: `name`, `size`, `md5`, `dataset_id`
4. **Report:** Shows summary of created/skipped/failed bundles

## Output

```
================================================================================
Bundle Population Script
================================================================================

Target Database: app
Dry Run: NO

Database URL: postgresql://<credentials>@localhost:5432/bioloop
[OK] Connected to database
[OK] HSI is available and authenticated

[QUERY] Finding archived datasets without bundles...
[INFO] Found 150 archived datasets without bundles

[1/150] Processing dataset 42...
[FETCH] Dataset 42 (Sample_001): /hpss/archive/Sample_001.tar
[SUCCESS] Created bundle for dataset 42 (Sample_001): d41d8cd98f00b204e9800998ecf8427e

[2/150] Processing dataset 43...
[SKIP] Dataset 43 (Sample_002): bundle already exists

...

================================================================================
Summary
================================================================================
Total datasets processed: 150
Created: 148
Skipped: 1
Failed: 1

[DONE] Bundle population complete
```

## Error Handling

### Common Errors

**1. HSI Not Available**
```
[ERROR] HSI is not available or not authenticated
        Make sure you are on a host with HSI installed and authenticated
        Run: kinit <username> (if using Kerberos)
```

**Solution:**
- Ensure you're on the correct host (e.g., `cmg-new-service1.sca.iu.edu`)
- Authenticate HSI: `kinit <your_username>`
- Test HSI: `hsi pwd`

**2. Database Connection Failed**
```
[ERROR] Can't reach database server
```

**Solution:**
- Check `.env` file has correct `DATABASE_URL`
- For `--target-db=app`, ensure `../api/.env` exists
- Test connectivity: `psql $DATABASE_URL`

**3. No Checksum Retrieved**
```
[FAIL] Dataset 123 (Sample_XYZ): no checksum retrieved
```

**Possible Causes:**
- Archive file doesn't exist in HPSS
- Archive path is incorrect
- HPSS permissions issue
- HSI output format unexpected

**Solution:**
- Manually check: `hsi ls /path/to/archive.tar`
- Manually get checksum: `hsi hashlist /path/to/archive.tar`
- These datasets can be skipped (won't be stageable)

## Idempotency

The script is **idempotent** - safe to run multiple times:
- Skips datasets that already have bundles
- Can be interrupted and restarted
- Use `--limit` to process in batches

## When to Run

**Scenarios:**

1. **After Initial CMG Migration:** Run once to populate bundles for all legacy archived datasets
2. **After Bigbang Without Bundles:** If bigbang was run with bundle population skipped
3. **Periodic Cleanup:** Occasionally check for missing bundles (rare)
4. **After Archive Recovery:** If datasets were re-archived outside Bioloop

**Not Needed For:**
- New datasets archived through Bioloop (bundles created automatically during archival workflow)
- Datasets that aren't archived (`archive_path IS NULL`)
- Datasets that are only staged (not archived)

## Production Workflow

```bash
# 1. SSH to production host with HSI access
ssh cmg-new-service1.sca.iu.edu

# 2. Navigate to data_sync directory
cd /opt/sca/cmg/data_sync

# 3. Authenticate HSI (if needed)
kinit <your_username>

# 4. Verify HSI works
hsi pwd

# 5. Dry run to see what would happen
node populate_bundles.js --target-db=app --dry-run

# 6. Populate bundles
node populate_bundles.js --target-db=app

# 7. Verify results
# Check total bundles created
```

## Verification

After running, verify bundles were created:

```sql
-- Count total bundles
SELECT COUNT(*) FROM bundle;

-- Check specific datasets
SELECT d.id, d.name, d.archive_path, b.md5, b.size
FROM dataset d
LEFT JOIN bundle b ON b.dataset_id = d.id
WHERE d.archive_path IS NOT NULL
ORDER BY d.id;

-- Find datasets still missing bundles
SELECT d.id, d.name, d.archive_path
FROM dataset d
WHERE d.archive_path IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM bundle WHERE dataset_id = d.id);
```

## Performance

- **Speed:** ~2-5 seconds per dataset (HSI query overhead)
- **Expected Runtime:** 
  - 100 datasets: ~5-10 minutes
  - 1000 datasets: ~45-90 minutes
- **Optimization:** Use `--limit` to process in batches if needed

## Troubleshooting

### Script hangs on HSI command

**Symptom:** Script stops at `[FETCH]` step and never completes

**Cause:** HSI command is waiting for authentication or network issue

**Solution:**
- Press Ctrl+C to stop
- Check HSI authentication: `klist`
- Renew ticket: `kinit <username>`
- Test HSI manually: `hsi ls /hpss`

### Wrong database being used

**Symptom:** Script modifies wrong database

**Solution:**
- Always specify `--target-db` explicitly
- Use `--dry-run` first to verify
- Check `DATABASE_URL` in output

### Permission denied errors

**Symptom:** `EACCES` or permission errors

**Solution:**
- Check file permissions: `ls -l populate_bundles.js`
- Make executable if needed: `chmod +x populate_bundles.js`
- Check database user has INSERT permissions on `bundle` table

## Related Documentation

- [Bigbang Sync Usage](BIGBANG_SYNC_USAGE.md) - Main migration documentation
- [Poller Sync Usage](POLLER_SYNC_USAGE.md) - Continuous sync documentation
- [Stage Workflow](../BIOLOOP_WORKFLOW_ARCHITECTURE.md) - How staging uses bundles

## Support

For issues or questions:
1. Check error messages and this troubleshooting guide
2. Verify HSI is working: `hsi pwd`
3. Test with `--dry-run` and `--limit=1`
4. Check logs for detailed error messages


# Conversion Logs Sync - Usage Guide

## Overview

The conversion logs sync process populates **historic CMG conversion logs from the filesystem into Bioloop's database** (`worker_process` and `log` tables).

This process migrates log files from `/N/project/CMG-SCA/production/runlogs/` into the PostgreSQL database.

## Two Ways to Run

### Method 1: As Part of Bigbang Migration (Recommended)

The conversion logs sync is **automatically included** in the bigbang migration (step 12 of 14).

```bash
# From host
cd /opt/sca/cmg/data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node /opt/sca/app/src/bigbang_sync.js --target-db=app

# Conversion logs will be synced automatically unless you skip them:
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node /opt/sca/app/src/bigbang_sync.js --target-db=app --skip-conversion-logs
```

**Use this method for:**
- Initial database migration
- Full data population including conversion logs
- Production deployment

### Method 2: Standalone Script (Optional)

Run the conversion logs sync **independently** of the bigbang migration:

```bash
# From host (script automatically execs into container)
cd /opt/sca/cmg/data_sync
./bin/sync_conversion_logs.sh --target-db=app

# Or from inside the container
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app
node src/standalone_sync_conversion_logs.js --target-db=app
```

**Use this method for:**
- Re-processing conversion logs after bigbang completed
- Testing conversion logs sync separately
- Debugging log population issues
- Overwriting existing logs with updated data

## Usage Examples

### Dry Run (Discovery Mode)

See what logs would be processed without making database changes:

```bash
# From host
./bin/sync_conversion_logs.sh --dry-run

# From inside container
node src/standalone_sync_conversion_logs.js --dry-run
```

**Output:**
- Lists all conversion log files found
- Shows which conversions would be processed
- Displays file sizes and missing files
- No database writes performed

### Sync to Sandbox (Testing)

Test the sync in an isolated sandbox database:

```bash
# From host
./bin/sync_conversion_logs.sh --target-db=sandbox

# From inside container
node src/standalone_sync_conversion_logs.js --target-db=sandbox
```

### Sync to Production Database

Populate conversion logs in the main application database:

```bash
# From host
./bin/sync_conversion_logs.sh --target-db=app

# From inside container
node src/standalone_sync_conversion_logs.js --target-db=app
```

### Overwrite Existing Logs

Re-process conversions that already have logs populated:

```bash
# From host
./bin/sync_conversion_logs.sh --target-db=app --overwrite-existing

# From inside container
node src/standalone_sync_conversion_logs.js --target-db=app --overwrite-existing
```

**⚠️ WARNING:** This will delete existing `worker_process` and `log` entries and recreate them.

## Options

| Option | Description |
|--------|-------------|
| `--target-db=<target>` | Target database: `sandbox` (default), `app`, or `custom` |
| `--dry-run` | Discovery mode - show what would happen without database writes |
| `--overwrite-existing` | Re-process conversions with existing logs (deletes and recreates) |
| `--help` | Show help message |

### Target Database Options

- **`sandbox`** (default): Writes to isolated test database
  - Uses `data_sync/.env` configuration
  - Safe for testing migrations
  
- **`app`**: Writes to the main application's production database
  - Automatically reads configuration from `../api/.env`
  - ⚠️ Use only in production environment
  
- **`custom`**: Uses `DATABASE_URL` from environment
  - For advanced/custom configurations

## Architecture

### Log File Structure

CMG conversion logs are stored as individual files:
```
/N/project/CMG-SCA/production/runlogs/
├── convert_dataset1.log
├── convert_dataset2.log
└── convert_dataset3.log
```

Each log file contains logs for **all conversions** performed on that dataset.

### Database Migration

For each conversion:

1. **Create `worker_process` record:**
   - `workflow_id`: `cmg-historic-conversion-{conversion_id}`
   - `task_id`: `cmg-conversion-{cmg_id}`
   - `step`: `'conversion'`
   - `hostname`: `'cmg-historic'`
   - `tags`: Contains provenance metadata

2. **Parse log file:**
   - Extract timestamps from log lines
   - Infer log level (ERROR, WARNING, INFO, DEBUG) from content
   - Batch process in chunks of 1000 entries

3. **Create `log` records:**
   - Each log line becomes a separate `log` entry
   - Linked to `worker_process` via `worker_process_id`
   - **Logs are duplicated** for each conversion sharing a file

4. **Update conversion:**
   - Set `workflow_id` on conversion record
   - Marks conversion as "logs populated"

### Idempotency

The sync is **fully idempotent**:

- Conversions with existing `workflow_id` are skipped (unless `--overwrite-existing`)
- Safe to re-run without creating duplicates
- Partial progress is retained if sync fails

## Environment Configuration

### Required Environment Variables

```bash
# In data_sync/.env or data_sync/.env.default
CMG_LEGACY_CONVERSIONS_LOGS_DIR=/N/project/CMG-SCA/runlogs
```

**Container path:**
```
/opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs
```

The directory is mounted read-only in `docker-compose.sandbox.yml`:
```yaml
volumes:
  - /N/project:/opt/sca/project/ingestion_source_dir:ro
```

### Path Translation

| Environment | Host Path | Container Path |
|-------------|-----------|----------------|
| Production | `/N/project/CMG-SCA/production/runlogs` | `/opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs` |
| Local/Dev | Not available | Not available (gracefully skipped) |

## Production Considerations

### When to Run

**Included in bigbang:**
- Conversion logs sync runs as step 12 of 14 in bigbang migration
- Happens after conversions are migrated (step 11)
- Happens before cursor initialization (step 14)

**Standalone:**
- Can be run any time after bigbang completes
- Can be run multiple times (idempotent)
- Can be used to re-populate logs if filesystem was unavailable during bigbang

### Filesystem Access

The conversion logs sync **requires access** to `/N/project/CMG-SCA/production/runlogs/`.

**If directory is not accessible:**
- Bigbang will log a warning and continue (skip conversion logs)
- Standalone script will log an error and exit gracefully
- No database writes occur

**In local/dev environments:**
- Directory does not exist (expected)
- Use `--skip-conversion-logs` flag in bigbang to avoid errors
- Standalone script will skip with informational message

### Performance

**Typical metrics:**
- ~2,700 conversions processed
- ~100 datasets with log files
- ~10-50 KB per log file
- Processing time: 5-15 minutes

**Memory usage:**
- Batch processing prevents memory issues
- 1000 log entries per batch
- Suitable for large log files

## Troubleshooting

### Container Not Running

```
Error: db_sandbox container is not running
```

**Solution:**
```bash
cd /opt/sca/cmg/data_sync
docker compose -f docker-compose.sandbox.yml up -d
```

### Filesystem Not Accessible

```
[CONVERSION LOGS] Logs directory not accessible: /opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs
```

**Causes:**
- Running in local/dev environment (expected)
- Filesystem not mounted in production (check docker-compose.sandbox.yml)
- Permissions issue on host

**Solution:**
- In local/dev: Use `--skip-conversion-logs` flag in bigbang
- In production: Verify `/N/project/` is mounted in container

### Missing Log Files

```
Log file not found: /path/to/convert_datasetname.log
```

**Causes:**
- Log file was never created (conversion ran before logging was implemented)
- Dataset name mismatch (file uses different naming convention)
- Log file was deleted or archived

**Solution:**
- Script will skip missing files and continue
- Check `missingLogFiles` count in final summary
- Missing log files are expected for some old conversions

### Already Processed

```
Conversions already processed (skipped): 2500
```

**Meaning:**
- These conversions already have `workflow_id` populated
- Logs were populated in a previous run

**Solution:**
- This is normal behavior (idempotency)
- Use `--overwrite-existing` to force re-processing

## Integration with Bigbang

The conversion logs sync is **step 12** in the bigbang migration order:

1. Create roles
2. Create CMG system user
3. Populate pipeline definitions
4. Convert users
5. Convert datasets (RAW_DATA and DATA_PRODUCT)
6. Convert audit logs
7. Convert import logs
8. Convert dataset hierarchies
9. Convert projects
10. Convert conversions
11. **← Conversions must exist before logs can be populated**
12. **Convert conversion logs (THIS STEP)**
13. Convert sessions
14. Initialize cursors

**To skip during bigbang:**
```bash
node src/bigbang_sync.js --skip-conversion-logs
```

## Related Documentation

- `BIGBANG_SYNC_USAGE.md` - Full bigbang migration guide
- `TARGET_DATABASE_CONFIGURATION.md` - Database targeting options
- `.ai/customizations/features/cmg-database-migration.md` - Architecture decisions
- `.ai/customizations/features/conversions.md` - Conversions feature overview

---

**Last Updated:** 2026-01-19


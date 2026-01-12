# CMG Incremental Poller - Usage Guide

## Overview

The incremental poller system provides **continuous synchronization** of CMG changes into Bioloop after the big-bang migration. It runs indefinitely (managed by PM2) and polls CMG for changes every 10-15 seconds.

## Prerequisites

1. **Big-Bang Complete**: Run `cmg_bigbang_sync.js` first
2. **Prisma Migration**: Sync tables must exist
3. **MongoDB Access**: CMG and Rhythm MongoDB credentials configured

## How It Works

The poller system consists of 5 independent pollers:

### 1. User Roles Poller (`user_roles`)
- **Polls**: CMG `users` collection
- **Interval**: 10 seconds
- **Updates**: User active status, role assignments
- **Does NOT update**: username, name, email, cas_id (immutable)

### 2. Project ACL Poller (`project_acl`)
- **Polls**: CMG `projects` collection
- **Interval**: 10 seconds
- **Updates**: Project description, browser_enabled, user/dataset associations
- **Does NOT update**: name, slug (immutable)
- **Operation**: Rebuilds associations (delete + insert)

### 3. Dataset Activity Poller (`dataset_activity`)
- **Polls**: CMG `datasets` and `dataproducts` collections (alternating)
- **Interval**: 10 seconds
- **Updates**: Paths (origin, archive, staged), is_staged flag
- **Does NOT**: Parse events, populate dataset_file

### 4. Dataset Metadata Poller (`dataset_metadata`)
- **Polls**: CMG `datasets` and `dataproducts` collections (alternating)
- **Interval**: 15 seconds
- **Updates**: Description, size, du_size, num_files, num_directories, file_type
- **Does NOT update**: name, type (immutable)

### 5. Workflow Status Poller (`workflow_status`)
- **Polls**: Rhythm MongoDB `workflow_meta` collection
- **Interval**: 10 seconds
- **Updates**: Dataset states based on completed workflows
- **Workflows tracked**: `integrated`, `stage`

## Running the Poller

### Method 1: Direct Execution (Development)

```bash
cd /opt/sca/app  # Or your project root

# Normal run (will fail if another instance is running)
node src/scripts/cmg_poller_sync.js

# Clear stale locks before starting
node src/scripts/cmg_poller_sync.js --clear-locks

# Show help
node src/scripts/cmg_poller_sync.js --help
```

**Command-line Options:**
- `--clear-locks`: Clear any existing process locks before starting (useful if previous instance crashed)
- `--help, -h`: Show usage information

### Method 2: PM2 (Production - Recommended)

**Start all services including poller:**
```bash
pm2 start ecosystem.config.js
```

**Start only the poller:**
```bash
pm2 start ecosystem.config.js --only cmg-poller
```

**View logs:**
```bash
pm2 logs cmg-poller
```

**View status:**
```bash
pm2 status
pm2 show cmg-poller
```

**Restart poller:**
```bash
pm2 restart cmg-poller
```

**Stop poller:**
```bash
pm2 stop cmg-poller
```

## Configuration

All configuration comes from the config system (environment variables):

### CMG MongoDB
```bash
CMG_MONGO_HOST=commons3.sca.iu.edu
CMG_MONGO_PORT=27017
CMG_MONGO_DB=cmg
CMG_MONGO_USERNAME=cmg
CMG_MONGO_PASSWORD=password
```

### Rhythm MongoDB
```bash
RHYTHM_MONGO_HOST=rhythm-host
RHYTHM_MONGO_PORT=27018
RHYTHM_MONGO_DB=celery
RHYTHM_MONGO_USERNAME=appuser
RHYTHM_MONGO_PASSWORD=password
```

## Polling Strategy

### Cursor-Based Incremental Sync

Each poller maintains a cursor in the `cmg_sync_cursor` table:

```sql
SELECT * FROM cmg_sync_cursor;
```

Example output:
```
 poller_name       | last_updated_at      | last_cmg_objectid | locked_by | lock_expires_at
-------------------+----------------------+-------------------+-----------+-----------------
 user_roles        | 2026-01-08 10:30:45  | 507f1f77bcf86cd7  | NULL      | NULL
 project_acl       | 2026-01-08 10:30:42  | 507f1f77bcf86cd8  | NULL      | NULL
 dataset_activity  | 2026-01-08 10:30:40  | 507f1f77bcf86cd9  | NULL      | NULL
 dataset_metadata  | 2026-01-08 10:30:38  | 507f1f77bcf86cda  | NULL      | NULL
 workflow_status   | 2026-01-08 10:30:35  | 507f1f77bcf86cdb  | NULL      | NULL
```

### Lock Mechanism

- Each poller acquires a lock before running
- Lock TTL: 60 seconds (prevents stuck locks)
- If lock held by another instance, skip the round
- Enables running multiple poller processes (though not recommended)

### Bounded Window Query

Pollers only fetch documents with `updatedAt <= roundStart` to avoid:
- Processing actively-being-updated documents
- Missing updates due to race conditions

### Time Budget

Each batch has a 30-second time budget:
- If exceeded, batch stops early
- Cursor updated to last processed document
- Next round continues from where it left off

## Monitoring

### Real-Time Logs

```bash
# Follow poller logs
pm2 logs cmg-poller --lines 100

# Follow specific poller activity
pm2 logs cmg-poller | grep user_roles
```

### Metrics Dashboard

The poller logs metrics every 60 seconds:

```
--- Poller Metrics ---
[user_roles]
  Running: true
  Total runs: 360
  Successful: 358
  Failed: 2
  Total processed: 1523
  Last run: 2026-01-08T10:30:45.000Z
  Last success: 2026-01-08T10:30:45.000Z
```

### Database Queries

**Check cursor status:**
```sql
SELECT 
  poller_name,
  last_updated_at,
  last_succeeded_at,
  last_failed_at,
  last_run_count,
  last_error
FROM cmg_sync_cursor
ORDER BY poller_name;
```

**Check for failed documents:**
```sql
SELECT 
  poller_name,
  cmg_id,
  failure_count,
  last_error,
  next_retry_at
FROM cmg_sync_retry
WHERE failure_count > 3
ORDER BY failure_count DESC;
```

**Check cursor lag:**
```sql
SELECT 
  poller_name,
  EXTRACT(EPOCH FROM (NOW() - last_updated_at)) / 60 as minutes_behind
FROM cmg_sync_cursor;
```

### Error Logs

Failed operations are logged to `/opt/sca/data/logs/cmg_sync_errors.jsonl`:

```bash
tail -f /opt/sca/data/logs/cmg_sync_errors.jsonl | jq
```

Each error entry includes:
- Timestamp
- Poller name
- CMG collection
- CMG document ID
- Full error stack
- Full CMG document
- Attempted Prisma query

## Troubleshooting

### Poller Not Starting

```
[FAILED] Poller initialization failed
```

**Check:**
1. MongoDB credentials
2. Prisma connection
3. Big-bang completed (cursors initialized)

```bash
# Test MongoDB connection
mongosh "mongodb://user:pass@host:27017/cmg"

# Check cursors exist
psql -d bioloop -c "SELECT * FROM cmg_sync_cursor;"
```

### Poller Stuck / Another Instance Running

If you see the error: `[FAILED] Another poller process is already running`

**Option 1: Restart with --clear-locks flag (Recommended)**
```bash
# Kill the stuck process first
pm2 stop cmg-poller

# Restart with lock clearing
node src/scripts/cmg_poller_sync.js --clear-locks

# Or if using PM2, manually clear locks then restart
pm2 restart cmg-poller
```

**Option 2: Manually clear process locks in database**
```sql
-- Check for stuck process locks
SELECT * FROM cmg_sync_process_lock WHERE locked_by IS NOT NULL;

-- Force release process lock
UPDATE cmg_sync_process_lock 
SET locked_by = NULL, lock_expires_at = NULL 
WHERE process_name = 'poller';

-- Also check individual poller locks
SELECT * FROM cmg_sync_cursor WHERE locked_by IS NOT NULL;

-- Force release individual poller lock (if needed)
UPDATE cmg_sync_cursor 
SET locked_by = NULL, lock_expires_at = NULL 
WHERE poller_name = 'user_roles';
```

Then restart:
```bash
pm2 restart cmg-poller
```

**Note:** Process-level locks (`cmg_sync_process_lock`) prevent multiple poller script instances. Individual poller locks (`cmg_sync_cursor`) prevent concurrent runs of the same poller type.

### High Error Rate

```sql
-- Check retry queue
SELECT poller_name, COUNT(*) as error_count
FROM cmg_sync_retry
GROUP BY poller_name;
```

**Common causes:**
- CMG document references non-existent Bioloop data
- Data type mismatches
- Constraint violations

**Fix:** Check error logs for specific failures:
```bash
grep "poller_name.*user_roles" /opt/sca/data/logs/cmg_sync_errors.jsonl | jq
```

### Cursor Lag

If `minutes_behind` is high:

1. **Increase batch size** (if memory allows)
2. **Decrease poll interval** (if load allows)
3. **Check for slow queries** (Prisma slow query log)

### Memory Issues

```bash
# Check memory usage
pm2 show cmg-poller
```

If memory restarts frequently:
- Increase `max_memory_restart` in `ecosystem.config.js`
- Decrease batch sizes in poller constructors

## Graceful Shutdown

The poller handles shutdown signals gracefully:

```bash
# Send SIGTERM (PM2 does this automatically)
pm2 stop cmg-poller
```

Shutdown process:
1. Stop all polling loops
2. Wait for current batches to complete
3. Close MongoDB connections
4. Close Prisma connection
5. Exit cleanly

## Performance Tuning

### Batch Sizes

Default batch sizes (documents per poll):
- user_roles: 200
- project_acl: 100 (smaller due to expensive operations)
- dataset_activity: 200
- dataset_metadata: 200
- workflow_status: 200

To change, edit poller constructors in `cmg_poller_sync.js`:

```javascript
const userRolesPoller = new UserRolesPoller(prisma, cmgDb, {
  batchSize: 300,  // Increase batch size
});
```

### Poll Intervals

Default intervals:
- user_roles: 10s (critical for auth)
- project_acl: 10s (critical for access)
- dataset_activity: 10s
- dataset_metadata: 15s (less critical)
- workflow_status: 10s

To change:

```javascript
const datasetMetadataPoller = new DatasetMetadataPoller(prisma, cmgDb, {
  pollIntervalMs: 20000,  // 20 seconds instead of 15
});
```

## Production Checklist

- [ ] Big-bang migration completed successfully
- [ ] Environment variables configured
- [ ] PM2 ecosystem.config.js updated
- [ ] Log directories exist: `/opt/sca/data/logs/`
- [ ] Database queries verified (cursor lag < 5 minutes)
- [ ] Monitoring dashboard set up
- [ ] Error alerting configured
- [ ] PM2 monitoring active: `pm2 startup` and `pm2 save`

## Differences from Big-Bang

| Aspect | Big-Bang | Incremental Poller |
|--------|----------|-------------------|
| **Runs** | Once | Continuously |
| **Data** | All historical data | Only changes since last poll |
| **Order** | Strict (users → datasets → projects) | Independent (any order) |
| **Cursor** | Not used | Tracks last processed document |
| **Locks** | Not needed | Required (prevents concurrent runs) |
| **Speed** | Slow (processes everything) | Fast (only changed docs) |
| **Purpose** | Initial population | Keep in sync |

## Next Steps After Starting Poller

1. **Monitor for 1 hour** - Check logs, metrics, cursor lag
2. **Verify data consistency** - Compare sample records between CMG and Bioloop
3. **Test a change** - Update a user role in CMG, verify it appears in Bioloop within 30 seconds
4. **Set up alerting** - Monitor cursor lag, error rates
5. **Document any custom configurations** - Batch sizes, intervals, etc.

---

**Last Updated**: 2026-01-08


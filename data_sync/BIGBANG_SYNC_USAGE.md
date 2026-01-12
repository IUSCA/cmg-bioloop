# CMG Big-Bang Sync - Usage Guide

## Overview

The big-bang synchronization script performs a **one-time migration** of all existing CMG data into Bioloop. It follows the exact same operations and order as the Python script `db_conversion/src/convert/scripts/convert.py`.

## Prerequisites

1. **Prisma Migration**: Run migration to create sync tables
   ```bash
   cd api
   npx prisma migrate dev --name add_cmg_sync_tables
   npx prisma generate
   ```

2. **MongoDB Access**: Ensure you have credentials for:
   - CMG MongoDB (legacy data)
   - Rhythm MongoDB (workflow data)

3. **Clean Bioloop Database**: The script assumes an empty or minimal Bioloop database

## Usage

### Method 1: Using Command-Line URI

Provide MongoDB connection string directly:

```bash
# Inside the sandbox container
docker exec -it bioloop_db_sandbox bash
cd /opt/sca/app

# Basic usage
node src/bigbang_sync.js \
  --cmg-uri="mongodb://username:password@host:27017/cmg"

# Skip sessions and clear any stale locks
node src/bigbang_sync.js \
  --cmg-uri="mongodb://username:password@host:27017/cmg" \
  --skip-sessions \
  --clear-locks
```

### Method 2: Using Environment Variables

Set environment variables in `.env` file and use the config system:

```bash
# From host, exec into container
docker exec -it bioloop_db_sandbox bash
cd /opt/sca/app

# Run the script (reads from .env file)
node src/bigbang_sync.js
```

### Method 3: One-Liner from Host

```bash
docker exec -it bioloop_db_sandbox node /opt/sca/app/src/bigbang_sync.js --cmg-uri="mongodb://..."
```

## Options

| Flag | Description |
|------|-------------|
| `--cmg-uri=<uri>` | MongoDB connection string for CMG database |
| `--skip-sessions` | Skip genome browser session conversion (recommended for first run) |
| `--clear-locks` | Clear any existing process locks before starting (useful if previous run crashed) |
| `--help`, `-h` | Show help message |

## MongoDB Connection String Format

```
mongodb://username:password@host:port/database
```

**Example**:
```
mongodb://cmg:All%20the%20GATTACA%20all%20the%20time!@commons3.sca.iu.edu:27017/cmg
```

**Note**: URL-encode special characters in password (e.g., spaces become `%20`)

## Migration Order

The script executes operations in this exact order (matching `convert.py`):

1. **Create roles** - `admin`, `operator`, `user`
2. **Create CMG system user** - `cmguser` for system operations
3. **Populate pipeline definitions** - cmd_line_programs, conversion_definitions, arguments
4. **Convert users** - CMG users → Bioloop users with role mappings
5. **Convert datasets** - RAW_DATA (datasets) and DATA_PRODUCT (dataproducts)
6. **Convert dataset audit logs** - From CMG events arrays
7. **Convert dataset hierarchies** - Links between raw data and derived products
8. **Convert projects** - With user and dataset associations
9. **Convert conversions** - Pipeline runs with derived dataset links
10. **Convert sessions** - Genome browser sessions (optional, often skipped)
11. **Initialize cursors** - Set starting points for incremental pollers

## What Gets Migrated

### Migrated
- All users with role mappings
- All datasets (RAW_DATA and DATA_PRODUCT)
- Dataset genomic attributes
- Dataset hierarchies (source → derived relationships)
- All projects with associations (users, datasets)
- All conversions with derived datasets
- Dataset audit logs (from events)
- Poller cursor initialization

### Partially Migrated
- **Genome browser sessions**: Most will be skipped due to missing `dataset_file` records (we don't populate this table in big-bang)

### Not Migrated
- `dataset_file` table - Not populated (large files, done on-demand)
- `dataset_file_hierarchy` table - Not populated
- System events (from CMG `events` collection) - Only dataset events are converted

## Error Handling

The script follows a **fail-fast** approach:
- Only catches expected errors (e.g., unique constraint violations for duplicate users)
- All unexpected errors will stop the migration and propagate up
- No hidden errors - everything is logged and visible

## Expected Warnings

You may see these warnings (they are normal):

```
[BIGBANG] Skipping duplicate user: email@example.com
[BIGBANG] User not found for session <id>
[BIGBANG] Dataset not found for track in session <id>
```

These indicate:
- Duplicate users being skipped
- Sessions referencing missing users or datasets
- Tracks referencing missing dataset_files

## After Big-Bang

1. **Verify Data Integrity**
   ```sql
   -- Check counts
   SELECT COUNT(*) FROM "user" WHERE cmg_id IS NOT NULL;
   SELECT COUNT(*) FROM dataset WHERE cmg_id IS NOT NULL;
   SELECT COUNT(*) FROM project WHERE cmg_id IS NOT NULL;
   
   -- Check associations
   SELECT COUNT(*) FROM project_user;
   SELECT COUNT(*) FROM project_dataset;
   SELECT COUNT(*) FROM dataset_hierarchy;
   ```

2. **Check Cursors**
   ```sql
   SELECT * FROM cmg_sync_cursor;
   ```
   
   Should show 5 cursors with `last_updated_at` set to max CMG updatedAt values.

3. **Start Incremental Poller**
   ```bash
   # The poller script
   node src/poller_sync.js
   ```

## Troubleshooting

### Another Instance Already Running

```
[FAILED] Another big-bang process is already running
```

**Cause**: The script uses a process-level lock to prevent multiple instances from running simultaneously.

**Fix Option 1 (Recommended)**: Restart with `--clear-locks` flag
```bash
node src/bigbang_sync.js --clear-locks
```

**Fix Option 2**: Manually clear the lock in database
```sql
-- Check for stuck process lock
SELECT * FROM cmg_sync_process_lock WHERE process_name = 'bigbang';

-- Force release process lock
UPDATE cmg_sync_process_lock 
SET locked_by = NULL, lock_expires_at = NULL 
WHERE process_name = 'bigbang';
```

Then re-run the script normally.

### Connection Issues

```
Error: MongoDB connection failed
```

**Fix**: Check credentials, host, and port. Test with `mongosh`:
```bash
mongosh "mongodb://user:pass@host:27017/cmg"
```

### Prisma Errors

```
Error: P2002 Unique constraint failed on fields: (cmg_id)
```

**Fix**: Database is not clean. Either:
- Run big-bang on a fresh database, OR
- The script is being re-run (it should be idempotent for most operations)

### Missing Data

```
[BIGBANG] User not found for CMG ID: <id>
```

**Fix**: This is expected - the script logs and continues. Some CMG data may reference users/datasets that don't exist or were skipped.

## Estimated Duration

Depends on CMG data size:
- Small dataset (< 1000 records): ~1-2 minutes
- Medium dataset (1000-10000 records): ~5-15 minutes
- Large dataset (> 10000 records): ~30-60 minutes

## Re-running Big-Bang

**Caution**: The script is designed for initial population. Re-running may cause issues:

- **Roles**: Will fail due to unique constraints (expected)
- **Users**: Duplicates will be skipped
- **Datasets**: May create duplicates with `DUPLICATE_` prefix
- **Projects**: May fail due to unique slug constraints

**Recommendation**: Only run big-bang once. Use the incremental poller for updates.

## Differences from Python Script

### Same Operations
- Exact same order of operations
- Same entity conversion logic
- Same duplicate handling
- Same role mapping

### Key Differences
1. **Language**: JavaScript/Node.js vs Python
2. **Database Client**: Prisma vs psycopg2
3. **Error Handling**: Fail-fast vs continue-on-error
4. **Configuration**: Uses Bioloop config system vs dotenv
5. **Connection String**: Can be provided via CLI or env vars

### Compatibility
The JavaScript version produces **identical results** to the Python version when given the same CMG data.

## Files Created

```
api/src/scripts/
├── cmg_bigbang_sync.js                    # Main script
└── cmg_sync/
    ├── bigbang/
    │   ├── seed_constants.js              # Roles, programs, definitions
    │   ├── sync_users.js                  # Users + roles
    │   ├── sync_datasets.js               # Datasets (RAW_DATA + DATA_PRODUCT)
    │   ├── sync_audit_logs.js             # Audit logs from events
    │   ├── sync_dataset_hierarchies.js    # Dataset relationships
    │   ├── sync_projects.js               # Projects + associations
    │   ├── sync_conversions.js            # Conversions + derived datasets
    │   ├── sync_sessions.js               # Genome browser sessions
    │   └── initialize_cursors.js          # Poller cursor setup
    └── utils/
        ├── cmg_helpers.js                 # CMG utility functions
        ├── role_mapper.js                 # Role mapping
        └── duplicate_handler.js           # Duplicate name handling
```

## Support

For issues or questions:
1. Check logs for specific error messages
2. Verify MongoDB connectivity
3. Ensure Prisma schema is up to date
4. Review CMG data structure in MongoDB

---

**Last Updated**: 2026-01-08


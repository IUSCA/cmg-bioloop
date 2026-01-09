# CMG to Bioloop Synchronization - Implementation Complete

**Date**: 2026-01-08  
**Status**: Implementation Complete - Ready for Testing

---

## Overview

Complete implementation of a two-phase synchronization system between CMG (MongoDB) and Bioloop (PostgreSQL):

1. **Big-Bang Script**: One-time initial population of all existing CMG data
2. **Incremental Pollers**: Continuous synchronization of CMG changes

Both systems follow the exact same operations and order as the original Python migration script (`db_conversion/src/convert/scripts/convert.py`).

---

## Phase 1: Big-Bang Migration (Complete)

### Purpose
One-time migration of all existing CMG data into an empty/minimal Bioloop database.

### Files Created

```
api/src/scripts/
├── cmg_bigbang_sync.js                    # Main orchestration script
├── CMG_BIGBANG_SYNC_USAGE.md              # Usage documentation
└── cmg_sync/bigbang/
    ├── seed_constants.js                   # Roles, programs, definitions
    ├── sync_users.js                       # Users + roles
    ├── sync_datasets.js                    # Datasets (RAW_DATA + DATA_PRODUCT)
    ├── sync_audit_logs.js                  # Audit logs from events
    ├── sync_dataset_hierarchies.js         # Dataset relationships
    ├── sync_projects.js                    # Projects + associations
    ├── sync_conversions.js                 # Conversions + derived datasets
    ├── sync_sessions.js                    # Genome browser sessions
    └── initialize_cursors.js               # Poller cursor setup
```

### Usage

```bash
# Using MongoDB URI
node src/scripts/cmg_bigbang_sync.js \
  --cmg-uri="mongodb://user:pass@host:27017/cmg?authSource=admin"

# Using environment variables (via config)
node src/scripts/cmg_bigbang_sync.js

# Skip sessions (recommended for first run)
node src/scripts/cmg_bigbang_sync.js --skip-sessions
```

### Migration Order (11 Steps)

1. Create roles (admin, operator, user)
2. Create CMG system user (cmguser)
3. Populate pipeline definitions (programs, conversions, arguments)
4. Convert users with role mappings
5. Convert datasets (RAW_DATA and DATA_PRODUCT)
6. Convert dataset audit logs
7. Convert dataset hierarchies
8. Convert projects with associations
9. Convert conversions with derived datasets
10. Convert genome browser sessions
11. Initialize poller cursors

---

## Phase 2: Incremental Poller System (Complete)

### Purpose
Continuous synchronization of CMG changes after big-bang migration. Runs indefinitely via PM2.

### Files Created

```
api/src/scripts/
├── cmg_poller_sync.js                     # Main poller orchestrator
├── CMG_POLLER_SYNC_USAGE.md               # Usage documentation
└── cmg_sync/pollers/
    ├── base_poller.js                      # Abstract base class
    ├── user_roles_poller.js                # User role sync
    ├── project_acl_poller.js               # Project ACL sync
    ├── dataset_activity_poller.js          # Dataset paths/flags
    ├── dataset_metadata_poller.js          # Dataset metadata
    └── workflow_status_poller.js           # Workflow states (existing)
```

### Pollers

| Poller | Source | Interval | Updates |
|--------|--------|----------|---------|
| user_roles | CMG users | 10s | Active status, roles |
| project_acl | CMG projects | 10s | Description, user/dataset associations |
| dataset_activity | CMG datasets/dataproducts | 10s | Paths, is_staged flag |
| dataset_metadata | CMG datasets/dataproducts | 15s | Size, description, counts |
| workflow_status | Rhythm workflow_meta | 10s | Dataset states from workflows |

### Usage

```bash
# Direct execution (development)
node src/scripts/cmg_poller_sync.js

# PM2 (production)
pm2 start ecosystem.config.js --only cmg-poller
pm2 logs cmg-poller
pm2 status
```

### Key Features

- **Cursor-based**: Only fetches changed documents
- **Lock mechanism**: Prevents concurrent runs
- **Bounded window**: Avoids race conditions
- **Time budget**: Prevents runaway batches
- **Error tracking**: Logs all failures with full context
- **Metrics**: Reports statistics every 60 seconds
- **Graceful shutdown**: Handles SIGTERM/SIGINT properly

---

## Supporting Files

### Shared Utilities

```
api/src/scripts/cmg_sync/
├── connections.js                         # MongoDB + Prisma connection management
├── constants.js                           # Seeding constants (roles, programs)
├── cursor_manager.js                      # Lock acquisition/release, cursor updates
├── error_logger.js                        # JSONL error logging
└── utils/
    ├── cmg_helpers.js                     # CMG document utilities
    ├── role_mapper.js                     # CMG → Bioloop role mapping
    ├── duplicate_handler.js               # Handle duplicate dataset names
    ├── event_parser.js                    # Parse CMG events (not used currently)
    └── state_mapper.js                    # Event → State mapping
```

### Database Schema

Already exists in `api/prisma/schema.prisma`:

```prisma
model cmg_sync_cursor {
  poller_name       String    @id
  last_updated_at   DateTime?
  last_cmg_objectid String?
  locked_by         String?
  lock_expires_at   DateTime?
  last_started_at   DateTime?
  last_succeeded_at DateTime?
  last_failed_at    DateTime?
  last_error        String?
  last_run_count    Int?
  updated_at        DateTime  @updatedAt
}

model cmg_sync_retry {
  id               Int      @id @default(autoincrement())
  poller_name      String
  cmg_id           String
  failure_count    Int      @default(0)
  last_error       String?
  next_retry_at    DateTime?
  updated_at       DateTime @updatedAt

  @@unique([poller_name, cmg_id])
}
```

### PM2 Configuration

Updated `api/ecosystem.config.js`:

```javascript
{
  script: 'src/scripts/cmg_poller_sync.js',
  name: 'cmg-poller',
  exec_mode: 'fork',
  instances: 1,
  autorestart: true,
  max_memory_restart: '1G',
  error_file: '/opt/sca/data/logs/cmg_poller_error.log',
  out_file: '/opt/sca/data/logs/cmg_poller_out.log',
}
```

---

## Prerequisites (User Action Required)

### 1. Run Prisma Migration

```bash
cd /path/to/cmg-bioloop/api
npx prisma migrate dev --name add_cmg_sync_tables
npx prisma generate
```

### 2. Configure Environment Variables

**CMG MongoDB:**
```bash
export CMG_MONGO_HOST=commons3.sca.iu.edu
export CMG_MONGO_PORT=27017
export CMG_MONGO_DB=cmg
export CMG_MONGO_AUTH_SOURCE=admin
export CMG_MONGO_USERNAME=cmg
export CMG_MONGO_PASSWORD='your_password'
```

**Rhythm MongoDB:**
```bash
export RHYTHM_MONGO_HOST=rhythm-host
export RHYTHM_MONGO_PORT=27018
export RHYTHM_MONGO_DB=celery
export RHYTHM_MONGO_AUTH_SOURCE=admin
export RHYTHM_MONGO_USERNAME=appuser
export RHYTHM_MONGO_PASSWORD='your_password'
```

### 3. Verify Connectivity

```bash
# Test CMG MongoDB
mongosh "mongodb://cmg:password@host:27017/cmg?authSource=admin"

# Test Rhythm MongoDB
mongosh "mongodb://user:password@host:27018/celery?authSource=admin"
```

---

## Execution Plan

### Step 1: Run Big-Bang

```bash
cd /opt/sca/app  # API container or project root
node src/scripts/cmg_bigbang_sync.js --skip-sessions
```

Expected duration: 5-60 minutes depending on data size

### Step 2: Verify Big-Bang Results

```sql
-- Check migrated counts
SELECT COUNT(*) FROM "user" WHERE cmg_id IS NOT NULL;
SELECT COUNT(*) FROM dataset WHERE cmg_id IS NOT NULL;
SELECT COUNT(*) FROM project WHERE cmg_id IS NOT NULL;

-- Check cursors initialized
SELECT * FROM cmg_sync_cursor ORDER BY poller_name;

-- Check associations
SELECT COUNT(*) FROM project_user;
SELECT COUNT(*) FROM project_dataset;
SELECT COUNT(*) FROM dataset_hierarchy;
```

### Step 3: Start Incremental Poller

```bash
# Development (foreground)
node src/scripts/cmg_poller_sync.js

# Production (PM2)
pm2 restart ecosystem.config.js
pm2 logs cmg-poller
```

### Step 4: Monitor

```bash
# View logs
pm2 logs cmg-poller --lines 200

# Check metrics (logged every 60s)
pm2 logs cmg-poller | grep "Poller Metrics"

# Check cursor status
psql -d bioloop -c "SELECT * FROM cmg_sync_cursor;"

# Check error log
tail -f /opt/sca/data/logs/cmg_sync_errors.jsonl | jq
```

---

## Design Principles

### Error Handling
- **Fail-fast approach**: Only catch expected errors (e.g., P2002 unique violations)
- **No hidden errors**: All unexpected errors propagate and stop execution
- **Comprehensive logging**: Full context for every error (document, query, stack trace)

### Idempotency
- **cmg_id as anchor**: Enables upsert operations
- **Big-bang is re-runnable**: Safe to run multiple times (with caveats)
- **Pollers are idempotent**: Safe to process same document multiple times

### Data Consistency
- **Transactions**: All batch operations use Prisma transactions
- **Bounded window**: Prevents race conditions with active updates
- **Cursor progression**: Only advances on successful batch commit
- **Lock mechanism**: Prevents concurrent runs

### Performance
- **Batch processing**: 100-200 documents per batch
- **Time budget**: 30-second max per batch
- **Connection pooling**: Reuses MongoDB and Prisma connections
- **Cursor-based**: Only fetches changed documents

---

## What Gets Synced

### Big-Bang (Initial Population)
- All users with roles
- All datasets (RAW_DATA and DATA_PRODUCT)
- Dataset genomic attributes
- Dataset hierarchies
- All projects with associations
- All conversions
- Dataset audit logs (from events)
- Genome browser sessions (most skipped due to missing dataset_file)

### Incremental Pollers (Continuous)

**Immutable Fields (Never Updated After Big-Bang):**
- User: username, name, email, cas_id
- Dataset: name, type
- Project: name, slug

**Updated Fields:**
- User: is_deleted, roles
- Dataset: paths, is_staged, size, description, metadata
- Project: description, browser_enabled, user/dataset associations
- Dataset states: INSPECTED, ARCHIVED, STAGED, READY (from workflows)

---

## Monitoring & Operations

### Health Checks

```sql
-- Cursor lag (should be < 5 minutes)
SELECT 
  poller_name,
  EXTRACT(EPOCH FROM (NOW() - last_updated_at)) / 60 as minutes_behind
FROM cmg_sync_cursor;

-- Error rate
SELECT poller_name, COUNT(*) 
FROM cmg_sync_retry 
GROUP BY poller_name;

-- Recent failures
SELECT * FROM cmg_sync_cursor 
WHERE last_failed_at > last_succeeded_at;
```

### Common Issues

**Stuck Lock:**
```sql
UPDATE cmg_sync_cursor 
SET locked_by = NULL, lock_expires_at = NULL 
WHERE poller_name = 'user_roles';
```

**Reset Cursor (re-sync from scratch):**
```sql
UPDATE cmg_sync_cursor 
SET last_updated_at = NULL, last_cmg_objectid = NULL 
WHERE poller_name = 'user_roles';
```

---

## Testing Checklist

- [ ] Run Prisma migration
- [ ] Test MongoDB connectivity (CMG and Rhythm)
- [ ] Run big-bang with sample data
- [ ] Verify data integrity (counts, associations)
- [ ] Start incremental poller
- [ ] Make a change in CMG, verify it appears in Bioloop within 30s
- [ ] Check error logs for issues
- [ ] Monitor cursor lag
- [ ] Test graceful shutdown (Ctrl+C)
- [ ] Test PM2 management (start, stop, restart, logs)

---

## Documentation Files

1. `CMG_BIGBANG_SYNC_USAGE.md` - Big-bang script usage
2. `CMG_POLLER_SYNC_USAGE.md` - Incremental poller usage
3. `CMG_BIOLOOP_DATABASE_SYNC---POLLING.md` - Architecture and design (existing)
4. `CMG_SYNC_IMPLEMENTATION_COMPLETE.md` - This file (summary)

---

## Status

**Implementation**: Complete  
**Testing**: Ready  
**Production Deployment**: Pending user actions (Prisma migration, credentials, testing)

All code is emoji-free, follows fail-fast error handling, and matches the Python implementation order exactly.

---

**Last Updated**: 2026-01-08


# CMG-Bioloop Migration & Workflow Analysis: Summary
**Date:** 2026-01-03  
**Status:** Analysis Complete - Ready for Implementation Planning

---

## What Was Analyzed

This analysis examined two critical aspects of the Bioloop system to inform the new JavaScript-based CMG synchronization process:

1. **Current Migration Process** (`CURRENT_MIGRATION_PROCESS_ANALYSIS.md`)
   - How the Python scripts convert CMG MongoDB to Bioloop PostgreSQL
   - Dependencies between conversion steps
   - Special handling for duplicates, role mapping, and associations
   - Immutable vs. mutable fields

2. **Workflow Architecture** (`BIOLOOP_WORKFLOW_ARCHITECTURE.md`)
   - How Bioloop uses sca_rhythm for orchestrating dataset operations
   - Integration between Bioloop API, Rhythm API, and Workers
   - Workflow types (staging, archival, conversion, etc.)
   - Data storage across PostgreSQL and Rhythm MongoDB

---

## Key Findings

### 1. Migration Process Dependencies

**Critical Order:**
```
Roles → Users → Datasets → Audit Logs → Hierarchies → Projects → Conversions → Sessions
```

**Why This Matters:**
- New sync process must respect these dependencies
- Can't create projects without users and datasets existing first
- Can't link conversions without datasets and conversion definitions

**Key Pattern:**
- Every Bioloop table has a `cmg_id` field
- This enables idempotent upserts: "Find by cmg_id, create if not exists, update if exists"

### 2. One-Time Seeding vs. Continuous Sync

**One-Time (Constants):**
- `role` table (hardcoded: user, operator, admin)
- `cmd_line_program` table (bcl2fastq, cellranger, etc.)
- `conversion_definition` table (pipeline definitions)
- `argument` table (conversion arguments)

**Continuous (CMG Data):**
- `user`, `user_role` (from CMG users)
- `dataset`, `dataset_genomic_attributes` (from CMG datasets/dataproducts)
- `project`, `project_user`, `project_dataset` (from CMG projects)
- `conversion`, `conversion_derived_dataset` (from CMG conversions)
- `genome_browser_session`, `track`, `session_track` (from CMG sessions)
- `dataset_audit` (from CMG events arrays)
- `dataset_hierarchy` (derived from dataproduct.dataset references)

**Implication:**
- Initial sync must seed constants once
- Daily sync only updates CMG-derived data

### 3. Workflow Table is Optional

**Current Migration:** `create_workflows_for_past_stagings()` is **commented out**

**Reason:** 
- Creating historical workflows is time-consuming
- Not necessary for Bioloop functionality
- Users care about current dataset state, not workflow history

**For New Sync Process:**
- **Skip `workflow` table** entirely for now
- Focus on dataset state (`is_staged`, `archive_path`, etc.)
- Populate `dataset_audit` instead (simpler, append-only)

### 4. Workflows Are Future-Only

**Bioloop Workflows:**
- Track **live operations** initiated by Bioloop users
- Stored in Rhythm MongoDB + Bioloop PostgreSQL
- **Not relevant** to CMG sync

**CMG Events:**
- Historical record of what happened in CMG
- Should sync to `dataset_audit` table (audit logs)
- **Don't create Bioloop workflows** for past CMG operations

**Key Insight:**
```
CMG events → Bioloop audit logs  ✅
CMG events → Bioloop workflows   ❌
```

### 5. Special Handling Requirements

**Duplicate Names:**
- CMG allows duplicate dataset names
- Bioloop has UNIQUE constraint on (name, type, is_deleted)
- Solution: Prefix with `DUPLICATE_`, `DUPLICATE_2_`, etc.

**Group Expansion:**
- CMG has `groups` containing `users`
- Bioloop has no groups; only direct user associations
- Solution: Expand groups to individual users every sync

**Role Mapping:**
- CMG roles ≠ Bioloop roles
- Must use consistent mapping:
  ```
  cmg.god → bioloop.admin
  cmg.admin → bioloop.operator
  cmg.user → bioloop.user
  cmg.guest → bioloop.user
  ```

**Genomic Attributes:**
- CMG has inconsistent field names (`genome_type`, `genomeType`)
- Must check multiple variants when extracting

---

## Implications for New Sync Process

### 1. Run from API Container

**User Requirement:** New process runs in Docker container

**Best Approach:** API container
- Already has Node.js environment
- Prisma client available
- Database access configured
- Can import existing services

**Implementation:**
```bash
# From api container
node src/scripts/sync_cmg_to_bioloop.js
```

**Alternative:** Separate sync container (more complex, probably unnecessary)

### 2. Script Structure

**Recommended Organization:**

```
api/src/scripts/
  ├── sync_cmg_to_bioloop.js         # Main entry point
  ├── sync/
  │   ├── constants.js                # CMD_LINE_PROGRAMS, CONVERSION_DEFINITIONS, etc.
  │   ├── connections.js              # MongoDB & PostgreSQL setup
  │   ├── seeding.js                  # One-time seeding (roles, programs, etc.)
  │   ├── sync_users.js               # User sync logic
  │   ├── sync_datasets.js            # Dataset sync logic
  │   ├── sync_projects.js            # Project sync logic
  │   ├── sync_conversions.js         # Conversion sync logic
  │   ├── sync_sessions.js            # Session sync logic
  │   ├── sync_audit_logs.js          # Audit log sync logic
  │   ├── sync_hierarchies.js         # Hierarchy sync logic
  │   └── utils.js                    # Shared utilities (handleDuplicateName, etc.)
```

### 3. Execution Strategy

**Option A: Full Sync (Simple, Safe)**
- Run entire sync process daily
- Use upsert logic (find by cmg_id, create or update)
- Idempotent: can run multiple times safely

**Option B: Incremental Sync (Complex, Efficient)**
- Track last sync timestamp in `sync_status` table
- Query CMG for documents updated since last sync
- Only update changed records

**Recommendation for MVP:** Start with Option A (full sync)
- Simpler to implement
- Easier to debug
- Good enough for 200 users
- Can optimize later if needed

### 4. Transaction Strategy

**Service Method Pattern:**

```javascript
// api/src/services/sync/users.js
async function syncUser(cmgUser, tx = prisma) {
  const bioloopUser = await tx.user.findFirst({
    where: { cmg_id: cmgUser._id.toString() }
  });
  
  if (bioloopUser) {
    // Update existing
    return tx.user.update({
      where: { id: bioloopUser.id },
      data: { /* updatable fields */ }
    });
  } else {
    // Create new
    return tx.user.create({
      data: { /* all fields including cmg_id */ }
    });
  }
}

// In main sync script
await prisma.$transaction(async (tx) => {
  for (const cmgUser of cmgUsers) {
    await syncUser(cmgUser, tx);
  }
});
```

**Key Points:**
- Service methods accept optional `tx` parameter
- Default to `prisma` if no transaction provided
- Main script wraps operations in transactions
- Enables rollback on failure

### 5. Event Detection Strategy

**Current Approach (Good Enough for MVP):**
- Run full sync daily
- Compare CMG dataset state with Bioloop dataset state
- Update Bioloop to match CMG

**Future Enhancement:**
- Use `sync_event_log` table
- Implement MongoDB change streams (requires MongoDB 3.6+, you have 4.0.28 ✅)
- Or poll CMG API for recent changes

**For Now:**
- Focus on state replication, not event detection
- Daily sync is acceptable (user said "can live with delays up to a few minutes")

---

## What Was NOT Analyzed

**Intentionally Deferred:**

1. **Dataset File Population**
   - User said: "for now, don't worry about periodically/on-demand populating dataset_file objects"
   - Current migration also skips this (commented out)
   - Can be implemented separately later

2. **Workflow Migration**
   - User said: "for now, don't worry about migrating cmg's workflow collection's info into bioloop"
   - Current migration also skips this (commented out)
   - Not necessary for Bioloop functionality

3. **CMG Workflow System**
   - CMG has its own Python-based workflow polling system
   - Independent from Bioloop's Rhythm-based system
   - No need to sync or integrate

---

## Recommended Next Steps

### Phase 1: Initial Setup (Before Implementation)

1. **Review Existing Documentation**
   - ✅ `CURRENT_MIGRATION_PROCESS_ANALYSIS.md` (this analysis)
   - ✅ `BIOLOOP_WORKFLOW_ARCHITECTURE.md` (this analysis)
   - ✅ `CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md` (earlier strategy doc)

2. **Verify Understanding**
   - Confirm approach with user
   - Clarify any ambiguities
   - Get approval to proceed

### Phase 2: Implementation

1. **Create Sync Infrastructure**
   - Set up MongoDB connection in API container
   - Create sync script structure
   - Add constants (CMD_LINE_PROGRAMS, etc.)

2. **Implement One-Time Seeding**
   - Roles
   - Command-line programs
   - Conversion definitions
   - Arguments

3. **Implement Sync Functions (In Order)**
   - Users (with role mapping)
   - Datasets (RAW_DATA + DATA_PRODUCT)
   - Audit logs (from events arrays)
   - Hierarchies
   - Projects (with group expansion)
   - Conversions
   - Sessions

4. **Add Orchestration**
   - Main sync script
   - Transaction wrapping
   - Error handling & logging
   - Dry-run mode for testing

5. **Testing**
   - Test with small subset of CMG data
   - Verify idempotency (can run multiple times)
   - Check foreign key relationships
   - Validate duplicate handling

6. **Scheduling**
   - Add cron job or scheduled task
   - Run daily at off-peak hours
   - Set up monitoring/alerting

### Phase 3: Enhancement (Future)

1. **Incremental Sync**
   - Add `sync_status` tracking
   - Query only changed documents
   - Reduce sync time

2. **Event Detection**
   - MongoDB change streams
   - Or CMG API polling
   - Populate `sync_event_log` table

3. **Dataset Files**
   - Gradual population process
   - Or on-demand when dataset staged

4. **Monitoring**
   - Sync success/failure tracking
   - Data quality checks
   - Drift detection

---

## Technical Considerations

### 1. MongoDB Driver for Node.js

**Recommendation:** Use official MongoDB Node.js driver

```bash
npm install mongodb
```

**Connection Example:**

```javascript
const { MongoClient } = require('mongodb');

const cmgMongoUri = `mongodb://${username}:${password}@${host}:${port}/${database}?authSource=${authSource}`;
const cmgClient = new MongoClient(cmgMongoUri);

await cmgClient.connect();
const cmgDb = cmgClient.db('cmg_database');

// Query collections
const cmgUsers = await cmgDb.collection('users').find({}).toArray();
```

**Configuration:** Add to `api/config/default.json`

```json
{
  "cmg_mongodb": {
    "host": "localhost",
    "port": 27017,
    "database": "cmg",
    "authSource": "admin",
    "username": "",
    "password": ""
  }
}
```

### 2. ObjectId Handling

**CMG MongoDB uses ObjectId:**

```javascript
const { ObjectId } = require('mongodb');

// CMG document
const cmgUser = {
  _id: ObjectId("507f1f77bcf86cd799439011"),
  username: "jdoe"
};

// Convert to string for Bioloop
const cmgId = cmgUser._id.toString();
// Result: "507f1f77bcf86cd799439011"

// Store in Bioloop
await prisma.user.create({
  data: {
    cmg_id: cmgId,
    username: cmgUser.username
  }
});
```

**Later lookup:**

```javascript
const bioloopUser = await prisma.user.findFirst({
  where: { cmg_id: cmgId }
});
```

### 3. Prisma Transactions

**Nested Transactions:**

```javascript
await prisma.$transaction(async (tx) => {
  // Sync users
  for (const cmgUser of cmgUsers) {
    await syncUser(cmgUser, tx);
  }
  
  // Sync datasets (depends on users for ownership)
  for (const cmgDataset of cmgDatasets) {
    await syncDataset(cmgDataset, tx);
  }
});
```

**Transaction Timeout:**

```javascript
await prisma.$transaction(async (tx) => {
  // ... operations ...
}, {
  maxWait: 30000, // 30 seconds max wait to start
  timeout: 600000, // 10 minutes max execution time
});
```

### 4. Error Handling

**Pattern:**

```javascript
try {
  await prisma.$transaction(async (tx) => {
    // Sync operations
  });
  logger.info('Sync completed successfully');
} catch (error) {
  logger.error('Sync failed:', error);
  // Send alert
  // Don't throw (let process exit gracefully)
} finally {
  await cmgClient.close();
  await prisma.$disconnect();
}
```

### 5. Logging

**Use Bioloop Logger:**

```javascript
const logger = require('@/services/logger');

logger.info('[SYNC] Starting CMG to Bioloop sync');
logger.info(`[SYNC] Found ${cmgUsers.length} CMG users`);
logger.warn(`[SYNC] Duplicate dataset name: ${name}`);
logger.error('[SYNC] Failed to sync user:', error);
```

---

## Docker Considerations

### Running Sync from API Container

**Option 1: Manual Execution**

```bash
# From host
docker exec -it bioloop-api node src/scripts/sync_cmg_to_bioloop.js

# Or enter container
docker exec -it bioloop-api bash
node src/scripts/sync_cmg_to_bioloop.js
```

**Option 2: Scheduled Execution**

Add to `api/ecosystem.config.js` (PM2):

```javascript
module.exports = {
  apps: [
    {
      name: 'api',
      script: './src/index.js',
      // ... existing config ...
    },
    {
      name: 'sync',
      script: './src/scripts/sync_cmg_to_bioloop.js',
      cron_restart: '0 2 * * *',  // Daily at 2 AM
      autorestart: false,
      watch: false,
    },
  ],
};
```

**Option 3: Cron Job in Container**

Add to `api/Dockerfile`:

```dockerfile
# Install cron
RUN apt-get update && apt-get install -y cron

# Add crontab
COPY crontab /etc/cron.d/sync-cron
RUN chmod 0644 /etc/cron.d/sync-cron
RUN crontab /etc/cron.d/sync-cron
```

Create `api/crontab`:

```
0 2 * * * cd /opt/sca/app && node src/scripts/sync_cmg_to_bioloop.js >> /var/log/sync.log 2>&1
```

**Recommendation:** Option 2 (PM2 cron) is simplest and most maintainable

---

## Success Criteria

### Minimal Viable Sync (MVP)

**Must Have:**
- ✅ Seed roles, programs, conversion definitions (one-time)
- ✅ Sync users with role mapping
- ✅ Sync datasets (RAW_DATA + DATA_PRODUCT)
- ✅ Sync projects with user/dataset associations
- ✅ Sync conversions with derived datasets
- ✅ Sync audit logs from events arrays
- ✅ Sync dataset hierarchies
- ✅ Handle duplicate names
- ✅ Expand groups to users
- ✅ Idempotent (can run multiple times)
- ✅ Transaction-safe (rollback on error)

**Nice to Have (Future):**
- Incremental sync (only changed records)
- Event detection (change streams or polling)
- Dataset file population
- Workflow migration
- Data quality checks
- Drift detection
- Monitoring dashboard

### Testing Checklist

**Before Production:**
- [ ] Test with small CMG dataset subset
- [ ] Verify all foreign keys valid
- [ ] Check duplicate name handling
- [ ] Verify role mapping correct
- [ ] Confirm group expansion works
- [ ] Test idempotency (run twice, no duplicates)
- [ ] Test transaction rollback on error
- [ ] Verify audit logs populated correctly
- [ ] Check dataset hierarchy links
- [ ] Confirm conversion associations correct

**Performance:**
- [ ] Sync completes within acceptable time window
- [ ] Database load acceptable
- [ ] Memory usage reasonable
- [ ] No connection leaks

---

## Summary

**What We Know:**
- Current Python migration process dependencies and patterns
- How Bioloop uses workflows (and that we don't need to sync them)
- Which tables to sync vs. which to seed once
- How to handle CMG-specific quirks (duplicates, groups, roles)

**What We'll Build:**
- JavaScript-based sync process running in API container
- Idempotent, transaction-safe, full-sync approach
- Focus on state replication, not workflow migration
- Good enough for 200 users, can optimize later

**What We're Deferring:**
- Dataset file population
- Workflow migration
- Incremental sync optimizations
- Real-time event detection

**Ready for:** Implementation planning and execution

---

**Related Documents:**
- `CURRENT_MIGRATION_PROCESS_ANALYSIS.md` - Detailed analysis of Python migration process
- `BIOLOOP_WORKFLOW_ARCHITECTURE.md` - Complete guide to workflow system
- `CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md` - Earlier strategy document
- `CMG_BIOLOOP_SYNC_STRATEGY_SUMMARY.md` - High-level strategy summary


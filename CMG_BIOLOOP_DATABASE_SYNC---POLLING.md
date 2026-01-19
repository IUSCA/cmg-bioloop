# CMG-Bioloop Database Synchronization: Polling Strategy
**Date:** 2026-01-04  
**Implementation Approach:** Multi-Poller Cursor-Based Incremental Sync  
**Status:** Implementation Ready

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Two-Phase Implementation](#two-phase-implementation)
4. [Poller Specifications](#poller-specifications)
5. [Schema Changes](#schema-changes)
6. [Cursor & Locking Mechanism](#cursor--locking-mechanism)
7. [Error Handling & Logging](#error-handling--logging)
8. [Deployment Strategy](#deployment-strategy)
9. [Testing & Validation](#testing--validation)
10. [Operational Considerations](#operational-considerations)

---

## Executive Summary

### The Challenge

Bioloop needs to stay synchronized with CMG's MongoDB database while:
- Avoiding daily ID changes (prevents token invalidation, data corruption)
- Detecting changes quickly (near real-time for access control)
- Handling large datasets efficiently (hundreds of TB)
- Supporting 200+ users with low traffic
- Running within existing Docker infrastructure

### The Solution

**Two-Script Approach:**

1. **Big-Bang Script** (`cmg_bigbang_sync.js`)
   - One-time initial population
   - Seeds constants (roles, programs, conversion definitions)
   - Migrates all existing CMG data to Bioloop
   - Respects dependency order

2. **Poller Script** (`cmg_poller_sync.js`)
   - Continuous incremental sync after big-bang
   - 5 concurrent pollers running in single Node process
   - Cursor-based: tracks `(updatedAt, _id)` per poller
   - Each poller has independent lock and cursor
   - PM2-managed in API container
   - **Note:** Workflow Status Poller connects to Rhythm MongoDB (not CMG)

### Key Benefits

✅ **No Missed Updates** - Bounded window cursor with tie-breaking  
✅ **No ID Changes** - `cmg_id` enables idempotent upserts  
✅ **Near Real-Time** - Pollers run every 10-15 seconds  
✅ **Transaction Safe** - All-or-nothing batch commits  
✅ **Production Ready** - Locks prevent concurrent runs  
✅ **Week-1 Robust** - Comprehensive error logging  

---

## Architecture Overview

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         CMG MongoDB                             │
│  Collections: users, datasets, dataproducts, projects, groups  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ 4 pollers query with cursor
                         │
┌────────────────────────▼────────────────────────────────────────┐
│              Bioloop API Container (Node.js)                    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │         cmg_poller_sync.js (PM2-managed)                 │  │
│  │                                                           │  │
│  │  ┌─────────────────┐  ┌─────────────────┐               │  │
│  │  │ User Roles      │  │ Project ACL     │               │  │
│  │  │ Poller (10s)    │  │ Poller (10s)    │               │  │
│  │  └─────────────────┘  └─────────────────┘               │  │
│  │                                                           │  │
│  │  ┌─────────────────┐  ┌─────────────────┐               │  │
│  │  │ Dataset Activity│  │ Dataset Metadata│               │  │
│  │  │ Poller (10-15s) │  │ Poller (10-15s) │               │  │
│  │  └─────────────────┘  └─────────────────┘               │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────┐                │  │
│  │  │ Workflow Status Poller (10s)        │                │  │
│  │  │ (connects to Rhythm MongoDB)        │                │  │
│  │  └─────────────────────────────────────┘                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                       │
│                         │ Prisma transactions                   │
│                         ▼                                       │
└─────────────────────────────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                  Bioloop PostgreSQL                             │
│  Tables: user, dataset, project, conversion, dataset_state      │
│  Sync Tables: cmg_sync_cursor, cmg_sync_retry                   │
└─────────────────────────────────────────────────────────────────┘
         ▲
         │ 1 poller queries Rhythm workflows
         │
┌────────┴────────────────────────────────────────────────────────┐
│                     Rhythm MongoDB                              │
│  Collections: workflow_meta, celery_taskmeta                    │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Per Poller

```
1. Acquire Lock
   ├─ Read cmg_sync_cursor for this poller
   ├─ Check if locked_by is null or lock expired
   └─ Set locked_by = INSTANCE_ID, lock_expires_at = now() + 60s

2. Fetch Batch from CMG
   ├─ Query: updatedAt > cursor.last_updated_at
   ├─ Bounded window: updatedAt <= round_end
   ├─ Tie-break: _id > cursor.last_cmg_objectid
   └─ Limit: BATCH_SIZE (e.g., 200)

3. Apply Batch (Transaction)
   ├─ For each CMG doc:
   │  ├─ Upsert Bioloop row (by cmg_id)
   │  └─ Handle relationships
   ├─ Update cursor to last doc in batch
   └─ Commit or rollback

4. Release Lock
   ├─ Set locked_by = null
   ├─ Update last_succeeded_at or last_failed_at
   └─ Log metrics

5. Wait & Repeat
   └─ Sleep for poll_interval_ms, then loop
```

---

## Two-Phase Implementation

### Phase 1: Big-Bang (One-Time Initial Population)

**Script:** `api/src/scripts/cmg_bigbang_sync.js`

**Purpose:** Migrate all existing CMG data to empty/reset Bioloop database

**Execution Order:**

```javascript
1. Seed Constants (One-Time)
   ├─ roles (user, operator, admin)
   ├─ cmd_line_programs (bcl2fastq, cellranger, etc.)
   ├─ conversion_definitions (map to programs)
   └─ arguments (link to programs)

2. Sync Users
   ├─ Insert users (with cmg_id)
   ├─ Map CMG roles → Bioloop roles
   └─ Create user_role associations

3. Sync Datasets
   ├─ CMG datasets → dataset (type=RAW_DATA)
   ├─ CMG dataproducts → dataset (type=DATA_PRODUCT)
   ├─ Insert dataset_genomic_attributes
   ├─ Parse events → create dataset_state rows
   └─ Handle duplicate names (DUPLICATE_ prefix)

4. Sync Dataset Hierarchies
   ├─ dataproduct.dataset → source_id
   └─ Create dataset_hierarchy links

5. Sync Projects
   ├─ Insert projects (with cmg_id)
   ├─ Expand groups → users
   ├─ Create project_user associations
   └─ Create project_dataset associations

6. Sync Conversions
   ├─ Insert conversions (with cmg_id)
   ├─ Map pipeline → conversion_definition
   └─ Create conversion_derived_dataset links

7. Initialize Cursors
   └─ Set initial cursor values for each poller
```

**Characteristics:**
- ✅ Full sweep of all CMG collections
- ✅ No cursor tracking during execution
- ✅ Strict dependency order
- ✅ Transaction-safe (rollback on error)
- ⏱️ Runs once (estimated 5-30 minutes depending on data size)

---

### Phase 2: Continuous Polling (Incremental Updates)

**Script:** `api/src/scripts/cmg_poller_sync.js`

**Purpose:** Keep Bioloop synchronized with CMG changes

**Pollers:** 4 concurrent pollers in single process

**Characteristics:**
- ✅ Cursor-based incremental updates
- ✅ Independent lock per poller
- ✅ No strict order (pollers are independent)
- ✅ Idempotent (safe to run multiple times)
- ⏱️ Runs continuously (PM2-managed)

---

## Poller Specifications

### 1. User Roles Poller

**Name:** `user_roles`  
**Interval:** 10 seconds  
**CMG Source:** `users` collection  
**Purpose:** Sync user role changes for authorization

#### What It Watches

```javascript
// CMG users document
{
  _id: ObjectId("..."),
  username: "jdoe",
  roles: ["admin", "user"],  // ← Watches this array
  active: true,
  updatedAt: ISODate("...")
}
```

#### What It Updates

**Bioloop Tables:**
- `user` (metadata only, NOT identity fields)
- `user_role` (diff-based add/remove)

**Update Logic:**

```javascript
// 1. Find user by cmg_id
const bioloopUser = await tx.user.findFirst({
  where: { cmg_id: cmgUser._id.toString() }
});

// 2. Update metadata only (NOT username, name, email, cas_id)
await tx.user.update({
  where: { id: bioloopUser.id },
  data: {
    is_deleted: !cmgUser.active,
    metadata: {
      ...existingMetadata,
      cmg_sync_state: {
        cmg_updated_at: cmgUser.updatedAt,
        last_sync_time: new Date()
      }
    }
  }
});

// 3. Sync roles (diff-based)
const cmgRoles = mapCMGRolesToBioloop(cmgUser.roles);
const existingRoles = await tx.user_role.findMany({
  where: { user_id: bioloopUser.id },
  include: { role: true }
});

// Add missing roles
for (const roleName of cmgRoles) {
  const role = await tx.role.findUnique({ where: { name: roleName } });
  if (!existingRoles.some(ur => ur.role_id === role.id)) {
    await tx.user_role.create({
      data: { user_id: bioloopUser.id, role_id: role.id }
    });
  }
}

// Remove extra roles
for (const ur of existingRoles) {
  if (!cmgRoles.includes(ur.role.name)) {
    await tx.user_role.delete({ where: { id: ur.id } });
  }
}
```

**Immutable Fields (Never Update After Big-Bang):**
- `username`
- `name`
- `email`
- `cas_id`

**Why:** Users may have edited these in Bioloop; don't overwrite

---

### 2. Project ACL Poller

**Name:** `project_acl`  
**Interval:** 10 seconds  
**CMG Sources:** `projects` + `groups` collections  
**Purpose:** Near real-time access control updates

#### What It Watches

```javascript
// CMG projects document
{
  _id: ObjectId("..."),
  name: "My Project",
  users: [ObjectId("..."), ObjectId("...")],      // ← Direct users
  groups: [ObjectId("..."), ObjectId("...")],     // ← Groups (expand to users)
  dataproducts: [ObjectId("..."), ObjectId("...")], // ← Dataset associations
  updatedAt: ISODate("...")
}

// CMG groups document (queried on-demand)
{
  _id: ObjectId("..."),
  name: "Lab Group",
  users: [ObjectId("..."), ObjectId("...")],  // ← Members
  members: [ObjectId("...")]  // ← Alternative field name
}
```

#### What It Updates

**Bioloop Tables:**
- `project` (metadata only, NOT name/slug)
- `project_user` (rebuild per project)
- `project_dataset` (rebuild per project)

**Update Logic:**

```javascript
// 1. Upsert project metadata
await tx.project.update({
  where: { cmg_id: cmgProject._id.toString() },
  data: {
    description: cmgProject.description,
    browser_enabled: cmgProject.browser,
    metadata: {
      ...existing,
      cmg_sync_state: {
        cmg_updated_at: cmgProject.updatedAt,
        last_sync_time: new Date()
      }
    }
  }
});

// 2. Expand groups to users
const directUserIds = cmgProject.users || [];
const groupUserIds = [];
for (const groupId of cmgProject.groups || []) {
  const group = await cmgDb.collection('groups').findOne({
    _id: new ObjectId(groupId)
  });
  if (group) {
    groupUserIds.push(...(group.users || group.members || []));
  }
}
const allUserIds = [...new Set([...directUserIds, ...groupUserIds])];

// 3. Rebuild project_user (delete + insert)
await tx.project_user.deleteMany({
  where: { project_id: bioloopProject.id }
});
for (const cmgUserId of allUserIds) {
  const user = await tx.user.findFirst({
    where: { cmg_id: cmgUserId.toString() }
  });
  if (user) {
    await tx.project_user.create({
      data: { project_id: bioloopProject.id, user_id: user.id }
    });
  }
}

// 4. Rebuild project_dataset (delete + insert)
await tx.project_dataset.deleteMany({
  where: { project_id: bioloopProject.id }
});
for (const cmgDataproductId of cmgProject.dataproducts || []) {
  const dataset = await tx.dataset.findFirst({
    where: { cmg_id: cmgDataproductId.toString() }
  });
  if (dataset) {
    await tx.project_dataset.create({
      data: { project_id: bioloopProject.id, dataset_id: dataset.id }
    });
  }
}
```

**Immutable Fields:**
- `name` (could break references if changed)
- `slug` (changing breaks URLs)

---

### 3. Dataset Activity Poller

**Name:** `dataset_activity`  
**Interval:** 10-15 seconds  
**CMG Sources:** `datasets` + `dataproducts` collections  
**Purpose:** Track dataset path changes and lifecycle flags

#### What It Watches

```javascript
// CMG datasets/dataproducts document
{
  _id: ObjectId("..."),
  name: "Dataset1",
  staged: true,           // ← Boolean flags
  archived: true,
  validated: false,
  paths: {
    origin: "/path/to/origin",
    archive: "/path/to/archive.tar",
    staged: "/path/to/staged"  // ← Path changes
  },
  updatedAt: ISODate("...")
}
```

#### What It Updates

**Bioloop Tables:**
- `dataset` (paths, flags)

**Update Logic:**

```javascript
// Update dataset paths and flags only
await tx.dataset.update({
  where: { cmg_id: cmgDataset._id.toString() },
  data: {
    archive_path: cmgDataset.paths?.archive,
    staged_path: cmgDataset.paths?.staged,
    is_staged: cmgDataset.staged || false,
    metadata: {
      ...existing,
      cmg_sync_state: {
        cmg_updated_at: cmgDataset.updatedAt,
        last_sync_time: new Date()
      }
    }
  }
});
```

**Exclusions:**
- ❌ Don't create `dataset_audit` logs (skip for now)
- ❌ Don't populate `dataset_file` table
- ❌ Don't parse events arrays (delegated to Workflow Status Poller)

**Note:** Dataset states are now handled by the Workflow Status Poller (see below)

---

### 4. Workflow Status Poller

**Name:** `workflow_status`  
**Interval:** 10 seconds  
**Source:** Rhythm MongoDB `workflow_meta` collection  
**Purpose:** Assign dataset states based on completed workflows

#### What It Watches

```javascript
// Rhythm workflow_meta document
{
  _id: ObjectId("..."),
  name: "integrated",     // ← Workflow type
  _status: "SUCCESS",     // ← Completion status
  app_id: "cmg",
  args: [123],            // ← dataset_id in args[0]
  updated_at: ISODate("...")
}
```

#### Workflow State Mapping

| Workflow Name | Bioloop States Assigned        | Notes                           |
|---------------|--------------------------------|---------------------------------|
| `integrated`  | `INSPECTED`, `ARCHIVED`, `STAGED` | All states when complete     |
| `stage`       | `STAGED`                       | Single state on completion      |
| `conversion`  | N/A                            | No states assigned              |
| `delete`      | N/A                            | No states assigned              |

**Only `integrated` and `stage` workflows assign states to datasets.**

#### What It Updates

**Bioloop Tables:**
- `dataset_state` (add state rows based on workflow completion)

**Update Logic:**

```javascript
// 1. Get dataset_id from workflow args
const datasetId = workflow.args[0];

// 2. Get states to assign based on workflow name
const stateConfig = WORKFLOW_STATE_MAP[workflow.name];

// 3. Assign all states for this workflow type
for (const stateName of stateConfig.states) {
  // Check if state already exists
  const existing = await tx.dataset_state.findFirst({
    where: {
      dataset_id: datasetId,
      state: stateName
    }
  });
  
  if (!existing) {
    await tx.dataset_state.create({
      data: {
        dataset_id: datasetId,
        state: stateName,
        timestamp: workflow.updated_at || new Date()
      }
    });
  }
}
```

**Key Points:**
- Only polls **completed** workflows (`_status: "SUCCESS"`)
- Uses `updated_at` from Rhythm workflow_meta for cursor
- Workflow links to dataset via `args[0]` field
- Application-level uniqueness check prevents duplicate states
- Other workflow types (conversion, delete) are ignored

---

### 5. Dataset Metadata Poller

**Name:** `dataset_metadata`  
**Interval:** 10-15 seconds  
**CMG Sources:** `datasets` + `dataproducts` collections  
**Purpose:** Sync dataset descriptive metadata

#### What It Watches

```javascript
// CMG datasets/dataproducts document
{
  _id: ObjectId("..."),
  name: "Dataset1",
  description: "Updated description",  // ← Metadata
  size: 123456789,                     // ← Size changes
  du_size: 987654321,
  files: 1234,                         // ← Count changes
  directories: 56,
  file_type: "FASTQ",                  // ← File type (dataproducts)
  updatedAt: ISODate("...")
}
```

#### What It Updates

**Bioloop Tables:**
- `dataset` (metadata fields only)

**Update Logic:**

```javascript
await tx.dataset.update({
  where: { cmg_id: cmgDataset._id.toString() },
  data: {
    description: cmgDataset.description,
    size: cmgDataset.size ? BigInt(cmgDataset.size) : null,
    du_size: cmgDataset.du_size ? BigInt(cmgDataset.du_size) : null,
    num_files: cmgDataset.files || 0,
    num_directories: cmgDataset.directories || 0,
    file_type: cmgDataset.file_type || null,  // dataproducts only
    metadata: {
      ...existing,
      cmg_sync_state: {
        cmg_updated_at: cmgDataset.updatedAt,
        last_sync_time: new Date()
      }
    }
  }
});
```

**Immutable Fields:**
- `name` (duplicate handling done in big-bang only)
- `type` (RAW_DATA vs DATA_PRODUCT never changes)

---

## Schema Changes

### New Tables

#### cmg_sync_cursor

**Purpose:** Track cursor position and lock state per poller

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
```

**Initial Rows (Created by Big-Bang):**
- `user_roles`
- `project_acl`
- `dataset_activity`
- `dataset_metadata`

#### cmg_sync_retry

**Purpose:** Track poison documents that repeatedly fail

```prisma
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

---

### Modified Tables

**Ensure `cmg_id` fields exist and are unique:**

```prisma
model user {
  // ... existing fields
  cmg_id String? @unique
}

model dataset {
  // ... existing fields
  cmg_id String? @unique
}

model project {
  // ... existing fields
  cmg_id String? @unique
}

model conversion {
  // ... existing fields
  cmg_id String? @unique
}
```

**No other constraints added** - keep existing schema as-is

---

## Cursor & Locking Mechanism

### Cursor Tuple

Each poller maintains:
```javascript
{
  last_updated_at: DateTime,     // Last processed CMG updatedAt
  last_cmg_objectid: String      // Last processed CMG _id (ObjectId as string)
}
```

### Query Pattern

```javascript
const cursor = await prisma.cmg_sync_cursor.findUnique({
  where: { poller_name: 'dataset_activity' }
});

const roundEnd = new Date(); // Bounded window

const query = {
  $or: [
    { updatedAt: { $gt: cursor.last_updated_at } },
    {
      updatedAt: cursor.last_updated_at,
      _id: { $gt: new ObjectId(cursor.last_cmg_objectid) }
    }
  ],
  updatedAt: { $lte: roundEnd }
};

const docs = await cmgCollection
  .find(query)
  .sort({ updatedAt: 1, _id: 1 })
  .limit(BATCH_SIZE)
  .toArray();
```

**Guarantees:**
- ✅ No missed updates (ordered by updatedAt, tie-break by _id)
- ✅ Deterministic progression
- ✅ Safe cutoff (bounded window)

### Lock Acquisition

```javascript
async function acquireLock(pollerName, instanceId, lockTtlMs = 60000) {
  return await prisma.$transaction(async (tx) => {
    const cursor = await tx.cmg_sync_cursor.findUnique({
      where: { poller_name: pollerName }
    });
    
    const now = new Date();
    const canAcquire = !cursor.locked_by || 
                       (cursor.lock_expires_at && cursor.lock_expires_at < now);
    
    if (!canAcquire) {
      return null; // Skip this tick
    }
    
    return await tx.cmg_sync_cursor.update({
      where: { poller_name: pollerName },
      data: {
        locked_by: instanceId,
        lock_expires_at: new Date(now.getTime() + lockTtlMs),
        last_started_at: now
      }
    });
  });
}
```

### Lock Release

```javascript
async function releaseLock(pollerName, success, error = null, processedCount = 0) {
  await prisma.cmg_sync_cursor.update({
    where: { poller_name: pollerName },
    data: {
      locked_by: null,
      lock_expires_at: null,
      ...(success ? {
        last_succeeded_at: new Date(),
        last_run_count: processedCount
      } : {
        last_failed_at: new Date(),
        last_error: error?.message?.substring(0, 500)
      })
    }
  });
}
```

---

## Error Handling & Logging

### Comprehensive Error Logging

**Log File:** `/opt/sca/data/logs/cmg_sync_errors.jsonl` (JSON Lines format)

**Log Entry Structure:**

```javascript
{
  timestamp: "2026-01-04T10:30:45.123Z",
  poller: "dataset_activity",
  operation: "upsert",
  cmg_collection: "datasets",
  cmg_id: "507f1f77bcf86cd799439011",
  error: {
    message: "Unique constraint failed on fields: (name,type,is_deleted)",
    code: "P2002",
    meta: {
      target: ["name", "type", "is_deleted"]
    },
    stack: "Error: ...\n at ..."
  },
  prisma_query: {
    model: "dataset",
    operation: "upsert",
    where: { cmg_id: "507f1f77bcf86cd799439011" },
    create: { /* FULL create object */ },
    update: { /* FULL update object */ },
    include: { /* ALL includes */ }
  },
  cmg_document: { /* ENTIRE CMG document */ }
}
```

### Error Handling Strategy

```javascript
try {
  await prisma.$transaction(async (tx) => {
    for (const cmgDoc of batch) {
      try {
        await syncEntity(cmgDoc, tx);
      } catch (docError) {
        // Log individual document error
        await logSyncError({
          poller: pollerName,
          cmgDoc,
          error: docError,
          prismaQuery: { /* capture attempted query */ }
        });
        
        // Track in retry table
        await trackRetry(pollerName, cmgDoc._id.toString(), docError);
        
        // Continue with rest of batch (don't throw)
      }
    }
    
    // Update cursor even if some docs failed
    await updateCursor(tx, pollerName, lastDoc);
  });
} catch (transactionError) {
  // Transaction-level error (rare)
  logger.error(`[${pollerName}] Transaction failed:`, transactionError);
  // Don't update cursor - will retry entire batch next run
}
```

### Retry Tracking

```javascript
async function trackRetry(pollerName, cmgId, error) {
  await prisma.cmg_sync_retry.upsert({
    where: {
      poller_name_cmg_id: {
        poller_name: pollerName,
        cmg_id: cmgId
      }
    },
    create: {
      poller_name: pollerName,
      cmg_id: cmgId,
      failure_count: 1,
      last_error: error.message.substring(0, 500),
      next_retry_at: new Date(Date.now() + 3600000) // 1 hour
    },
    update: {
      failure_count: { increment: 1 },
      last_error: error.message.substring(0, 500),
      next_retry_at: new Date(Date.now() + 3600000)
    }
  });
}
```

---

## Deployment Strategy

### File Structure

```
api/src/scripts/
  ├── cmg_bigbang_sync.js           # One-time initial population
  ├── cmg_poller_sync.js             # Continuous polling (PM2)
  ├── cmg_sync/
  │   ├── connections.js             # MongoDB + Prisma setup
  │   ├── constants.js               # CMD_LINE_PROGRAMS, etc.
  │   ├── error_logger.js            # JSONL error logging
  │   ├── cursor_manager.js          # Lock acquire/release/update
  │   ├── pollers/
  │   │   ├── base_poller.js         # Base poller class
  │   │   ├── user_roles_poller.js
  │   │   ├── project_acl_poller.js
  │   │   ├── dataset_activity_poller.js
  │   │   └── dataset_metadata_poller.js
  │   ├── bigbang/
  │   │   ├── seed_constants.js
  │   │   ├── sync_users.js
  │   │   ├── sync_datasets.js
  │   │   ├── sync_projects.js
  │   │   ├── sync_conversions.js
  │   │   └── initialize_cursors.js
  │   └── utils/
  │       ├── cmg_helpers.js
  │       ├── event_parser.js        # Parse CMG events
  │       ├── state_mapper.js        # Event → State mapping
  │       ├── role_mapper.js         # CMG roles → Bioloop roles
  │       └── duplicate_handler.js   # Handle duplicate names
```

### PM2 Configuration

**File:** `api/ecosystem.config.js`

```javascript
module.exports = {
  apps: [
    {
      name: 'api',
      script: './src/index.js',
      // ... existing config
    },
    {
      name: 'cmg-poller',
      script: './src/scripts/cmg_poller_sync.js',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        INSTANCE_ID: require('os').hostname(),
      },
      error_file: '/opt/sca/data/logs/cmg_poller_error.log',
      out_file: '/opt/sca/data/logs/cmg_poller_out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    },
  ],
};
```

### Execution Steps

**Step 1: Run Big-Bang (Once)**

```bash
# From API container
docker exec -it bioloop-api bash
cd /opt/sca/app
node src/scripts/cmg_bigbang_sync.js
```

**Step 2: Start Poller (PM2)**

```bash
# Restart PM2 with new config
pm2 restart ecosystem.config.js
pm2 logs cmg-poller  # Watch logs
```

**Step 3: Monitor**

```bash
# Check poller health
pm2 status

# View logs
tail -f /opt/sca/data/logs/cmg_poller_out.log
tail -f /opt/sca/data/logs/cmg_sync_errors.jsonl

# Query cursor status
psql -d bioloop -c "SELECT * FROM cmg_sync_cursor;"
```

---

## Testing & Validation

### Pre-Production Checklist

**1. Schema Migration**
```bash
cd api
npx prisma migrate dev --name add_cmg_sync_tables
npx prisma generate
```

**2. Big-Bang Test (Dry Run)**
```bash
# Test with subset of CMG data
BATCH_SIZE=10 DRY_RUN=true node src/scripts/cmg_bigbang_sync.js
```

**3. Verify Data Integrity**
```sql
-- Check user count matches
SELECT COUNT(*) FROM "user" WHERE cmg_id IS NOT NULL;

-- Check dataset count matches
SELECT COUNT(*) FROM dataset WHERE cmg_id IS NOT NULL;

-- Check project associations
SELECT p.name, COUNT(pu.user_id) as user_count, COUNT(pd.dataset_id) as dataset_count
FROM project p
LEFT JOIN project_user pu ON p.id = pu.project_id
LEFT JOIN project_dataset pd ON p.id = pd.project_id
GROUP BY p.id, p.name;

-- Check dataset states
SELECT d.name, ds.state, ds.timestamp
FROM dataset d
JOIN dataset_state ds ON d.id = ds.dataset_id
WHERE d.cmg_id IS NOT NULL
ORDER BY d.name, ds.timestamp;
```

**4. Poller Test**
```bash
# Run poller in test mode (single iteration)
TEST_MODE=true node src/scripts/cmg_poller_sync.js
```

**5. Verify Idempotency**
```bash
# Run big-bang twice - should not create duplicates
node src/scripts/cmg_bigbang_sync.js
node src/scripts/cmg_bigbang_sync.js

# Verify counts unchanged
```

---

## Operational Considerations

### Performance Tuning

**Batch Sizes:**
- User roles: 200 docs/batch
- Project ACL: 100 docs/batch (expensive - rebuilds relationships)
- Dataset activity: 200 docs/batch
- Dataset metadata: 200 docs/batch

**Poll Intervals:**
- User roles: 10s (critical for auth)
- Project ACL: 10s (critical for access)
- Dataset activity: 10-15s (important for UI state)
- Dataset metadata: 10-15s (less critical)

**Lock TTL:**
- Default: 60 seconds
- Adjust based on average batch processing time

### Monitoring Metrics

**Key Metrics to Track:**
- Cursor lag (updatedAt delta between CMG and cursor)
- Batch processing time per poller
- Error rate per poller
- Lock contention (if running multi-instance)
- Retry queue size

**Dashboard Queries:**

```sql
-- Poller health
SELECT 
  poller_name,
  last_succeeded_at,
  last_failed_at,
  last_run_count,
  EXTRACT(EPOCH FROM (NOW() - last_updated_at)) / 60 as minutes_behind
FROM cmg_sync_cursor;

-- Error trends
SELECT 
  poller_name,
  COUNT(*) as error_count,
  AVG(failure_count) as avg_failures
FROM cmg_sync_retry
GROUP BY poller_name;
```

### Disaster Recovery

**Scenario: Poller Gets Stuck**

```bash
# Check lock status
psql -c "SELECT * FROM cmg_sync_cursor WHERE locked_by IS NOT NULL;"

# Force release lock
psql -c "UPDATE cmg_sync_cursor SET locked_by = NULL, lock_expires_at = NULL WHERE poller_name = 'dataset_activity';"

# Restart poller
pm2 restart cmg-poller
```

**Scenario: Need to Resync Everything**

```bash
# Stop poller
pm2 stop cmg-poller

# Reset cursors
psql -c "UPDATE cmg_sync_cursor SET last_updated_at = NULL, last_cmg_objectid = NULL;"

# Run big-bang (will update existing records)
node src/scripts/cmg_bigbang_sync.js

# Restart poller
pm2 restart cmg-poller
```

---

## Future Enhancements

### Phase 3: Advanced Features (Later)

1. **Session Sync Poller**
   - Add when genome browser sessions become critical
   - Low priority (sessions don't change frequently)

2. **Dataset File Population**
   - Gradual background process
   - Or on-demand when dataset first accessed

3. **Real-Time Change Streams**
   - MongoDB change streams (requires MongoDB 3.6+, have 4.0.28 ✅)
   - Push-based instead of poll-based
   - Near-instant propagation

4. **Reconciliation Poller**
   - Run every 6-24 hours
   - Re-check last 24h of changes
   - Catch any missed updates due to bugs

5. **Data Quality Checks**
   - Automated validation queries
   - Detect drift between CMG and Bioloop
   - Alert on inconsistencies

---

## Success Criteria

### Week 1 Goals

- ✅ Big-bang successfully migrates all existing CMG data
- ✅ 5 pollers run continuously without crashes
- ✅ User role changes reflected within 30 seconds
- ✅ Project access changes reflected within 30 seconds
- ✅ Dataset state changes reflected within 1 minute (via Workflow Status Poller)
- ✅ No duplicate states per dataset
- ✅ Comprehensive error logging for all failures
- ✅ Idempotent operations (safe to re-run)

### Production Ready

- ✅ Zero data loss (no missed updates)
- ✅ Less than 1% error rate
- ✅ Average cursor lag < 2 minutes
- ✅ PM2 keeps poller running 24/7
- ✅ Clear operational runbook for common issues

---

## Glossary

**Big-Bang:** One-time initial population of all CMG data into Bioloop  
**Poller:** Background process that polls CMG for changes in a specific collection  
**Cursor:** Bookmark tracking last processed document (updatedAt, _id)  
**Lock:** Mechanism preventing concurrent runs of same poller  
**Bounded Window:** Query constraint limiting documents to specific time range  
**Tie-Breaking:** Using _id to order documents with same updatedAt  
**Idempotent:** Operation that produces same result when run multiple times  
**Upsert:** Insert if doesn't exist, update if exists  
**Poison Document:** Document that repeatedly fails processing  

---

**Status:** Ready for implementation  
**Next Steps:** Begin with schema migration and big-bang script structure


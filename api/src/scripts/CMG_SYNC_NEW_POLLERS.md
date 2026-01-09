# CMG Sync - New Pollers Added

## Summary

Added two new metadata pollers to the CMG sync system:
1. **Project Metadata Poller** - Syncs project metadata changes
2. **Session Metadata Poller** - Syncs genome browser session metadata changes

Also removed the **Workflow Status Poller** as requested by the user (will discuss alternative approach later).

---

## 1. Project Metadata Poller

**File:** `api/src/scripts/cmg_sync/pollers/project_metadata_poller.js`

### What It Syncs

Monitors CMG's `projects` collection and syncs the following fields to Bioloop:

- `name` - Project name
- `description` - Project description
- `browser_enabled` - Whether IGV browser is enabled (mapped from CMG's `igv_enabled`)
- `funding` - Funding information
- `metadata` - JSON metadata (preserves existing fields, updates `cmg_sync_state`)

### What It Does NOT Sync

- `slug` - Derived from name, should not be overwritten
- `cmg_id` - Immutable identifier
- Relationships (`users`, `datasets`, `contacts`) - Handled by `project_acl_poller`

### Configuration

- **Poll Interval:** 20 seconds (less frequent than other pollers)
- **Batch Size:** 100 projects per batch
- **Collection:** `projects`

### Example Usage

```javascript
const ProjectMetadataPoller = require('./cmg_sync/pollers/project_metadata_poller');

const poller = new ProjectMetadataPoller(prisma, cmgDb, {
  pollIntervalMs: 20000,
  batchSize: 100
});

poller.start();
```

### CMG → Bioloop Field Mapping

| CMG Field       | Bioloop Field    | Notes                          |
|-----------------|------------------|--------------------------------|
| `name`          | `name`           | Direct mapping                 |
| `description`   | `description`    | Direct mapping, nullable       |
| `igv_enabled`   | `browser_enabled`| Boolean, defaults to false     |
| `funding`       | `funding`        | Direct mapping, nullable       |
| `updatedAt`     | `metadata.cmg_sync_state.cmg_updated_at` | Stored in metadata |

---

## 2. Session Metadata Poller

**File:** `api/src/scripts/cmg_sync/pollers/session_metadata_poller.js`

### What It Syncs

Monitors CMG's `sessions` collection and syncs the following fields to Bioloop:

- `title` - Session title/name
- `access_count` - Number of times session was accessed
- `is_public` - Whether session is publicly accessible
- `staging_requested` - Staging request details (JSON)
- `staging_completed` - Whether staging is complete
- `staging_requested_by` - User who requested staging (resolved to user_id)

### What It Does NOT Sync

- `genome` - Immutable after session creation
- `genome_type` - Immutable after session creation
- Tracks (`session_tracks`) - Handled during big-bang sync only

### Configuration

- **Poll Interval:** 30 seconds (least frequent - sessions change rarely)
- **Batch Size:** 50 sessions per batch
- **Collection:** `sessions`

### User Resolution

The `staging_requested_by` field in CMG may contain either a username or a CMG user ID. The poller handles both:

```javascript
// Look up user by username or cmg_id
const requestingUser = await tx.user.findFirst({
  where: {
    OR: [
      { username: stagingRequestedBy },
      { cmg_id: stagingRequestedBy },
    ],
  },
});
stagingRequestedById = requestingUser ? requestingUser.id : null;
```

### CMG → Bioloop Field Mapping

| CMG Field              | Bioloop Field           | Notes                              |
|------------------------|-------------------------|------------------------------------|
| `title`                | `title`                 | Direct mapping, nullable           |
| `access_count`         | `access_count`          | Integer, defaults to 0             |
| `is_public`            | `is_public`             | Boolean, defaults to false         |
| `staging_requested`    | `staging_requested`     | JSON, nullable                     |
| `staging_completed`    | `staging_completed`     | Boolean, defaults to false         |
| `staging_requested_by` | `staging_requested_by`  | Resolved to user_id, nullable      |

---

## 3. Integration Changes

### Updated Files

#### `api/src/scripts/cmg_poller_sync.js`

Added initialization for both new pollers:

```javascript
// 5. Project Metadata Poller
const projectMetadataPoller = new ProjectMetadataPoller(prisma, cmgDb);
pollers.push(projectMetadataPoller);
logger.info('  - project_metadata (20s interval)');

// 6. Session Metadata Poller
const sessionMetadataPoller = new SessionMetadataPoller(prisma, cmgDb);
pollers.push(sessionMetadataPoller);
logger.info('  - session_metadata (30s interval)');
```

Removed workflow status poller initialization (as requested).

#### `api/src/scripts/cmg_sync/bigbang/initialize_cursors.js`

Added cursor initialization for new pollers:

```javascript
{
  poller_name: 'project_metadata',
  last_updated_at: maxProjectUpdatedAt ? new Date(maxProjectUpdatedAt) : new Date(),
  last_cmg_objectid: null,
},
{
  poller_name: 'session_metadata',
  last_updated_at: maxSessionUpdatedAt ? new Date(maxSessionUpdatedAt) : new Date(),
  last_cmg_objectid: null,
},
```

Removed workflow status cursor initialization.
Removed Rhythm MongoDB connection (no longer needed).

#### `api/src/scripts/cmg_bigbang_sync.js`

- Removed all Rhythm MongoDB connection code
- Removed `--rhythm-uri` CLI argument
- Removed Rhythm environment variables from help text
- Updated `initializeCursors()` call to not pass `rhythmDb`

---

## 4. Poll Intervals Summary

Updated polling schedule with new pollers:

| Poller                 | Interval | Batch Size | Priority |
|------------------------|----------|------------|----------|
| `user_roles`           | 10s      | 200        | High     |
| `project_acl`          | 10s      | 100        | High     |
| `dataset_activity`     | 10s      | 200        | High     |
| `dataset_metadata`     | 15s      | 200        | Medium   |
| `project_metadata`     | 20s      | 100        | Medium   |
| `session_metadata`     | 30s      | 50         | Low      |

**Rationale:**
- Projects change less frequently than datasets
- Sessions change very rarely (mostly static after creation)
- Lower intervals reduce MongoDB load for less critical updates

---

## 5. Database Schema

No schema changes required. The pollers use existing columns:

### Project Table
```prisma
model project {
  id              String   @id @default(uuid())
  slug            String   @unique
  name            String
  description     String?
  browser_enabled Boolean  @default(false)
  funding         String?
  metadata        Json?
  cmg_id          String?  // Used for lookups
  // ... relations
}
```

### Genome Browser Session Table
```prisma
model genome_browser_session {
  id                   Int      @id @default(autoincrement())
  title                String?
  genome               String?
  genome_type          String?
  access_count         Int      @default(0)
  is_public            Boolean  @default(false)
  staging_requested    Json?
  staging_completed    Boolean  @default(false)
  staging_requested_by Int?
  cmg_id               String?  // Used for lookups
  // ... relations
}
```

---

## 6. Deployment

### After Big-Bang Migration

The cursors for the new pollers will be automatically initialized during the big-bang migration:

```bash
node src/scripts/cmg_bigbang_sync.js --cmg-uri="mongodb://..."
```

### Starting the Poller System

```bash
# Using PM2 (recommended)
pm2 start ecosystem.config.js

# Or directly
node src/scripts/cmg_poller_sync.js
```

The new pollers will start automatically with the configured intervals.

---

## 7. Monitoring

Check poller metrics in logs every 60 seconds:

```
--- Poller Metrics ---
[project_metadata]
  Running: true
  Total runs: 45
  Successful: 45
  Failed: 0
  Total processed: 234
  Last run: 2026-01-09T10:30:00Z
  Last success: 2026-01-09T10:30:00Z

[session_metadata]
  Running: true
  Total runs: 30
  Successful: 30
  Failed: 0
  Total processed: 89
  Last run: 2026-01-09T10:30:00Z
  Last success: 2026-01-09T10:30:00Z
```

Check cursor status in database:

```sql
SELECT poller_name, last_updated_at, locked_by, last_succeeded_at, last_run_count
FROM cmg_sync_cursor
WHERE poller_name IN ('project_metadata', 'session_metadata');
```

---

## 8. Testing

### Manual Testing

1. **Project Metadata:**
   ```javascript
   // In CMG MongoDB
   db.projects.updateOne(
     { _id: ObjectId("...") },
     { $set: { description: "Updated description", updatedAt: new Date() } }
   );
   
   // Wait 20 seconds
   // Check Bioloop PostgreSQL
   SELECT name, description FROM project WHERE cmg_id = '...';
   ```

2. **Session Metadata:**
   ```javascript
   // In CMG MongoDB
   db.sessions.updateOne(
     { _id: ObjectId("...") },
     { $set: { access_count: 42, updatedAt: new Date() } }
   );
   
   // Wait 30 seconds
   // Check Bioloop PostgreSQL
   SELECT title, access_count FROM genome_browser_session WHERE cmg_id = '...';
   ```

---

## 9. Removed Component

### Workflow Status Poller (REMOVED)

**File:** `api/src/scripts/cmg_sync/pollers/workflow_status_poller.js` - DELETED

This poller was monitoring Rhythm MongoDB's `workflow_meta` collection to assign dataset states based on workflow completion.

**User's Note:** Will discuss alternative approach later.

**Impact of Removal:**
- Dataset states (`STAGED`, `ARCHIVED`, `INSPECTED`) will not be automatically updated based on workflow completion
- Alternative approach will be discussed and implemented later
- All Rhythm MongoDB connection code has been removed from the sync system

---

## 10. Files Created/Modified

### Created:
- `api/src/scripts/cmg_sync/pollers/project_metadata_poller.js`
- `api/src/scripts/cmg_sync/pollers/session_metadata_poller.js`
- `api/src/scripts/CMG_SYNC_NEW_POLLERS.md` (this file)

### Modified:
- `api/src/scripts/cmg_poller_sync.js`
- `api/src/scripts/cmg_sync/bigbang/initialize_cursors.js`
- `api/src/scripts/cmg_bigbang_sync.js`

### Deleted:
- `api/src/scripts/cmg_sync/pollers/workflow_status_poller.js`

---

## Summary

Two new pollers have been successfully integrated into the CMG sync system, handling metadata updates for projects and genome browser sessions. The workflow status poller has been removed as requested, with an alternative approach to be discussed later. All changes maintain the existing architecture patterns and require no database schema modifications.


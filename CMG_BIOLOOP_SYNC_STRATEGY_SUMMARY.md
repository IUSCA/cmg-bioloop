# CMG-to-Bioloop Sync Strategy: Executive Summary
**Date:** 2026-01-03  
**Purpose:** Quick reference for sync strategy decisions and rationale

---

## Core Strategy: Polling-Based Incremental Sync

### Why Polling Instead of Change Data Capture (CDC)?

**Decision: Use polling every 2-5 minutes**

**Reasons:**
1. **MongoDB 4.0.28 limitations**: Change Streams require replica sets (not guaranteed in CMG)
2. **No guaranteed indexes**: Cannot rely on efficient change queries beyond schema-defined indexes
3. **Low traffic environment**: ~200 users, minimal concurrent activity
4. **Acceptable latency**: Few minutes delay is tolerable
5. **Simplicity**: Polling is battle-tested and easy to debug
6. **No CMG modifications**: Doesn't require changes to CMG codebase

**Trade-offs:**
- ❌ Not real-time (2-5 minute delay)
- ❌ Repeated queries to MongoDB
- ✅ Simple implementation
- ✅ No CMG code changes
- ✅ Easy to monitor and debug
- ✅ Scales fine for current load

---

## ID Stability: The cmg_id Anchor Pattern

### Problem
Running daily conversions (like current Python script) regenerates all Bioloop IDs, causing:
- User JWT tokens to become invalid
- In-progress file population to corrupt
- Cross-table references to break

### Solution: cmg_id Field + Upsert Pattern

**Every Bioloop table has `cmg_id` field storing CMG's MongoDB `_id`**

```javascript
// Lookup by cmg_id, update existing record (preserve Bioloop ID)
const existing = await prisma.user.findFirst({
  where: { cmg_id: cmgUser._id.toString() }
});

if (existing) {
  // UPDATE existing record (ID stays the same)
  await prisma.user.update({
    where: { id: existing.id },
    data: { ...userData }
  });
} else {
  // CREATE new record
  await prisma.user.create({ data: { ...userData } });
}
```

**Result:**
- ✅ Bioloop IDs never change after initial creation
- ✅ User tokens remain valid
- ✅ File population can run for days without corruption
- ✅ Referential integrity maintained

---

## Sync Frequency & Timing

### Initial Population: Once
**When:** Day 1 of deployment  
**What:** Full database drop + repopulate (like current Python script)  
**Duration:** ~5 minutes  
**Tables:** All except `dataset_file`, `workflow`

### Incremental Sync: Every 2-5 Minutes
**What:** Poll CMG for changes since last sync, upsert in Bioloop  
**Duration:** <30 seconds per run  
**Tables:** Same as initial population

### Workflow Event Detection: Every 2-5 Minutes
**What:** Detect state changes (staging complete, archival done, etc.)  
**How:** Compare CMG boolean flags against Bioloop state  
**Action:** Log events, update Bioloop, optionally trigger notifications

---

## What Gets Synced (and What Doesn't)

### ✅ Synced Business Objects

| CMG Collection | Bioloop Table | Frequency |
|----------------|---------------|-----------|
| `users` | `user`, `user_role` | Incremental |
| `datasets` | `dataset` (type='raw_data') | Incremental |
| `dataproducts` | `dataset` (type='data_product') | Incremental |
| `dataproducts` | `dataset_genomic_attributes` | Incremental |
| `events` (embedded) | `dataset_audit` | Incremental |
| `dataproducts.dataset` | `dataset_hierarchy` | Incremental |
| `projects` | `project` | Incremental |
| `projects.users` | `project_user` | Incremental |
| `projects.dataproducts` | `project_dataset` | Incremental |
| `conversions` | `conversion` | Incremental |
| `dataproducts.conversion` | `conversion_derived_dataset` | Incremental |
| N/A (constants) | `conversion_definition`, `cmd_line_program`, `argument` | Initial only |

### ❌ NOT Synced (Excluded)

| Table | Reason |
|-------|--------|
| `dataset_file` | Too large (~hundreds of TB); populate on-demand or separate process |
| `workflow` | Bioloop-specific; CMG has different workflow system |
| `genome_browser_session`, `track`, `session_track` | Bioloop-only features |

---

## Change Detection: updatedAt Timestamp Query

### MongoDB Query Pattern

```javascript
// Get records changed since last sync
const changedRecords = await cmgDb.collection('users').find({
  $or: [
    { updatedAt: { $gt: lastSyncTimestamp } },
    { createdAt: { $gt: lastSyncTimestamp } }
  ]
}).toArray();
```

### Last Sync Tracking

**Bioloop table:** `sync_metadata`

```sql
-- Get last successful sync
SELECT last_sync_at FROM sync_metadata 
WHERE sync_type = 'users' AND status = 'success'
ORDER BY last_sync_at DESC LIMIT 1;
```

**Fallback:** If no sync record exists, use epoch (1970-01-01) to sync all records

---

## Workflow Event Detection

### Problem
Bioloop needs to know when CMG:
- Completes staging a dataset
- Archives a dataset
- Finishes a conversion

### Solution: Poll CMG State Fields

**CMG fields to monitor:**
- `datasets.staged` (Boolean)
- `datasets.archived` (Boolean)
- `dataproducts.staged` (Boolean)

**Detection logic:**
```javascript
// CMG says staged=true, but Bioloop says is_staged=false
if (cmgDataset.staged && !bioloopDataset.is_staged) {
  console.log('[EVENT] Staging completed');
  // Update Bioloop + log event
}
```

**Event logging:** `sync_event_log` table tracks detected events

---

## Error Handling Strategy

### Partial Sync Failures

**Approach:** Continue with other sync tasks even if one fails

```javascript
// Sync users, datasets, projects independently
for (const task of syncTasks) {
  try {
    await task.fn();
    results.push({ task: task.name, status: 'success' });
  } catch (error) {
    results.push({ task: task.name, status: 'failed', error });
    // CONTINUE with next task
  }
}
```

### Retry Logic

**Pattern:** Retry with exponential backoff

```javascript
async function withRetry(fn, maxRetries = 3, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === maxRetries) throw error;
      await sleep(delayMs * attempt); // 1s, 2s, 3s
    }
  }
}
```

### Connection Failures

**Pattern:** Graceful cleanup

```javascript
try {
  mongoClient = await connectMongo();
  // ... sync logic ...
} finally {
  await closeMongo(); // Always close connection
}
```

---

## Deployment Options

### Option 1: Cron Job (Production)
```bash
# /etc/crontab
*/2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js
```

**Pros:** Simple, reliable, standard Unix tool  
**Cons:** No built-in monitoring, logs to file

### Option 2: PM2 with Cron (Development)
```javascript
// ecosystem.config.js
{
  name: 'bioloop-sync',
  script: 'src/scripts/sync/incrementalSync.js',
  cron_restart: '*/2 * * * *',
}
```

**Pros:** Easy restart, log management, process monitoring  
**Cons:** Requires PM2 installed

### Option 3: Node.js setInterval (Docker)
```javascript
// Continuous loop with sleep
while (true) {
  await runIncrementalSync();
  await sleep(2 * 60 * 1000); // 2 minutes
}
```

**Pros:** Self-contained, works in Docker  
**Cons:** Must handle process crashes

**Recommendation:** Use cron in production, PM2 in development

---

## Monitoring & Health Checks

### Key Queries

```sql
-- Last successful sync
SELECT * FROM sync_metadata 
WHERE status = 'success' 
ORDER BY last_sync_at DESC LIMIT 1;

-- Recent failures
SELECT * FROM sync_metadata 
WHERE status = 'failed' 
ORDER BY last_sync_at DESC LIMIT 10;

-- Sync lag (how old is last sync?)
SELECT 
  sync_type,
  last_sync_at,
  NOW() - last_sync_at AS lag
FROM sync_metadata
WHERE status = 'success'
ORDER BY last_sync_at DESC;

-- Detected workflow events
SELECT * FROM sync_event_log 
ORDER BY timestamp DESC LIMIT 20;
```

### Alerting Thresholds

- ⚠️ **Warning:** Sync lag > 10 minutes
- 🚨 **Critical:** Sync lag > 30 minutes
- 🚨 **Critical:** 3+ consecutive sync failures

---

## Performance Characteristics

### Initial Population
- **Duration:** ~5 minutes
- **Records:** ~10,000 users, ~50,000 datasets, ~500 projects
- **Load:** High (full table scans)
- **Frequency:** Once

### Incremental Sync
- **Duration:** <30 seconds
- **Records:** ~10-100 changed records per run
- **Load:** Low (indexed queries on updatedAt)
- **Frequency:** Every 2-5 minutes

### Workflow Event Detection
- **Duration:** <10 seconds
- **Records:** ~1-10 events per run
- **Load:** Very low
- **Frequency:** Every 2-5 minutes

**Total overhead:** ~40 seconds every 2-5 minutes = <10% CPU utilization

---

## Future Enhancements (Post-MVP)

### Phase 2: dataset_file Population
**Approach:** Separate periodic process
- Process one dataset at a time
- Populate files for staged datasets only
- Run daily during off-peak hours

### Phase 3: Real-Time Event Webhooks
**Approach:** Modify CMG to publish events (if allowed)
- CMG API publishes to RabbitMQ on key events
- Bioloop worker consumes events
- Reduces sync lag to <1 second

### Phase 4: Change Data Capture (CDC)
**Approach:** Upgrade MongoDB to 4.2+ with replica sets
- Use MongoDB Change Streams
- Real-time sync with <1 second lag
- Requires CMG infrastructure upgrade

---

## Key Principles

1. **✅ Preserve Bioloop IDs**: Never delete/recreate, only update
2. **✅ Use cmg_id as anchor**: All lookups use this field
3. **✅ Respect dependencies**: Sync parents before children
4. **✅ Fail gracefully**: Log errors, continue with other tasks
5. **✅ Idempotent operations**: Upserts are safe to run repeatedly
6. **✅ Simple over perfect**: Polling is fine for this scale
7. **✅ Monitor continuously**: Check sync_metadata regularly

---

## Quick Start Commands

```bash
# Initial setup (Day 1)
cd /opt/sca/cmg-bioloop/api
node src/scripts/sync/initialPopulation.js

# Start incremental sync (cron)
crontab -e
# Add: */2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js

# Check sync health
psql -U bioloopuser -d bioloop_db -c "SELECT * FROM sync_metadata ORDER BY last_sync_at DESC LIMIT 5;"

# View recent events
psql -U bioloopuser -d bioloop_db -c "SELECT * FROM sync_event_log ORDER BY timestamp DESC LIMIT 10;"
```

---

**End of Summary**

For detailed implementation instructions, see: `CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md`


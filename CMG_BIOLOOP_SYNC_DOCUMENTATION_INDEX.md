# CMG-to-Bioloop Database Synchronization: Documentation Index
**Date:** 2026-01-03  
**Status:** Design Complete - Ready for Implementation

---

## Overview

This documentation suite provides a complete strategy for synchronizing data between CMG (MongoDB) and Bioloop (PostgreSQL) systems. The solution uses a **polling-based incremental sync** approach that preserves Bioloop ID stability while keeping data synchronized with CMG.

---

## Documentation Files

### 1. **CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md** (Primary Implementation Guide)
**Purpose:** Detailed technical instructions for implementing the sync system  
**Audience:** AI agents and developers implementing the sync  
**Length:** ~1,500 lines

**Contents:**
- Overview & constraints
- Synchronization strategy
- Initial population process
- Incremental update process
- Business object mapping (users, datasets, projects, conversions)
- Change detection strategy
- Implementation guide with code examples
- Workflow event detection
- Error handling & recovery
- Execution schedule

**When to use:** Reference this document when implementing any part of the sync system.

---

### 2. **CMG_BIOLOOP_SYNC_STRATEGY_SUMMARY.md** (Executive Summary)
**Purpose:** High-level overview of sync strategy and key decisions  
**Audience:** Project managers, architects, and quick reference  
**Length:** ~400 lines

**Contents:**
- Core strategy explanation (polling-based)
- Why polling instead of CDC
- ID stability pattern (cmg_id anchor)
- Sync frequency & timing
- What gets synced (and what doesn't)
- Change detection approach
- Workflow event detection
- Error handling strategy
- Deployment options
- Monitoring & health checks
- Performance characteristics
- Future enhancements

**When to use:** Quick reference for understanding the overall approach and rationale.

---

### 3. **SYNC_APPROACH_COMPARISON.md** (Decision Analysis)
**Purpose:** Compare different sync strategies and justify chosen approach  
**Audience:** Technical decision-makers, architects  
**Length:** ~600 lines

**Contents:**
- Comparison matrix of 6 different approaches
- Detailed pros/cons for each:
  1. Daily Full Rebuild (current Python script)
  2. **Polling-Based Incremental (CHOSEN)**
  3. MongoDB Change Streams (CDC)
  4. CMG Event Webhooks
  5. Dual-Write Pattern
  6. Database Replication (FDW)
- Decision matrix showing why polling wins
- Hybrid approach explanation
- Polling frequency trade-offs
- Migration path to real-time (future)
- Risk analysis
- Performance projections

**When to use:** Understanding why this approach was chosen over alternatives.

---

### 4. **SYNC_IMPLEMENTATION_ROADMAP.md** (Step-by-Step Guide)
**Purpose:** Phased implementation plan with concrete tasks  
**Audience:** AI agents and developers doing the implementation  
**Length:** ~800 lines

**Contents:**
- 8 implementation phases with time estimates
- Phase 0: Preparation (2 hours)
- Phase 1: Connection & Utilities (3 hours)
- Phase 2: User Sync (4 hours)
- Phase 3: Dataset Sync (6 hours)
- Phase 4: Project & Conversion Sync (4 hours)
- Phase 5: Initial Population Script (2 hours)
- Phase 6: Incremental Sync Script (2 hours)
- Phase 7: Automation & Monitoring (2 hours)
- Phase 8: Documentation & Handoff (1 hour)
- Code examples for each phase
- Testing checklist
- Success criteria
- Timeline summary (26 hours / 3-4 days total)

**When to use:** Follow this roadmap phase-by-phase during implementation.

---

## Quick Start Guide

### For Understanding the Strategy
1. Read **CMG_BIOLOOP_SYNC_STRATEGY_SUMMARY.md** (15 minutes)
2. Review **SYNC_APPROACH_COMPARISON.md** if you need to understand why this approach (20 minutes)

### For Implementation
1. Read **CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md** thoroughly (1 hour)
2. Follow **SYNC_IMPLEMENTATION_ROADMAP.md** phase by phase (3-4 days)
3. Reference **CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md** for detailed patterns as needed

---

## Key Concepts

### The cmg_id Anchor Pattern
Every Bioloop table has a `cmg_id` field storing CMG's MongoDB `_id`. This is the **anchor** for all sync operations:

```javascript
// Lookup by cmg_id, update existing record (preserve Bioloop ID)
const existing = await prisma.user.findFirst({
  where: { cmg_id: cmgUser._id.toString() }
});

if (existing) {
  // UPDATE (ID stays the same)
  await prisma.user.update({
    where: { id: existing.id },
    data: { ...userData }
  });
} else {
  // CREATE new record
  await prisma.user.create({ data: { ...userData } });
}
```

**Result:** Bioloop IDs never change after initial creation.

---

### Polling-Based Change Detection

Query CMG MongoDB for records changed since last sync:

```javascript
const changedRecords = await cmgDb.collection('users').find({
  $or: [
    { updatedAt: { $gt: lastSyncTimestamp } },
    { createdAt: { $gt: lastSyncTimestamp } }
  ]
}).toArray();
```

**Frequency:** Every 2-5 minutes (configurable)

---

### Two-Phase Sync Process

**Phase 1: Initial Population (One-time)**
- Drop and recreate Bioloop data
- Populate all tables from CMG
- Establish baseline with cmg_id tracking

**Phase 2: Incremental Sync (Recurring)**
- Poll CMG for changes since last sync
- Upsert changed records in Bioloop
- Preserve Bioloop IDs

---

## Architecture Diagram (Text)

```
┌─────────────────────────────────────────────────────────────────┐
│                         CMG (Legacy)                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  MongoDB 4.0.28                                          │   │
│  │  - users                                                 │   │
│  │  - datasets (raw_data)                                   │   │
│  │  - dataproducts (data_product)                           │   │
│  │  - projects                                              │   │
│  │  - conversions                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Poll every 2-5 minutes
                              │ Query: updatedAt > lastSync
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Sync Scripts (Node.js)                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  incrementalSync.js                                      │   │
│  │  - Connect to CMG MongoDB                                │   │
│  │  - Get lastSyncTimestamp from Bioloop                    │   │
│  │  - Query changed records                                 │   │
│  │  - Upsert in Bioloop (using cmg_id)                      │   │
│  │  - Update sync_metadata                                  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Upsert (preserve IDs)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Bioloop (New System)                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PostgreSQL + Prisma                                     │   │
│  │  - user (cmg_id field)                                   │   │
│  │  - dataset (cmg_id field)                                │   │
│  │  - project (cmg_id field)                                │   │
│  │  - conversion (cmg_id field)                             │   │
│  │  - sync_metadata (tracking table)                        │   │
│  │  - sync_event_log (event tracking)                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Critical Constraints

1. ✅ **NO CMG source code modifications**
2. ✅ **Bioloop IDs must remain stable** (no regeneration)
3. ✅ **Use Prisma/JavaScript only** (no Python for sync logic)
4. ✅ **Quick implementation** (3-4 days)
5. ✅ **MongoDB 4.0.28 compatible** (no replica set required)
6. ✅ **Acceptable latency** (2-5 minutes)
7. ❌ **NO workflow table sync** (Bioloop-specific)
8. ❌ **NO dataset_file population** (too large, separate process)

---

## Tables Synced

### ✅ Synced from CMG

| CMG Collection | Bioloop Table | Notes |
|----------------|---------------|-------|
| `users` | `user`, `user_role` | Role mapping applied |
| `datasets` | `dataset` (type='raw_data') | |
| `dataproducts` | `dataset` (type='data_product') | |
| `dataproducts` | `dataset_genomic_attributes` | Extracted fields |
| `events` | `dataset_audit` | Embedded in datasets |
| `dataproducts.dataset` | `dataset_hierarchy` | Parent-child relationship |
| `projects` | `project` | |
| `projects.users` | `project_user` | Array expansion |
| `projects.dataproducts` | `project_dataset` | Array expansion |
| `conversions` | `conversion` | |
| `dataproducts.conversion` | `conversion_derived_dataset` | |
| N/A (constants) | `conversion_definition`, `cmd_line_program`, `argument` | Hardcoded |

### ❌ NOT Synced

| Table | Reason |
|-------|--------|
| `dataset_file` | Too large (~hundreds of TB); separate process |
| `workflow` | Bioloop-specific; different system |
| `genome_browser_session`, `track`, `session_track` | Bioloop-only features |

---

## Monitoring Queries

### Check Sync Health
```sql
-- Last successful sync
SELECT * FROM sync_metadata 
WHERE status = 'success' 
ORDER BY last_sync_at DESC LIMIT 1;

-- Recent failures
SELECT * FROM sync_metadata 
WHERE status = 'failed' 
ORDER BY last_sync_at DESC LIMIT 10;

-- Sync lag
SELECT 
  sync_type,
  last_sync_at,
  NOW() - last_sync_at AS lag
FROM sync_metadata
WHERE status = 'success'
ORDER BY last_sync_at DESC;
```

### Check Synced Data
```sql
-- Count synced records
SELECT 
  'users' as table_name, COUNT(*) as count, COUNT(cmg_id) as with_cmg_id FROM "user"
UNION ALL
SELECT 'datasets', COUNT(*), COUNT(cmg_id) FROM dataset
UNION ALL
SELECT 'projects', COUNT(*), COUNT(cmg_id) FROM project
UNION ALL
SELECT 'conversions', COUNT(*), COUNT(cmg_id) FROM conversion;

-- Verify ID stability (run after multiple syncs)
SELECT id, username, cmg_id, updated_at 
FROM "user" 
ORDER BY updated_at DESC 
LIMIT 10;
```

---

## Implementation Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| Preparation | 2 hours | Schema updates, dependencies, env vars |
| Connection & Utilities | 3 hours | MongoDB connection, sync metadata, utils |
| User Sync | 4 hours | User + role sync logic |
| Dataset Sync | 6 hours | Datasets, dataproducts, genomic attributes |
| Project & Conversion Sync | 4 hours | Projects, conversions, relationships |
| Initial Population Script | 2 hours | One-time full population |
| Incremental Sync Script | 2 hours | Recurring sync runner |
| Automation & Monitoring | 2 hours | Cron setup, monitoring queries |
| Documentation & Handoff | 1 hour | README, usage instructions |
| **TOTAL** | **26 hours** | **~3-4 days** |

---

## Success Criteria

### Phase Complete When:
- ✅ All scripts implemented and tested
- ✅ Initial population runs successfully
- ✅ Incremental sync runs successfully
- ✅ Bioloop IDs remain stable across syncs
- ✅ Cron job configured and running
- ✅ Monitoring queries work
- ✅ Documentation complete

### Production Ready When:
- ✅ Synced for 1 week without issues
- ✅ No ID regeneration observed
- ✅ Sync lag consistently <5 minutes
- ✅ Error handling tested and working
- ✅ Monitoring alerts configured

---

## Future Enhancements

### Phase 2: dataset_file Population
- Separate periodic process
- Process one dataset at a time
- Populate files for staged datasets only
- Run daily during off-peak hours

### Phase 3: Real-Time Event Webhooks
- Modify CMG to publish events (if allowed)
- Bioloop worker consumes events
- Reduces sync lag to <1 second

### Phase 4: Change Data Capture (CDC)
- Upgrade MongoDB to 4.2+ with replica sets
- Use MongoDB Change Streams
- Real-time sync with <1 second lag

---

## Troubleshooting

### Sync Not Running
```bash
# Check cron job
crontab -l

# Check logs
tail -f /var/log/bioloop-sync.log

# Run manually
cd /opt/sca/cmg-bioloop/api
node src/scripts/sync/incrementalSync.js
```

### IDs Changing (Critical Issue)
```sql
-- Verify cmg_id populated
SELECT COUNT(*), COUNT(cmg_id) FROM "user";

-- Check if upsert logic is working
SELECT id, username, cmg_id, updated_at 
FROM "user" 
WHERE cmg_id IS NOT NULL
ORDER BY updated_at DESC LIMIT 5;
```

### Sync Lag Too High
```sql
-- Check last sync time
SELECT sync_type, last_sync_at, NOW() - last_sync_at AS lag
FROM sync_metadata
WHERE status = 'success'
ORDER BY last_sync_at DESC;
```

**Solutions:**
- Reduce `SYNC_INTERVAL_MINUTES` in `.env`
- Check for MongoDB connection issues
- Verify cron job running

---

## Contact & Support

**Documentation Author:** AI Agent (Claude Sonnet 4.5)  
**Date Created:** 2026-01-03  
**Version:** 1.0

**For Questions:**
- Review documentation files in order listed above
- Check code examples in `SYNC_IMPLEMENTATION_ROADMAP.md`
- Refer to business object mapping in `CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md`

---

## Document Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-03 | 1.0 | Initial documentation suite created |

---

**Next Steps:** Review documentation, then begin implementation following `SYNC_IMPLEMENTATION_ROADMAP.md`


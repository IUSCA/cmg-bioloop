# CMG Database Migration to Bioloop

**Feature Scope:** Migrating CMG's MongoDB database to Bioloop's PostgreSQL database with ongoing synchronization.

**Status:** In Progress

**Related Documentation:**
- `/CMG_BIOLOOP_DATABASE_SYNC---POLLING.md`
- `/data_sync/` directory
- `/CMG_SYNC_IMPLEMENTATION_PROGRESS.md`

---

## 2026-01-16

### Initial State Documentation

**Context:** This feature manages the migration of CMG's legacy MongoDB database to Bioloop's PostgreSQL database, along with ongoing synchronization.

**Implementation Approach:** Two-phase strategy
1. **Big-Bang Script** (`cmg_bigbang_sync.js`): One-time initial population
   - Seeds constants (roles, programs, conversion definitions)
   - Migrates all existing CMG data to Bioloop
   - Respects dependency order

2. **Poller Script** (`cmg_poller_sync.js`): Continuous incremental sync
   - 5 concurrent pollers running in single Node process
   - Cursor-based tracking: `(updatedAt, _id)` per poller
   - Independent locks and cursors per poller
   - PM2-managed in API container

**Key Architecture Decisions:**
- Decision: Use `cmg_id` field in Bioloop tables to map to CMG MongoDB `_id`
- Decision: Idempotent upserts to avoid creating duplicate records
- Decision: Transaction-safe batch commits (all-or-nothing)
- Decision: No ID changes after initial migration (preserves referential integrity)
- Constraint: Workflow Status Poller connects to Rhythm MongoDB (not CMG)

**Pollers:**
1. User Roles Poller (10s interval)
2. Project ACL Poller (10s interval)
3. Dataset Activity Poller (10-15s interval)
4. Dataset Metadata Poller (10-15s interval)
5. Workflow Status Poller (10s interval) - connects to Rhythm MongoDB

**Database Schema Additions:**
- `cmg_id` fields added to relevant tables (user, dataset, project, etc.)
- `sync_cursor` table for tracking poller state
- `sync_lock` mechanism to prevent concurrent poller runs

**Current Status:**
- Big-Bang script: Implemented
- Poller script: Implemented
- Running in production: [Status to be determined]

---

## Future Entries

Add entries here as decisions are made, changes are implemented, or issues are resolved.

Format:
```
## YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]
```

---

**Last Updated:** 2026-01-16


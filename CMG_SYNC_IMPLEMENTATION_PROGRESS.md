# CMG Sync Implementation Progress

**Started:** 2026-01-04  
**Status:** In Progress - Foundation & Utils Complete, Workflow Poller Added (Phases 1-2)

**Recent Pivot:** Dataset states are now assigned by polling Rhythm MongoDB's `workflow_meta` collection instead of parsing CMG events arrays. This approach is more reliable as it directly checks workflow completion status.  

---

## ✅ Phase 1: Foundation (COMPLETE)

### Schema Changes
- ✅ Added `cmg_sync_cursor` table to schema.prisma
- ✅ Added `cmg_sync_retry` table to schema.prisma
- ⏳ **TODO:** Run Prisma migration

### Configuration
- ✅ Added `cmg_mongodb` config to `api/config/default.json`
- ✅ Added `rhythm_mongodb` config to `api/config/default.json`
- ✅ Added environment variable mappings to `api/config/custom-environment-variables.json`

### Core Modules Created
- ✅ `api/src/scripts/cmg_sync/connections.js` - MongoDB + Prisma connection management
- ✅ `api/src/scripts/cmg_sync/error_logger.js` - JSONL error logging with full context
- ✅ `api/src/scripts/cmg_sync/cursor_manager.js` - Lock acquisition, cursor tracking
- ✅ `api/src/scripts/cmg_sync/constants.js` - All seeding constants

### Documentation
- ✅ `CMG_BIOLOOP_DATABASE_SYNC---POLLING.md` - Complete strategy document (60+ pages)

---

## ✅ Phase 2: Utility Modules (COMPLETE)

### Utils Created
- ✅ `api/src/scripts/cmg_sync/utils/cmg_helpers.js`
  - ObjectId handling
  - CMG document transformations
  - Common CMG data patterns
  - Group expansion
  - Path extraction
  - Slug generation

- ✅ `api/src/scripts/cmg_sync/utils/event_parser.js`
  - **NOT USED** (kept for potential future use)
  - Parse CMG events arrays
  - Extract event timestamps
  - **NOTE:** Dataset states now assigned via Workflow Status Poller instead

- ✅ `api/src/scripts/cmg_sync/utils/state_mapper.js`
  - Map CMG events → Bioloop states
  - Application-level state uniqueness
  - State addition logic

- ✅ `api/src/scripts/cmg_sync/utils/role_mapper.js`
  - CMG role → Bioloop role mapping
  - Role validation
  - Role ID resolution

- ✅ `api/src/scripts/cmg_sync/utils/duplicate_handler.js`
  - Handle duplicate dataset names
  - Generate DUPLICATE_ prefixes
  - Check name uniqueness
  - Handle unknown names

---

## 🚧 Phase 3: Big-Bang Script (TODO)

### Seeding Modules
- ⏳ `api/src/scripts/cmg_sync/bigbang/seed_constants.js`
  - Seed roles
  - Seed cmd_line_programs
  - Seed conversion_definitions
  - Seed arguments

- ⏳ `api/src/scripts/cmg_sync/bigbang/sync_users.js`
  - Convert CMG users → Bioloop users
  - Map roles
  - Handle CMG system user

- ⏳ `api/src/scripts/cmg_sync/bigbang/sync_datasets.js`
  - Convert CMG datasets → RAW_DATA
  - Convert CMG dataproducts → DATA_PRODUCT
  - Insert genomic attributes
  - Parse events → dataset_state rows
  - Handle duplicate names

- ⏳ `api/src/scripts/cmg_sync/bigbang/sync_projects.js`
  - Convert CMG projects
  - Expand groups → users
  - Create project_user associations
  - Create project_dataset associations

- ⏳ `api/src/scripts/cmg_sync/bigbang/sync_conversions.js`
  - Convert CMG conversions
  - Map pipeline → conversion_definition
  - Link derived datasets

- ⏳ `api/src/scripts/cmg_sync/bigbang/initialize_cursors.js`
  - Initialize cursor for each poller
  - Set initial timestamps

### Main Big-Bang Script
- ⏳ `api/src/scripts/cmg_bigbang_sync.js`
  - Orchestrate all seeding and sync
  - Strict dependency order
  - Transaction management
  - Progress logging

---

## 🚧 Phase 4: Poller Implementation (TODO)

### Base Poller
- ⏳ `api/src/scripts/cmg_sync/pollers/base_poller.js`
  - Abstract base class for all pollers
  - Common polling logic
  - Lock acquisition/release
  - Cursor management
  - Error handling

### Individual Pollers
- ⏳ `api/src/scripts/cmg_sync/pollers/user_roles_poller.js`
  - Poll CMG users collection
  - Update user metadata (NOT identity fields)
  - Diff-based role updates

- ⏳ `api/src/scripts/cmg_sync/pollers/project_acl_poller.js`
  - Poll CMG projects collection
  - Expand groups on-demand
  - Rebuild project_user associations
  - Rebuild project_dataset associations

- ⏳ `api/src/scripts/cmg_sync/pollers/dataset_activity_poller.js`
  - Poll CMG datasets + dataproducts
  - Update paths and flags
  - **NOTE:** Does NOT parse events for states (delegated to Workflow Status Poller)

- ✅ `api/src/scripts/cmg_sync/pollers/workflow_status_poller.js`
  - Poll Rhythm MongoDB workflow_meta collection
  - Assign dataset states based on completed workflows
  - Only "integrated" and "stage" workflows assign states
  - Application-level state uniqueness

- ⏳ `api/src/scripts/cmg_sync/pollers/dataset_metadata_poller.js`
  - Poll CMG datasets + dataproducts
  - Update size, description, counts
  - Update file_type

### Main Poller Script
- ⏳ `api/src/scripts/cmg_poller_sync.js`
  - Start all pollers concurrently
  - Graceful shutdown handling
  - Health monitoring
  - Log aggregation

---

## 🚧 Phase 5: Deployment (TODO)

### PM2 Configuration
- ⏳ Update `api/ecosystem.config.js`
  - Add cmg-poller app
  - Configure autorestart
  - Set log paths

### Environment Variables
- ⏳ Document required env vars
  - CMG MongoDB credentials
  - Batch sizes
  - Poll intervals

### Testing
- ⏳ Test connections
- ⏳ Test big-bang with sample data
- ⏳ Test pollers with sample data
- ⏳ Verify idempotency
- ⏳ Test error logging

### Deployment Scripts
- ⏳ Migration script
  - Run Prisma migrate
  - Verify schema

- ⏳ Initialization script
  - Run big-bang
  - Start pollers

---

## 🚧 Phase 6: Monitoring & Operations (TODO)

### Monitoring Tools
- ⏳ Create cursor status dashboard query
- ⏳ Create error statistics query
- ⏳ Create lag monitoring query

### Operational Runbook
- ⏳ Common troubleshooting steps
- ⏳ Emergency procedures
- ⏳ Performance tuning guide

---

## File Structure (Current State)

```
api/
├── config/
│   ├── default.json                      ✅ DONE (added cmg_mongodb + rhythm_mongodb)
│   └── custom-environment-variables.json ✅ DONE (added both mongo configs)
├── prisma/
│   └── schema.prisma                     ✅ DONE (added sync tables)
└── src/scripts/
    ├── cmg_bigbang_sync.js               ⏳ TODO
    ├── cmg_poller_sync.js                ⏳ TODO
    └── cmg_sync/
        ├── connections.js                ✅ DONE (MongoDB + Prisma)
        ├── error_logger.js               ✅ DONE (JSONL error logging)
        ├── cursor_manager.js             ✅ DONE (Lock + cursor tracking)
        ├── constants.js                  ✅ DONE (All seeding data)
        ├── pollers/
        │   ├── base_poller.js            ⏳ TODO
        │   ├── user_roles_poller.js      ⏳ TODO
        │   ├── project_acl_poller.js     ⏳ TODO
        │   ├── dataset_activity_poller.js⏳ TODO
        │   ├── workflow_status_poller.js ✅ DONE (Rhythm workflows → states)
        │   └── dataset_metadata_poller.js⏳ TODO
        ├── bigbang/
        │   ├── seed_constants.js         ⏳ TODO
        │   ├── sync_users.js             ⏳ TODO
        │   ├── sync_datasets.js          ⏳ TODO
        │   ├── sync_projects.js          ⏳ TODO
        │   ├── sync_conversions.js       ⏳ TODO
        │   └── initialize_cursors.js     ⏳ TODO
        └── utils/
            ├── cmg_helpers.js            ✅ DONE (ObjectId, paths, groups)
            ├── event_parser.js           ✅ DONE (Not used - kept for future)
            ├── state_mapper.js           ✅ DONE (Events → States)
            ├── role_mapper.js            ✅ DONE (CMG → Bioloop roles)
            └── duplicate_handler.js      ✅ DONE (Handle duplicate names)
```

---

## Estimated Completion

- **Phase 1 (Foundation):** ✅ Complete
- **Phase 2 (Utils):** ~2-3 hours
- **Phase 3 (Big-Bang):** ~4-6 hours
- **Phase 4 (Pollers):** ~4-6 hours
- **Phase 5 (Deployment):** ~2-3 hours
- **Phase 6 (Operations):** ~1-2 hours

**Total Remaining:** ~13-20 hours of implementation

---

## Next Steps (Immediate)

1. Run Prisma migration to create new tables
2. Create utility modules (event parser, state mapper, etc.)
3. Implement big-bang seeding modules
4. Implement big-bang main script
5. Test big-bang with sample CMG data
6. Implement base poller class
7. Implement individual pollers
8. Test pollers with sample data
9. Update PM2 configuration
10. Deploy and monitor

---

**Last Updated:** 2026-01-04  
**By:** AI Agent  
**Status:** Foundation complete, ready for Phase 2


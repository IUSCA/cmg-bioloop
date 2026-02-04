# CMG → Bioloop Sync Testing

## Overview

Autonomous testing framework for verifying data synchronization between CMG (MongoDB) and Bioloop (PostgreSQL) databases. Enables two AI agents to coordinate testing with minimal human supervision.

**Created:** 2026-02-04  
**Status:** Active  
**Related Systems:** Data Sync (`data_sync/`), Pollers, Bigbang

---

## Architecture

### Components

1. **CMG Agent** (in CMG repository)
   - Makes controlled changes in CMG MongoDB
   - Logs test entries to shared files
   - Verifies CMG database safety before writes

2. **Bioloop Agent** (in this repository)
   - Runs bigbang sync to initialize data
   - Starts and monitors pollers
   - Verifies changes appear in Bioloop PostgreSQL
   - Generates test reports

3. **Shared Communication Directory**
   - **Location:** `/tmp/cmg-bioloop/` on host
   - **Mounted in:** Both CMG and Bioloop containers
   - **Purpose:** Real-time file-based coordination between agents

---

## Shared Directory Structure

### Location & Mount Configuration

**Host Path:** `/tmp/cmg-bioloop/`

**Bioloop Mount:**
```yaml
# data_sync/docker-compose.localhost.yml
volumes:
  - /tmp/cmg-bioloop:/tmp/cmg-bioloop
```

**CMG Mount:**
```yaml
# /Users/ripandey/dev/cmg/docker-compose.local.yml (or equivalent)
volumes:
  - /tmp/cmg-bioloop:/tmp/cmg-bioloop
```

### File Structure

```
/tmp/cmg-bioloop/
├── Documentation (Protocol & Instructions)
│   ├── AGENT_COORDINATION_PROTOCOL.md       # Master protocol for both agents
│   ├── CMG_AGENT_AUTONOMOUS_INSTRUCTIONS.md # CMG agent step-by-step guide
│   ├── FIELD_MAPPING_QUICK_REF.md           # CMG ↔ Bioloop field translations
│   ├── CMG_MONGODB_INDEXES.md               # Required MongoDB indexes documentation
│   ├── README.md                             # Quick start guide for both agents
│   ├── ALL_SYSTEMS_READY.md                 # System status summary
│   ├── STATUS_FOR_USER.md                   # Human-readable status report
│   └── BIOLOOP_SETUP_COMPLETE.md            # Bioloop setup phase summary
│
├── Communication Files (Agent Coordination)
│   ├── agent_status.jsonl                   # Real-time agent status updates
│   ├── coordination_queue.jsonl             # Commands from one agent to another
│   ├── coordination_responses.jsonl         # Responses to commands
│   └── coordination.log                     # Combined timestamped activity log
│
├── Test Execution Files
│   ├── test_queue.jsonl                     # CMG agent writes: test changes made
│   └── test_results.jsonl                   # Bioloop agent writes: verification results
│
└── Output Files
    ├── bigbang_output.log                   # Bigbang sync logs
    ├── poller_output.log                    # Poller activity logs
    ├── monitor_output.log                   # Test monitor logs
    └── FINAL_REPORT.md                      # Complete test results (generated at end)
```

---

## Protocol Files

### AGENT_COORDINATION_PROTOCOL.md

**Purpose:** Master coordination document that defines:
- Communication file formats
- Test execution flow
- Safety rules (CMG database protection)
- Success criteria
- Error handling procedures

**Used By:** Both agents at initialization

### CMG_AGENT_AUTONOMOUS_INSTRUCTIONS.md

**Purpose:** Step-by-step instructions for CMG agent:
- MongoDB safety checks (verify NOT production)
- How to make test changes
- How to log changes to test_queue.jsonl
- How to wait for Bioloop verification
- Test templates and examples

**Used By:** CMG agent only

### FIELD_MAPPING_QUICK_REF.md

**Purpose:** Quick reference for field name translations between CMG and Bioloop:
- Projects: `description`, `notes` (same in both)
- Users: `name`, `email`, `username` (same), but `active` doesn't exist in Bioloop
- Datasets: `staged` (CMG) → `is_staged` (Bioloop), `path.staged` → `path_staged`, etc.

**Important:** DO NOT rename fields in either application to match the other.

**Used By:** Both agents when mapping fields

---

## Communication Protocol

### File Formats

#### agent_status.jsonl
One JSON object per line, each line is a status update from an agent.

```jsonl
{"agent":"bioloop","timestamp":"2026-02-04T02:00:00.000Z","status":"ready","phase":"testing","message":"All pollers healthy"}
{"agent":"cmg","timestamp":"2026-02-04T02:00:30.000Z","status":"working","phase":"testing","message":"Executing test_001"}
```

**Status Values:** `initializing`, `ready`, `working`, `waiting`, `error`, `complete`  
**Phases:** `setup`, `bigbang`, `testing`, `cleanup`

#### coordination_queue.jsonl
Commands from one agent to another.

```jsonl
{"id":"cmd_001","from":"bioloop","to":"cmg","command":"start_testing","timestamp":"...","params":{...}}
```

**Commands:** `start_testing`, `verify_change`, `pause`, `resume`, `complete_batch`

#### test_queue.jsonl
Test entries logged by CMG agent after making changes.

```jsonl
{"test_id":"test_001","batch":"batch_1","timestamp":"2026-02-04T02:00:00.000Z","collection":"projects","cmg_id":"606b521f...","field":"description","bioloop_field":"description","old_value":"...","new_value":"TEST_DESC_123","poller":"project_metadata","cmg_updated_at":"2026-02-04T02:00:00.123Z"}
```

**Required Fields:**
- `test_id` - Unique test identifier
- `collection` - CMG collection name (`projects`, `users`, `datasets`)
- `cmg_id` - MongoDB ObjectId as string
- `field` - Field name in CMG
- `bioloop_field` - Corresponding field in Bioloop (may differ)
- `new_value` - Value set in CMG
- `poller` - Which poller should detect this change
- `cmg_updated_at` - Timestamp when `updatedAt` was set in CMG

#### test_results.jsonl
Verification results from Bioloop agent.

```jsonl
{"test_id":"test_001","status":"success","cmg_timestamp":"2026-02-04T02:00:00.000Z","bioloop_timestamp":"2026-02-04T02:00:15.234Z","delay_ms":15234,"verified_value":"TEST_DESC_123","poller":"project_metadata"}
```

**Status Values:** `success`, `timeout`, `error`

#### coordination.log
Timestamped activity log from both agents.

```
[2026-02-04T02:00:00.000Z] [BIOLOOP] [BIGBANG] Starting bigbang with --target-db=app --skip-conversion-logs --clear-target-db
[2026-02-04T02:10:34.567Z] [BIOLOOP] [BIGBANG] Completed in 10m 34s
[2026-02-04T02:11:00.123Z] [CMG] [SAFETY] Verified MongoDB host: db (NOT production)
[2026-02-04T02:11:31.012Z] [CMG] [TEST] Starting test_001: Project description change
```

**Format:** `[ISO_TIMESTAMP] [AGENT] [CATEGORY] message`

---

## Test Execution Flow

### Phase 1: Bioloop Setup

1. **Verify CMG MongoDB Indexes**
   - Check `updatedAt` indexes exist on all collections
   - Required for efficient poller queries

2. **Run Bigbang Sync**
   - Command: `node src/bigbang_sync.js --target-db=app --skip-conversion-logs --clear-target-db`
   - Duration: ~6-10 minutes
   - Syncs all data from CMG to Bioloop
   - Initializes poller cursors

3. **Start Pollers**
   - Command: `node src/poller_sync.js --target-db=app`
   - 5 pollers run continuously:
     - `user_roles` (10s interval)
     - `project_acl` (10s interval)
     - `dataset_activity` (10s interval)
     - `dataset_metadata` (15s interval)
     - `project_metadata` (20s interval)

4. **Signal Ready**
   - Write to `coordination_queue.jsonl`: `{"command":"start_testing"}`
   - Update `agent_status.jsonl`: `{"status":"ready","phase":"testing"}`

### Phase 2: Test Execution (Both Agents)

#### CMG Agent Loop

For each test:

1. **Safety Check**
   ```javascript
   const host = mongoUrl.match(/mongodb:\/\/([^:\/]+)/)[1];
   if (host.startsWith('commons3')) {
     throw new Error('PRODUCTION DATABASE - ABORTING');
   }
   ```

2. **Make Change in MongoDB**
   ```javascript
   await db.collection('projects').updateOne(
     { _id: ObjectId("...") },
     { $set: { 
       description: "TEST_DESC_" + Date.now(),
       updatedAt: new Date()  // CRITICAL!
     }}
   )
   ```

3. **Log to test_queue.jsonl**

4. **Wait for Bioloop verification**

5. **Wait 30 seconds before next test**

#### Bioloop Agent Loop

For each new test in `test_queue.jsonl`:

1. **Poll Bioloop database every 2 seconds**
   ```javascript
   const record = await prisma.project.findUnique({
     where: { cmg_id: test.cmg_id },
     select: { [test.bioloop_field]: true }
   });
   ```

2. **Check if value matches**
   - If match: Log success with sync delay
   - If no match after 60s: Log timeout

3. **Write result to test_results.jsonl**

### Phase 3: Reporting (Bioloop Agent)

1. Analyze all test results
2. Generate `/tmp/cmg-bioloop/FINAL_REPORT.md`:
   - Test summary (success/failure/timeout counts)
   - Average sync delays per poller
   - Issues encountered
   - Recommendations

---

## Safety Rules

### CMG Database Protection

**CRITICAL:** CMG agent MUST verify database is local before ANY write operation.

```javascript
// REQUIRED safety check
const mongoUrl = process.env.MONGODB_URL;
const host = mongoUrl.match(/mongodb:\/\/([^:\/]+)/)?.[1];

if (host.startsWith('commons3')) {
  throw new Error('🚨 PRODUCTION DATABASE - ABORTING ALL OPERATIONS');
}

if (host !== 'db' && host !== 'localhost' && host !== '127.0.0.1') {
  throw new Error(`🚨 Unknown host "${host}" - only db/localhost/127.0.0.1 are safe`);
}

// Log verification
fs.appendFileSync('/tmp/cmg-bioloop/coordination.log', 
  `[${new Date().toISOString()}] [CMG] [SAFETY] ✓ Verified: ${host} (LOCAL)\n`);
```

**This check MUST be logged to coordination.log for audit trail.**

### Bioloop Database Operations

- Always use `--target-db=app` for bigbang/pollers
- Never reset database without user permission
- All operations logged to coordination.log

---

## Test Cases

### Standard Test Suite

**Batch 1: Projects** (2 tests)
- Test 001: Change `description` field
- Test 002: Change `notes` field
- Poller: `project_metadata` (20s interval)
- Test ID: `606b521f02d8137b0ff40049`

**Batch 2: Users** (2 tests)
- Test 003: Change `name` field
- Test 004: Change `email` field (use unique value!)
- Poller: `user_roles` (10s interval)
- Test ID: `5f577fb638972540c2718122`

**Batch 3: Datasets** (2 tests)
- Test 005: Change `description` field
- Test 006: Change `size` field
- Poller: `dataset_metadata` (15s interval)
- Test ID: Use any valid dataset from CMG MongoDB

### Success Criteria

A test is successful if:
1. Change detected in Bioloop within 60 seconds
2. Value matches exactly what was set in CMG
3. No errors in poller logs

Typical sync delays:
- Projects: 15-20 seconds (20s poller interval)
- Users: 8-12 seconds (10s poller interval)
- Datasets: 12-18 seconds (10-15s poller intervals)

---

## Known Issues & Bugs Fixed

### Bug: Cursor Update Error (Fixed 2026-02-04)

**Symptoms:**
- Dataset pollers crash with "Invalid lastDoc for cursor update"
- Error shows timestamps from when poller started, not from bigbang
- User/project pollers work fine

**Root Cause:**
`base_poller.js:272` was passing wrong object structure to `updateCursor()`:

```javascript
// WRONG (was passing object with wrong keys):
await updateCursor(tx, this.pollerName, {
  last_updated_at: lastDoc.updatedAt,      // Wrong key name
  last_cmg_objectid: lastDoc._id.toString() // Wrong key name
});

// CORRECT (now passing raw document):
await updateCursor(tx, this.pollerName, lastDoc);
```

**Fix Applied:** `/Users/ripandey/dev/cmg-bioloop-3/data_sync/src/sync/pollers/base_poller.js:272`

**Impact:** All 5 pollers now work correctly with cursors set by bigbang.

---

## Troubleshooting

### Pollers Not Detecting Changes

**Check:**
1. Is `updatedAt` being set in CMG when making changes?
   ```javascript
   // MUST include updatedAt in $set
   { $set: { field: value, updatedAt: new Date() } }
   ```

2. Are indexes present in CMG MongoDB?
   ```javascript
   db.collection.getIndexes() // Should show updatedAt_1__id_1
   ```

3. Check poller logs:
   ```bash
   tail -50 /tmp/cmg-bioloop/poller_output.log
   ```

### Test Times Out

**Possible causes:**
1. Record doesn't exist in Bioloop (run bigbang first)
2. `updatedAt` not set in CMG
3. Poller crashed (check poller_output.log)
4. Field name mismatch (check FIELD_MAPPING_QUICK_REF.md)

### Cannot Access Shared Directory

**Check:**
1. Mount exists in docker-compose file:
   ```yaml
   volumes:
     - /tmp/cmg-bioloop:/tmp/cmg-bioloop
   ```

2. Directory created on host:
   ```bash
   ls -la /tmp/cmg-bioloop
   ```

3. Container can access:
   ```bash
   docker exec <container> ls -la /tmp/cmg-bioloop
   ```

---

## Maintenance

### Adding New Test Cases

1. Update test suite in `AGENT_COORDINATION_PROTOCOL.md`
2. Add field mappings to `FIELD_MAPPING_QUICK_REF.md`
3. Update CMG agent instructions if needed

### Updating Field Mappings

**DO NOT rename fields in either application.**

If field names differ:
1. Add mapping to `FIELD_MAPPING_QUICK_REF.md`
2. Update poller sync logic in `data_sync/src/sync/pollers/*_poller.js`
3. Document in `data_sync/CMG_BIOLOOP_FIELD_MAPPING.md`

### Cleaning Up After Tests

```bash
# Clear test files
rm -f /tmp/cmg-bioloop/test_*.jsonl
rm -f /tmp/cmg-bioloop/agent_status.jsonl
rm -f /tmp/cmg-bioloop/coordination*.jsonl
rm -f /tmp/cmg-bioloop/*.log

# Keep documentation files:
# - AGENT_COORDINATION_PROTOCOL.md
# - CMG_AGENT_AUTONOMOUS_INSTRUCTIONS.md
# - FIELD_MAPPING_QUICK_REF.md
# - ALL_SYSTEMS_READY.md
```

---

## Related Documentation

### In This Repository
- `data_sync/BIGBANG_SYNC_USAGE.md` - Bigbang migration guide
- `data_sync/POLLER_SYNC_USAGE.md` - Poller architecture
- `data_sync/CMG_BIOLOOP_FIELD_MAPPING.md` - Complete field mappings
- `data_sync/docker-compose.localhost.yml` - Sandbox container config
- `.ai/features/cmg-bioloop-sync-testing.md` - This comprehensive guide

### In Shared Mount (`/tmp/cmg-bioloop/`)
**Documentation & Protocols:**
- `AGENT_COORDINATION_PROTOCOL.md` - Master protocol (read first!)
- `CMG_AGENT_AUTONOMOUS_INSTRUCTIONS.md` - CMG agent guide
- `FIELD_MAPPING_QUICK_REF.md` - Quick field reference
- `CMG_MONGODB_INDEXES.md` - Required MongoDB indexes
- `README.md` - Quick start guide

**Status & Reports:**
- `ALL_SYSTEMS_READY.md` - Current system status
- `STATUS_FOR_USER.md` - Human-readable progress
- `BIOLOOP_SETUP_COMPLETE.md` - Setup phase summary
- `FINAL_REPORT.md` - Test results (generated after tests)

---

## Example: Monitoring Test Progress

```bash
# Watch real-time coordination
tail -f /tmp/cmg-bioloop/coordination.log

# Check agent status
cat /tmp/cmg-bioloop/agent_status.jsonl | tail -5 | jq .

# Monitor test queue
tail -f /tmp/cmg-bioloop/test_queue.jsonl

# Watch test results come in
tail -f /tmp/cmg-bioloop/test_results.jsonl

# Check poller health
tail -30 /tmp/cmg-bioloop/poller_output.log | grep "Round complete"
```

---

## Summary

This testing framework enables autonomous end-to-end verification of CMG→Bioloop data synchronization using:

1. **Shared Directory:** `/tmp/cmg-bioloop/` mounted in both containers
2. **File-Based Coordination:** JSONL files for agent communication
3. **Safety First:** Mandatory CMG database verification before writes
4. **Real-Time Monitoring:** Comprehensive logging and status tracking
5. **Minimal Supervision:** Agents coordinate autonomously, human only checks final report

**Key Success Factors:**
- All pollers must be healthy before testing
- CMG `updatedAt` field must be set on every change
- Both agents must have access to shared directory
- Safety checks must pass before any CMG writes

**Typical Test Duration:** 4-6 minutes for 6 tests (including 30s waits between tests)

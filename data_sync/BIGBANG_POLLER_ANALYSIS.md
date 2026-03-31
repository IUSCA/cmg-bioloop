# Bigbang & Poller Analysis — CMG + Xenium Migration

**Scope:** Code-level review of `data_sync/src/bigbang_cmg_sync.js`, `data_sync/src/bigbang_xenium_sync.js`, `data_sync/src/poller_cmg_sync.js`, `data_sync/src/poller_xenium_sync.js`, all sync modules under `src/sync/cmg/` and `src/sync/xenium/`, shared utilities in `src/sync/shared/`, and `data_sync/bin/init.sh`.

**Context:** CMG bigbang/pollers were battle-tested through a full production run before the xenium migration was added. The code was then substantially refactored to support both sources. Since this refactor, no full bigbang has been run. The xenium bigbang/pollers have never been run at all. Both need to be re-verified before first production use.

**Method:** Every issue below was cross-referenced against `.ai/customizations/features/cmg-database-migration.md` and `.ai/features/xenium-migration.md` to confirm it is not intentional design.

---

## Summary Table

| ID | Issue | Severity | Applies To |
|----|-------|----------|------------|
| H1 | `isLegacySourceActive` inherits CMG's boolean when xenium key absent | **HIGH** | Xenium |
| H2 | `--clear-xenium-target-data` doesn't auto-clear bigbang lock | **HIGH** | Xenium |
| H3 | Retry table is dead-letter log; failed rows bypassed when cursor advances | **HIGH** | Both |
| H4 | `clearXeniumTargetRows` single transaction will timeout for large datasets | **HIGH** | Xenium |
| H5 | `trackRetry` inside transaction — entry lost on transaction rollback | **HIGH** | Both |
| M1 | `cmguser` origin contaminated by which bigbang runs first | **MEDIUM** | Both |
| M2 | `syncProjectAssociations` delete+createMany not wrapped in transaction | **MEDIUM** | Both |
| M3 | Pollers don't check for running bigbang (only bigbang checks for pollers) | **MEDIUM** | Both |
| M4 | Source DB queries inside target DB transaction — timeout risk | **MEDIUM** | Both |
| M5 | CMG bigbang log step counter inconsistent (1/15 → 4/16 → 16/18) | **MEDIUM** | CMG only |
| M6 | Null cursor triggers full-table scan on cold start | **MEDIUM** | Xenium |
| L1 | No `error_logger` for xenium pollers | **LOW** | Xenium |
| L2 | Xenium metrics reporter missing `lastSuccessTime` | **LOW** | Xenium |
| L3 | `MONGO_URI` shell warning fires even when `CMG_MONGO_*` vars are set | **LOW** | init.sh |
| L4 | `toDateOrNow(null)` returns non-deterministic current timestamp | **LOW** | Xenium |
| L5 | No exit guard on uniqueness loops | **LOW** | Xenium |
| L6 | Help text includes shebang line | **LOW** | init.sh |
| L7 | Silent skip for users named 'cmguser'/'xeniumuser' in xenium source | **LOW** | Xenium |

---

## HIGH SEVERITY — Real Bugs

---

### H1. `isLegacySourceActive` inherits CMG's boolean when `xenium` key is absent

**File:** `data_sync/src/sync/xenium/bigbang/helpers.js`

**Code (lines 11–25):**
```js
function isLegacySourceActive(sourceName = 'xenium') {
  const legacyConfig = readLegacyConfig();

  if (typeof legacyConfig === 'boolean') return legacyConfig;
  if (legacyConfig && typeof legacyConfig === 'object') {
    if (Object.prototype.hasOwnProperty.call(legacyConfig, sourceName)) {
      return Boolean(legacyConfig[sourceName]);
    }
    if (Object.prototype.hasOwnProperty.call(legacyConfig, 'cmg')) {
      return Boolean(legacyConfig.cmg);   // <-- BUG
    }
  }

  return true;
}
```

**The bug:** If `legacy_application_active` is configured as `{ cmg: false }` — without a `xenium` key — the function falls into the second `hasOwnProperty` branch and returns CMG's `false` for xenium. This causes every call to `withDatasetOrigin()` from xenium bigbang modules to return `'bioloop'` instead of `'legacy_xenium'`.

**Impact:**
- All xenium-migrated rows get `metadata.origin = 'bioloop'`
- `isXeniumDataset()` checks fail (return false for migrated rows)
- `stage_migrated_xenium` workflow is never triggered for these datasets
- Hydration and legacy migration status APIs return wrong results

**When it fires:** If CMG is retired before xenium (i.e. someone sets `{ cmg: false }` while xenium is still active and hasn't been given its own explicit key). The AI docs explicitly require CMG and xenium to be independently retire-able on different dates. This fallback violates that contract.

**Fix:** Remove the `hasOwnProperty('cmg')` fallback block entirely. When `xenium` is absent from the config object, the function should fall through to `return true` (default active), not inherit from CMG.

---

### H2. `--clear-xenium-target-data` does not auto-clear the bigbang lock (asymmetry with CMG)

**Files:** `data_sync/src/bigbang_cmg_sync.js` vs `data_sync/src/bigbang_xenium_sync.js`

**CMG bigbang — correct pattern:**
```js
// 1. Check poller lock (exits if pollers running)
const pollerLockStatus = await checkProcessLockStatus(prisma, 'poller');
if (pollerLockStatus) { ... process.exit(1); }

// 2. Clear data AND auto-clear all CMG locks
if (options.clearCmgTargetData) {
  await clearCmgTargetData(prisma);
  await forceReleaseAllProcessLocks(prisma);  // clears stale bigbang lock too
}

// 3. Acquire bigbang lock — always succeeds after step 2
lockAcquired = await acquireProcessLock(prisma, 'bigbang', DEFAULT_LOCK_TTL_MS);
```

**Xenium bigbang — broken pattern:**
```js
// 1. Clear locks ONLY if --clear-locks flag provided
if (options.clearLocks) { await forceReleaseAllProcessLocks(prisma); }

// 2. Check xenium poller lock
const pollerLockStatus = await checkProcessLockStatus(prisma, 'xenium_poller');
if (pollerLockStatus) { ... process.exit(1); }

// 3. Acquire bigbang lock — FAILS if stale lock from previous crash
lockAcquired = await acquireProcessLock(prisma, 'xenium_bigbang', DEFAULT_LOCK_TTL_MS);
if (!lockAcquired) { ... process.exit(1); }  // exits here before reaching step 4

// 4. Clear data — NEVER REACHED if step 3 failed
if (options.clearXeniumTargetData) { await clearXeniumTargetRows(prisma); }
```

**The bug:** If a previous xenium bigbang crashed without releasing its `xenium_bigbang` lock, passing `--clear-xenium-target-data` alone is not sufficient — the script exits at step 3 before ever reaching the clear step. The operator must separately pass `--clear-locks` in addition. In CMG, `--clear-cmg-target-data` handles this scenario implicitly via `forceReleaseAllProcessLocks`. This is an undocumented asymmetry that will be a footgun during disaster recovery.

**Fix options (either):**
1. Move `clearXeniumTargetRows` and `forceReleaseAllProcessLocks` to before `acquireProcessLock`, mirroring the CMG pattern.
2. Or: document explicitly in `--help` output and the ops runbook that `--clear-locks` is always required alongside `--clear-xenium-target-data`.

---

### H3. Retry table is a dead-letter log, not a retry queue — failed rows can be silently bypassed

**Files:** `data_sync/src/sync/xenium/pollers/base_poller.js` (same logic in `data_sync/src/sync/cmg/pollers/base_poller.js`)

**Code (xenium base_poller.js, syncBatch):**
```js
await this.prisma.$transaction(async (tx) => {
  for (const row of rows) {   // rows sorted by (updated_at ASC, id ASC)
    try {
      await this.processRow(row, tx);
      processedCount++;
      lastRow = row;           // only advances on SUCCESS
    } catch (rowError) {
      await this.trackRetry(tx, row.id, rowError);
      // lastRow NOT updated — failed row does not advance cursor
    }
  }

  if (lastRow) {
    await updateCursor(tx, this.pollerName, lastRow.updated_at, lastRow.id);
  }
});
```

**The bug (applies equally to CMG):** `lastRow` only advances on successful rows. If row[0] fails but rows[1..N-1] succeed, `lastRow = row[N-1]`, and the cursor advances to `(row[N-1].updated_at, row[N-1].id)`. The next poll query is:

```sql
WHERE (updated_at > row[N-1].updated_at) OR (updated_at = row[N-1].updated_at AND id > row[N-1].id)
```

Row[0] has a **lower** `(updated_at, id)` than `row[N-1]`. It will never appear in any future query unless it is modified in the source DB (which updates its `updated_at`). The entry in `*_sync_retry` is a record of the failure, but **nothing reads from that table to actually re-process entries**. The retry table is a dead-letter log only.

**Impact:** Individual row failures where the row appears early in a batch result in permanently lost sync for that row. The poller continues operating normally; the failure is logged but the data is never applied.

**Additional note for xenium:** The xenium base poller does not have an equivalent of CMG's `logSyncError` call (from `cmg/error_logger.js`). CMG pollers write richer diagnostic context (collection name, full document, operation) before `trackRetry`. Xenium pollers only call `trackRetry`, providing only the xenium ID and error message. Less information is available for post-mortem.

---

### H4. `clearXeniumTargetRows` uses a single Prisma `$transaction` — will timeout for large datasets

**File:** `data_sync/src/bigbang_xenium_sync.js`

**Code:**
```js
async function clearXeniumTargetRows(prisma) {
  await prisma.$transaction(async (tx) => {
    await tx.project.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.dataset.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.user.deleteMany({ where: { xenium_id: { not: null } } });
    await tx.xenium_sync_retry.deleteMany({});
    await tx.xenium_sync_cursor.deleteMany({});
  });
}
```

Prisma interactive transactions default to a **5-second timeout**. When `dataset.deleteMany` runs, the schema's `onDelete: Cascade` triggers cascading deletes for every associated `dataset_file`, `dataset_audit`, `dataset_import_log`, `dataset_hierarchy`, `data_access_log`, `stage_request_log`, and `bundle` row. For a production xenium migration with hundreds of datasets and thousands of associated rows, this single transaction will exceed the timeout and throw a `PrismaClientKnownRequestError`.

**Compare to CMG:** `clearCmgTargetData` does not use a transaction at all. It runs sequential `deleteMany` calls outside any transaction context, explicitly pre-deleting dependent records (argument_values, process_requests, data_access_logs, stage_request_logs) before the main entity deletes. This avoids the timeout at the cost of atomicity.

Since the bigbang is idempotent (and a failed clear is always recoverable by re-running with `--clear-locks`), the CMG approach's lack of atomicity is acceptable. The xenium transaction-based approach is conceptually cleaner but will fail at scale.

**Fix:** Match the CMG pattern — remove the transaction wrapper and use sequential `prisma.*` calls. Pre-delete or rely on cascade explicitly after testing timeout behavior with production-scale data.

---

### H5. `trackRetry` is called inside the Prisma transaction — retry record is lost on rollback

**Files:** `data_sync/src/sync/xenium/pollers/base_poller.js`, `data_sync/src/sync/cmg/pollers/base_poller.js`

**Code:**
```js
await this.prisma.$transaction(async (tx) => {
  for (const row of rows) {
    try {
      await this.processRow(row, tx);
    } catch (rowError) {
      await this.trackRetry(tx, row.id, rowError);  // uses tx, not prisma
    }
  }
  if (lastRow) {
    await updateCursor(tx, this.pollerName, lastRow.updated_at, lastRow.id);
  }
});
```

`trackRetry` writes to `xenium_sync_retry` (or `cmg_sync_retry`) using the transaction client `tx`. If any subsequent operation in the transaction causes a rollback (for example, `updateCursor` throws because the cursor row was deleted, or a DB constraint is violated on a later `processRow`), the entire transaction rolls back — **including the retry entry**. The failure is logged to the application log but the persistent record in the retry table is lost.

**Impact:** The retry table cannot be relied upon as a complete audit of all sync failures. Failures that occur in batches where the transaction ultimately rolls back will not appear in the retry table.

**Fix:** Write retry entries using the main `prisma` client (outside the transaction) rather than `tx`. A separate, non-transactional upsert to the retry table would survive the main transaction's rollback.

---

## MEDIUM SEVERITY — Real Concerns

---

### M1. `cmguser` system user gets incorrect `metadata.origin` based on which bigbang runs first

**Files:** `data_sync/src/sync/xenium/constants.js`, `data_sync/src/sync/xenium/bigbang/seed_constants.js`

The xenium constants define the system user as:
```js
const XENIUM_USER = {
  username: 'cmguser',  // shares the CMG system user
  ...
};
```

`createXeniumUser` creates 'cmguser' with `metadata: withDatasetOrigin({ source: 'xenium_system_user' })`, which sets `metadata.origin = 'legacy_xenium'` (default `sourceName = 'xenium'`).

**Run order dependency:**
- If **xenium bigbang runs first**: 'cmguser' is created with `origin = 'legacy_xenium'`. CMG bigbang then finds 'cmguser' by username and skips creation. The CMG system user permanently has `origin = 'legacy_xenium'`.
- If **CMG bigbang runs first**: 'cmguser' is created with `origin = 'legacy'`. Xenium bigbang finds it and skips. The CMG system user has the correct origin.

`metadata.origin` on the system user affects any downstream code that calls `isLegacyDataset()` / `isXeniumDataset()` on user-related rows that reference this user. It also contaminates `metadata.origin` semantics if used as a fallback for attribution.

**Fix:** Give xenium a dedicated system user with `username: 'xeniumuser'` and update `sync_audit_logs.js` and `sync_import_logs.js` to fall back to 'xeniumuser' instead of 'cmguser'. The CMG docs already define 'xeniumuser' as the xenium system account.

---

### M2. `syncProjectAssociations` delete+createMany is not atomic

**Files:** `data_sync/src/sync/xenium/bigbang/sync_projects.js`, `data_sync/src/sync/cmg/bigbang/sync_projects.js`

```js
async function syncProjectAssociations(prisma, xeniumPrisma, sourceProjectId, targetProjectId, maps) {
  // ...resolve associations...

  await prisma.project_user.deleteMany({ where: { project_id: targetProjectId } });
  await prisma.project_dataset.deleteMany({ where: { project_id: targetProjectId } });

  if (resolvedProjectUsers.length > 0) {
    await prisma.project_user.createMany({ data: ..., skipDuplicates: true });
  }
  if (resolvedProjectDatasets.length > 0) {
    await prisma.project_dataset.createMany({ data: ..., skipDuplicates: true });
  }
}
```

The delete and createMany calls are two separate DB operations with no transaction wrapper. If the process crashes between the `deleteMany` and `createMany` calls, the project loses all its user and dataset associations. There is no partially-completed-state guard.

**Idempotency note:** The bigbang IS idempotent — re-running would re-delete (finding nothing) and re-create the associations. So recovery via re-run is possible. But the intermediate window where the project has no members is a real concern if the target DB is being read during the bigbang (e.g. by the running application).

**Fix:** Wrap both operations in a transaction:
```js
await prisma.$transaction(async (tx) => {
  await tx.project_user.deleteMany(...);
  await tx.project_dataset.deleteMany(...);
  if (...) await tx.project_user.createMany(...);
  if (...) await tx.project_dataset.createMany(...);
});
```

---

### M3. Pollers do not check if bigbang is currently running (one-directional guard only)

**Files:** `data_sync/src/bigbang_xenium_sync.js`, `data_sync/src/bigbang_cmg_sync.js`

Bigbang correctly checks for running pollers and exits:
```js
const pollerLockStatus = await checkProcessLockStatus(prisma, 'xenium_poller');
if (pollerLockStatus) { ... process.exit(1); }
```

But pollers do **not** check for a running bigbang:
```js
// poller_xenium_sync.js — no bigbang check
lockAcquired = await acquireProcessLock(prisma, 'xenium_poller', POLLER_LOCK_TTL_MS);
```

If xenium pollers are started (via `init.sh --xenium-start-pollers`) while a xenium bigbang is mid-run in a separate session, both processes would write concurrently to the target DB. The bigbang's idempotency checks (`findFirst({ where: { xenium_id } })`) could race with pollers writing the same entities, potentially causing duplicate records or FK violations.

**Fix:** Add a bigbang lock check at poller startup (symmetric with bigbang's poller check):
```js
const bigbangLockStatus = await checkProcessLockStatus(prisma, 'xenium_bigbang');
if (bigbangLockStatus) {
  logger.error('Xenium bigbang is currently running. Start pollers after bigbang completes.');
  process.exit(1);
}
```

---

### M4. Source DB queries run inside the target DB transaction — timeout risk under load

**Files:** `data_sync/src/sync/xenium/pollers/project_acl_poller.js`, `data_sync/src/sync/cmg/pollers/project_acl_poller.js`

In `XeniumProjectACLPoller.processRow()`, which is called inside `prisma.$transaction()`:

```js
// This is called inside this.prisma.$transaction(async (tx) => { ... })
async processRow(row, tx) {
  const bioloopProject = await tx.project.findFirst(...);  // target DB, in tx

  const [sourceProjectUsers, sourceProjectDatasets] = await Promise.all([
    this.xeniumPrisma.project_user.findMany({ where: { project_id: row.id } }),  // source DB, outside tx
    this.xeniumPrisma.project_dataset.findMany({ where: { project_id: row.id } }),
  ]);
  // ...write to target DB via tx...
}
```

Prisma's default interactive transaction timeout is **5 seconds**. The transaction begins in `syncBatch`, and `processRow` is called for each row in the batch. If the xenium source DB is slow (network latency, lock contention, query planning), each `processRow` call holds an open target DB transaction while waiting for source DB results. For a batch of 200 project rows each requiring two source DB queries, this can easily exceed 5 seconds.

**Note:** This is the same pattern in CMG's `project_acl_poller.js`. It was considered acceptable during CMG's battle-testing. But the risk should be acknowledged for both systems.

**Mitigation options:**
1. Pre-fetch all source DB associations before opening the transaction, and pass them to `processRow` as in-memory data.
2. Increase the Prisma transaction timeout via `prisma.$transaction(..., { timeout: 30000 })`.
3. Reduce batch size for the project ACL poller specifically.

---

### M5. CMG bigbang log step counter is inconsistent (refactoring artifact)

**File:** `data_sync/src/bigbang_cmg_sync.js`

The log messages use inconsistent step fractions introduced during the refactor that added steps:

```
[1/15] Creating roles...
[2/15] Creating CMG system user...
[3/15] Populating pipeline definitions...
[4/16] Seeding analysis types...
[5/16] Seeding import sources...
[6/16] Populating Bioloop users from JSON files...
...
[15/16] Converting dataset hierarchies...
[16/18] Converting historic conversion logs...
[17/18] Converting genome browser sessions...
[18/18] Initializing poller cursors...
```

The denominator changes from **15 → 16 → 18** mid-run. The comment block at the top says 18 steps. The actual step count is 18. Steps 1–3 were never updated after new steps were added. This is confusing when reading production logs and makes it impossible to estimate progress.

**Fix:** Update all step log messages to use `[N/18]` consistently.

---

### M6. Null cursor on cold start causes full-table scan in xenium pollers

**File:** `data_sync/src/sync/xenium/pollers/base_poller.js`

```js
buildWhere(cursor, roundEnd) {
  const where = {};

  if (cursor.last_updated_at) {            // skipped if null
    where.OR = [
      { updated_at: { gt: cursor.last_updated_at } },
      { updated_at: cursor.last_updated_at, id: { gt: cursor.last_xenium_id } },
    ];
  }

  if (roundEnd) {
    where.updated_at = { ...where.updated_at, lte: roundEnd };  // { lte: roundEnd } only
  }

  return where;
}
```

If `cursor.last_updated_at` is null, the `OR` block is skipped entirely. The resulting Prisma `WHERE` clause is just `updated_at <= roundEnd`, which returns **every row in the source table** up to the current time.

**When this fires:** `initializeCursors` sets `last_updated_at = null` if the source table was empty at bigbang time (e.g. a xenium instance with no datasets yet). When the poller starts after data is added to xenium, the first poll round fetches all rows. With `batchSize = 200`, it processes 200 rows per round and requires many rounds to drain. This is catch-up behavior rather than a hard failure, but it is unexpected and could produce a long initial sync delay.

**Fix:** Document this behavior. Optionally, guard with a log warning when `cursor.last_updated_at` is null to make the catch-up behavior visible in logs.

---

## LOW SEVERITY — Minor Gaps

---

### L1. No `error_logger` module for xenium pollers

**File:** `data_sync/src/sync/xenium/pollers/base_poller.js`

CMG base poller imports and calls `logSyncError` from `data_sync/src/sync/cmg/error_logger.js` before each `trackRetry`:

```js
// CMG base_poller.js
await logSyncError({
  poller: this.pollerName,
  operation: 'processDocument',
  cmg_collection: collectionName,
  cmg_id: doc._id.toString(),
  error: docError,
  cmg_document: doc,       // full document snapshot
});
await this.trackRetry(tx, doc._id.toString(), docError);
```

No equivalent file exists at `data_sync/src/sync/xenium/error_logger.js`. Xenium pollers only call `trackRetry`. Only the last error message per `(poller_name, xenium_id)` pair is preserved (in `xenium_sync_retry.last_error`, capped at 500 chars). No full row snapshot, no operation context, no history of multiple failures.

**Impact:** Xenium sync failures have less diagnostic data for post-mortem investigation.

---

### L2. Xenium poller metrics reporter omits `lastSuccessTime`

**File:** `data_sync/src/poller_xenium_sync.js`

The xenium `startMetricsReporter` logs `lastRunTime` but not `lastSuccessTime`, even though the metric is tracked in `XeniumBasePoller.metrics.lastSuccessTime`:

```js
// xenium startMetricsReporter — missing lastSuccessTime
logger.info(`  Last run: ${metrics.lastRunTime || 'Never'}`);
// (no lastSuccessTime line)
```

CMG's `startMetricsReporter` logs both. This makes it harder to distinguish "last run was a while ago" from "last successful run was a while ago" when reviewing xenium poller logs.

---

### L3. `MONGO_URI` shell warning in `init.sh` fires even when `CMG_MONGO_*` env vars are correctly set

**File:** `data_sync/bin/init.sh`

```bash
if [[ -z "$MONGO_URI" ]]; then
  echo -e "${YELLOW}Warning: MONGO_URI is not set (required for CMG operations).${NC}"
fi
```

The CMG bigbang (`bigbang_cmg_sync.js`) reads MongoDB connection info from the **config system** — environment variables `CMG_MONGO_HOST`, `CMG_MONGO_PORT`, `CMG_MONGO_DB`, `CMG_MONGO_USERNAME`, `CMG_MONGO_PASSWORD` — not from a `MONGO_URI` shell variable. A correctly configured environment using `CMG_MONGO_*` variables will still trigger this shell warning, making operators think something is misconfigured when it is not.

---

### L4. `toDateOrNow` returns a non-deterministic timestamp for null source dates

**File:** `data_sync/src/sync/xenium/bigbang/helpers.js`

```js
function toDateOrNow(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) return new Date();  // current time
  return date;
}
```

If a source record has `created_at = null` or an invalid date, the target record is assigned the current execution timestamp. Re-running the bigbang's update path would call this function again and produce a different timestamp. While idempotency is maintained at the row level (using `xenium_id` for existence checks), the `created_at` / `updated_at` fields of affected rows change on each re-run. This makes log-based debugging of repeated runs harder.

---

### L5. No maximum-iteration guard in uniqueness loops

**Files:** `data_sync/src/sync/xenium/bigbang/sync_datasets.js`, `data_sync/src/sync/xenium/bigbang/helpers.js`

```js
// sync_datasets.js
// eslint-disable-next-line no-constant-condition
while (true) {
  const existing = await prisma.dataset.findFirst({ where: { name: candidate, ... } });
  if (!existing) return candidate;
  candidate = `${baseName}--xenium-${suffix}`;
  suffix += 1;
}
```

Same pattern in `generateUniqueProjectSlug`. No maximum retry count or timeout guard. In the unlikely but possible scenario where a large number of name collisions exist (e.g. many re-runs, or pre-existing data), this loops indefinitely, blocking the bigbang with no error or log message.

**Fix:** Add a maximum iteration guard (e.g. 1000 attempts) and throw a descriptive error if exceeded.

---

### L6. `init.sh` help text includes the shebang line

**File:** `data_sync/bin/init.sh`

```bash
show_help() {
  grep '^#' "$SCRIPT_PATH" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}
```

`grep '^#'` matches `#!/usr/bin/env bash` on line 1. The `sed 's/^# \?//'` strips `# ` but not `!`, so the help output starts with `!/usr/bin/env bash`. Cosmetic, but operators relying on `--help` output will see noise at the top.

---

### L7. Silent skip for users named 'cmguser' or 'xeniumuser' in the xenium source DB

**File:** `data_sync/src/sync/xenium/bigbang/sync_users.js`

```js
if (sourceUser.username === 'cmguser' || sourceUser.username === 'xeniumuser') {
  skippedCount += 1;
  continue;
}
```

If a real xenium user happens to have the username 'cmguser' or 'xeniumuser' (e.g. a test account that was not cleaned up in the xenium source DB), they are silently skipped. The `skippedCount` increments but no warning is logged identifying which user was skipped or why. Low practical risk but the silent nature of the skip could cause confusion during post-migration user count reconciliation.

**Fix:** At minimum, log a warning when skipping a user for this reason:
```js
logger.warn(`[XENIUM][sync_users] Skipping reserved username: ${sourceUser.username} (id=${xeniumId})`);
```

---

## Known Outstanding TODOs (from AI docs, not bugs)

The following are **known gaps documented in `.ai/customizations/features/cmg-database-migration.md`** (2026-02-22 section). They are listed here for completeness since they affect the completeness of the live-sync story after bigbang.

- **No CMG Upload importer poller**: No CMG poller currently creates new dataset rows from CMG Uploads. New datasets created in CMG after the bigbang will not appear in Bioloop until this poller is implemented. Confirmed in AI docs as NOT yet implemented.
- **No conversion poller for `metadata.origin`**: When a new Conversion happens in CMG and a poller creates the corresponding `conversion` row in Bioloop, `metadata = { origin: 'legacy' }` must be set. Not yet implemented.
- **No dataset poller for xenium new uploads**: Same gap on the xenium side — new datasets registered in xenium after bigbang are not automatically synced to Bioloop via pollers (handled by `watch.py` observer going forward instead).

These are by-design omissions tracked for future work, not regressions.

---

## Open Design Decisions (To Be Resolved Before Finalizing Refactor)

The following are not bugs — they are architectural questions that need a deliberate decision. The current code has a position on each (implicit or explicit), but that position has not been consciously chosen and documented. Resolving these will determine whether H3/H5 from the bug list get fixed, removed, or replaced.

---

### Decision 1: Should per-row sync failures be retried, and if so, how?

#### The current situation

The schema has two tables dedicated to tracking per-row sync failures:
- `cmg_sync_retry` — one row per `(poller_name, cmg_id)` with `failure_count`, `last_error`, `next_retry_at`
- `xenium_sync_retry` — same structure with `xenium_id`

The pollers populate these tables when `processRow` throws. However, **nothing reads from these tables**. There is no scheduler, no retry worker, no background job that queries `next_retry_at <= now()` and re-processes those entries. The tables are populated but never consumed. As documented in H3: failed rows that fall early in a batch are cursor-bypassed and permanently dropped from sync unless the source record is modified in the source DB (which would update its `updated_at` and re-trigger polling).

The schema infrastructure for retries is in place. The implementation is not.

#### Arguments for implementing retries

- **Correctness**: A transient error (DB connection blip, Prisma constraint race, temporary network issue to the source DB) should not permanently skip a business object. Without retries, a user whose role change fails to sync during a brief DB hiccup stays desynced indefinitely.
- **Observability**: Retries give the system a chance to self-heal. Without them, the only signal that a sync failed is a line in the application log and a row in a table no one queries.
- **The schema already supports it**: `next_retry_at`, `failure_count`, and `last_error` are all present. The investment to implement a retry worker on top of this schema is relatively small.

#### Arguments against implementing retries (or for removing the infrastructure)

- **Stale-state problem (the key concern)**: This is the most important objection. Suppose a project's ACL changes at T=1, the poller fails to apply it, and a retry entry is created for that project. Between T=1 and the retry at T=2 (one hour later by default), the project's ACL changes again — a user is added, a dataset is removed, etc. The retry at T=2 would re-apply the T=1 state, which is now stale. If the T=2 changes were successfully polled, the retry would overwrite correct Bioloop state with older CMG/xenium state. This is a consistency inversion — the retry mechanism designed to improve consistency would actively corrupt it.

  The ACL poller is the most dangerous example because it does a full replace (delete-all, recreate-from-source). If a retry re-applies a stale source snapshot, it removes membership changes that were correctly applied in a later successful poll round.

- **Pollers are idempotent by design**: The correct and safe way to recover a failed row is to wait for the source to modify that record again (which will update its `updated_at` and re-trigger polling), or to manually touch the record in the source DB. This is the "correct" recovery path — it guarantees the retry uses current source state, not stale state.

- **The current poller cursor design already provides eventual consistency**: Any record that is modified in the source DB after a failure will be re-fetched and re-applied in the next poll round. Transient failures self-heal as long as the source record changes again.

- **Retry complexity is high**: A retry worker needs to fetch the *current* source state at retry time (not the state at failure time, which is what `cmg_sync_retry` stores implicitly via a reference). This means the retry worker is essentially a mini-poller that re-queries the source DB. That is non-trivial to implement correctly.

#### Challenges if retries are implemented

1. **Stale-state guard**: Before applying a retry, the worker must compare the retry's `next_retry_at` against the cursor's `last_updated_at` for that poller. If the cursor has advanced past the failed record (i.e. the record was successfully re-polled in a later round), the retry entry should be discarded, not applied. Without this guard, retries will corrupt data.

2. **Re-fetching current source state**: The retry must not replay the original failed operation with the original data. It must re-query the source DB (MongoDB or xenium PostgreSQL) for the current state of that record and apply that. This means the retry worker needs a live connection to the source DB and must implement the same mapping logic as `processRow`. In practice, the retry worker *is* a targeted poller run.

3. **Exponential backoff and dead-letter promotion**: `next_retry_at` is currently set to `now + 1 hour` on every failure, regardless of attempt count. A proper retry mechanism should use exponential backoff and eventually promote records to a dead-letter state after N failures, so operators can be alerted and take manual action.

4. **Concurrent retry + poller races**: If a retry worker and the regular poller both attempt to process the same record simultaneously, you get the same race condition the cursor lock was designed to prevent. The retry worker would need to participate in the cursor lock protocol.

5. **Order sensitivity**: Some pollers have implicit ordering requirements. For example, a user must exist before their roles can be synced; a dataset must exist before its import logs can be synced. A retry worker that processes entries in `next_retry_at` order may violate these dependencies.

#### Would a message queue be the industry-standard approach?

A message queue (Celery, RabbitMQ, Redis Streams, etc.) is a common approach to job retry in distributed systems. The general pattern: on failure, enqueue a retry task with the job payload; the queue broker handles delivery, retry scheduling, and dead-lettering. This is well-understood and has strong tooling.

**Queue advantages in this context:**
- Retry scheduling, backoff, and dead-letter handling are handled by the broker, not custom code.
- Visibility into pending retries is available via the broker's UI/API.
- Failed tasks can be inspected, manually replayed, or discarded from the broker UI.
- Celery is already used in this codebase (workers), so the infrastructure exists.

**Queue disadvantages in this context:**
- **The stale-state problem is not solved by a queue**. A queued retry task still carries the risk of applying stale data if the task payload is the original failed data. The task must re-query the source at execution time to get current state — which makes the queue's payload essentially a pointer (record ID), not a data snapshot. At that point, the queue is just a scheduled re-poll of a specific ID, not much different from the existing retry table.
- **Source DB coupling**: The retry task needs a live connection to CMG MongoDB or xenium PostgreSQL to re-fetch current state. In a queued system this adds dependency and failure modes.
- **Ordering guarantees**: Message queues typically provide at-most-once or at-least-once delivery but not ordered delivery across different record types. The dependency ordering problem (user before roles, dataset before import logs) is harder to solve in a queue-based retry than in a sequential poller.
- **Operational overhead**: Introducing a message broker adds infrastructure to operate, monitor, and keep in sync with the rest of the system. For a migration that has a defined end date (when CMG/xenium are retired), this may not be worth it.

**Verdict on queues**: A queue is the right tool if you want robust, visible, instrumentable retry with dead-lettering and backoff. But the **stale-state guard** must still be implemented in the task handler regardless of the queueing mechanism. The queue solves scheduling and observability, not data correctness.

#### Recommendation (for decision-maker)

Choose one of three explicit paths:

1. **Remove the retry infrastructure** entirely (`*_sync_retry` tables, `trackRetry` calls). Accept that transient failures create permanent sync gaps that are only resolved when the source record is next modified. Add an admin notification (see Decision 2) to surface these gaps. This is the simplest and least dangerous option given the stale-state concern.

2. **Keep the retry tables as a diagnostic dead-letter log only** (current de-facto state). Do not implement a retry worker. Rename them to `*_sync_failure_log` to reflect their actual purpose. Add querying / reporting tools (admin notification or dashboard) to surface entries.

3. **Implement targeted retry** with the stale-state guard: a scheduled job re-queries the source DB for each entry in `*_sync_retry` where `next_retry_at <= now()`, checks whether the cursor has already advanced past the record (discards if so), and re-applies only if the source record is still in a state newer than the cursor. Use exponential backoff. This is the most correct option but also the most complex, and the stale-state guard alone is a significant implementation.

---

### Decision 2: Replace per-row failure tracking with admin notifications

#### The proposal

Remove the `*_sync_retry` tables and the `trackRetry` calls from both CMG and xenium pollers entirely. Replace them with a lightweight **admin-only notification** mechanism: when `processRow` fails for a row, instead of writing to a retry table, create a Bioloop notification (using the existing `notification` table) visible to admin users that says something like:

> Sync warning: Failed to apply xenium changes for dataset ID 4721 (xenium_id=182). Error: "Foreign key constraint failed." The dataset may show stale data until the source record is next modified.

#### How this helps

- **Admin visibility**: An admin investigating a data discrepancy in the UI (e.g. a user sees wrong project membership, a dataset shows stale description) currently has no signal pointing to a sync failure. With notifications, they would see a concrete message explaining why the discrepancy exists, which record is affected, and when it occurred.
- **No stale-state risk**: Notifications are informational only. They do not attempt to re-apply any data, so the stale-state problem from Decision 1 is avoided entirely.
- **Simpler operational model**: No retry queue, no scheduled worker, no stale-state guard, no cursor race conditions. The poller remains a simple forward-only cursor scan with per-row error isolation.
- **Actionable for admins**: The admin can see the notification, investigate the specific record in CMG or xenium, and manually correct the Bioloop state if needed — or simply wait for the next source update to re-trigger the poller.

#### Limitations

- **Not self-healing**: Notifications inform but do not fix. For high-volume, transient failures (e.g. 50 rows failed due to a 10-second DB blip), the admin would receive 50 notifications. Notification volume could become noise.
  - Mitigation: batch notifications per poller run (one notification summarizing N failures) rather than one per row.
- **No automatic recovery**: If the admin does nothing and the source record is never modified again, the Bioloop data stays stale permanently. The notification only improves visibility of the gap, not resolution.
- **Admin fatigue**: If sync failures are frequent (e.g. due to a systemic issue like a schema mismatch between xenium fork and cmg-bioloop), the admin notification feed would be overwhelmed. This should trigger investigation and a code fix, but the UX would be poor in the interim.
- **Notification infrastructure coupling**: This approach requires the poller (a `data_sync/` Node.js script) to write to the Bioloop `notification` table directly via Prisma. The poller already has a Prisma connection to the target DB, so this is technically straightforward. But it couples the sync infrastructure to the application's notification domain model, which some may consider a separation-of-concerns concern.

#### Comparison to the retry table approach

| Aspect | `*_sync_retry` table (current) | Admin notification |
|---|---|---|
| Visibility to operators | None (no UI, no alerts) | Admin UI notification feed |
| Self-healing | No (nothing reads the table) | No |
| Stale-state risk | Yes, if retry worker is added | None (notifications are read-only) |
| Implementation complexity | Low (already done) | Low (add notification write in catch block) |
| Operational noise | Silent | Configurable (batch or per-row) |
| Actionability | Requires DB query to see | Direct admin UI visibility |

#### Recommendation (for decision-maker)

If the retry infrastructure is not going to be implemented (Decision 1, option 1 or 2), replacing `trackRetry` with admin notifications is a meaningfully better outcome than the current silent dead-letter table. The notification approach:

1. Costs roughly the same implementation effort as the current `trackRetry` call.
2. Provides direct operator visibility that the retry table never did.
3. Avoids all the stale-state complexity of actual retries.
4. Integrates with the existing Bioloop notification system that admins already use.

The only prerequisite is deciding on batching behavior (per-row vs. per-run summary) to manage notification volume.

---

## Recommended Fix Priority

1. **H1** — Fix `isLegacySourceActive` fallback before any production run. The bug is triggered by a config state that is expected to occur (CMG retirement) and silently corrupts `metadata.origin` for all xenium data.

2. **H2** — Fix the xenium lock asymmetry before first xenium bigbang in production. The fix is a one-line `forceReleaseAllProcessLocks` call inside `clearXeniumTargetRows` (or restructure to mirror CMG's order).

3. **H4** — Test `clearXeniumTargetRows` against production-scale data before relying on it. If it times out, remove the transaction wrapper and pre-delete dependent records explicitly.

4. **M5** — Fix CMG bigbang step counter before production run for log readability.

5. **H3 / H5** — The retry table issues exist in CMG too and were present during battle-testing, so they may be acceptable as-is. However, consider moving `trackRetry` to use `prisma` (outer client) instead of `tx` to at least ensure failure records survive transaction rollbacks.

6. **M1** — Run CMG bigbang first to ensure 'cmguser' gets `origin = 'legacy'`. Or better: assign xenium its own system user ('xeniumuser') as the fallback for xenium audit/import log attribution.

7. **M3** — Add bigbang lock check to pollers (symmetric guard). Low operational risk but a correctness gap.

---

*Analysis performed: 2026-03-13. All code references verified against current HEAD of `data_sync/src/`.*

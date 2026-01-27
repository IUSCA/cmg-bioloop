# Poller Field Update Issues

## Summary

**CRITICAL:** All pollers are updating fields that should NOT be updated after bigbang sync.

---

## Fields That Should NOT Be Updated by Pollers

Based on your requirements, these fields should be populated during bigbang but NOT updated by pollers:

### Table: `dataset`
- `origin_path`
- `metadata`
- `file_type`

### Table: `project`
- `metadata`

### Table: `genome_browser_session`
- `access_count`
- `staging_requested`
- `staging_completed`
- `staging_requested_by`

### Table: `user`
- `metadata`

---

## Current Violations (All Pollers Have Issues)

### ❌ `dataset_activity_poller.js`
**Updates:** `origin_path`, `metadata`

**Lines 68-84:**
```javascript
await tx.dataset.update({
  where: { id: bioloopDataset.id },
  data: {
    origin_path: newOriginPath,  // ❌ Should NOT update
    metadata: {                   // ❌ Should NOT update
      ...existingMetadata,
      cmg_sync_state: { ... },
    },
  },
});
```

**Impact:** Overwrites Bioloop-managed origin_path and metadata

---

### ❌ `dataset_metadata_poller.js`
**Updates:** `description`, `file_type`, `metadata`

**Lines 88-100:**
```javascript
await tx.dataset.update({
  where: { id: bioloopDataset.id },
  data: {
    ...updateData,  // Contains file_type ❌
    metadata: {     // ❌ Should NOT update
      ...existingMetadata,
      cmg_sync_state: { ... },
    },
  },
});
```

**Impact:** Overwrites file_type and metadata

---

### ❌ `project_acl_poller.js`
**Updates:** `metadata`

**Lines 81-93:**
```javascript
await tx.project.update({
  where: { id: bioloopProject.id },
  data: {
    ...updateData,
    metadata: {  // ❌ Should NOT update
      ...existingMetadata,
      cmg_sync_state: { ... },
    },
  },
});
```

**Impact:** Overwrites project metadata

---

### ❌ `project_metadata_poller.js`
**Updates:** `metadata`

**Lines 83-95:**
```javascript
await tx.project.update({
  where: { id: bioloopProject.id },
  data: {
    ...updateData,
    metadata: {  // ❌ Should NOT update
      ...existingMetadata,
      cmg_sync_state: { ... },
    },
  },
});
```

**Impact:** Overwrites project metadata

---

### ❌ `session_metadata_poller.js`
**Updates:** `access_count`, `staging_requested`, `staging_completed`, `staging_requested_by`

**Lines 109-112:**
```javascript
await tx.genome_browser_session.update({
  where: { id: bioloopSession.id },
  data: updateData,  // Contains all staging_* and access_count fields ❌
});
```

**Impact:** Overwrites Bioloop-managed session access counts and staging state

---

### ❌ `user_roles_poller.js`
**Updates:** `metadata`

**Lines 61-73:**
```javascript
await tx.user.update({
  where: { id: bioloopUser.id },
  data: {
    is_deleted: newIsDeleted,
    metadata: {  // ❌ Should NOT update
      ...existingMetadata,
      cmg_sync_state: { ... },
    },
  },
});
```

**Impact:** Overwrites user metadata

---

## Staging Field Usage Analysis

### ✅ `staging_*` fields ARE being used in Bioloop code

**Location:** `api/src/routes/sessions.js`

**Line 1443-1518:** `/sessions/:id/datasets/stage` endpoint
- Reads session tracks to determine which datasets need staging
- Creates staging workflows for datasets
- Returns staging results

**This endpoint uses dataset staging workflows, but does NOT use the `staging_*` fields in `genome_browser_session` table.**

### ❌ `staging_*` fields in `genome_browser_session` table are NOT used

Based on code search:
- Schema defines: `staging_requested`, `staging_completed`, `staging_requested_by`
- Only location: `session_metadata_poller.js` (updates from CMG)
- **NOT used anywhere in Bioloop API or UI code**
- These fields appear to be CMG-specific and not part of Bioloop functionality

### ✅ `access_count` field IS being used

**Location:** `api/src/routes/sessions.js`

**Line 686-690:** Increments access_count when session is accessed
```javascript
await prisma.genome_browser_session.update({
  where: { id },
  data: { access_count: { increment: 1 } },
});
```

**This is Bioloop-managed data, should NOT be synced from CMG.**

---

## Bigbang Behavior Analysis

### Bigbang DOES populate these fields:

**`sync_datasets.js` (line 169-173):**
```javascript
origin_path: cmgItem.paths?.origin || null,  // ✓ Populated
archive_path: cmgItem.paths?.archive || null,
staged_path: null,
is_staged: false,
metadata: null,  // ✓ Set to null (not populated from CMG)
```

**`sync_sessions.js` (line 84):**
```javascript
access_count: cmgSession.access_count || 0,  // ✓ Populated from CMG
```

**Note:** `staging_*` fields are NOT populated in bigbang (not in the insert statement)

---

## Shared Code Between Bigbang and Pollers

### ⚠️ CRITICAL: No shared update methods

- Bigbang uses `prisma.create()` and `prisma.createMany()` - INSERT operations only
- Pollers use `prisma.update()` - UPDATE operations only
- **No shared code that would break if we stop pollers from updating these fields**

**Conclusion:** Safe to remove field updates from pollers without affecting bigbang.

---

## Recommendations

1. **Remove `metadata` updates from ALL pollers** - This field is being used to store `cmg_sync_state` tracking info, but it overwrites any Bioloop-specific metadata

2. **Remove `origin_path` and `file_type` updates from dataset pollers** - These should be immutable after bigbang

3. **Remove ALL fields from `session_metadata_poller.js`** - The `staging_*` fields aren't used in Bioloop, and `access_count` is Bioloop-managed

4. **Consider removing `session_metadata_poller.js` entirely** - None of its fields should be synced from CMG

5. **If sync tracking is needed:**
   - Create a separate `cmg_sync_tracking` table
   - Do NOT use the `metadata` JSON field in entity tables
   - Store: `entity_type`, `entity_id`, `cmg_id`, `last_cmg_updated_at`, `last_sync_time`

---

## Alternative Approach: Sync Tracking

Instead of storing sync state in entity `metadata` fields, create a dedicated tracking table:

```sql
CREATE TABLE cmg_sync_tracking (
  id SERIAL PRIMARY KEY,
  poller_name VARCHAR(50) NOT NULL,
  entity_type VARCHAR(50) NOT NULL,
  entity_id INTEGER NOT NULL,
  cmg_id VARCHAR(100),
  last_cmg_updated_at TIMESTAMP,
  last_sync_time TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE(poller_name, entity_type, entity_id)
);
```

This allows tracking sync progress without polluting entity data.

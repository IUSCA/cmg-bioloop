# CMG to Bioloop Field Mapping Reference

**Purpose:** This document maps CMG MongoDB fields to Bioloop PostgreSQL fields for data sync testing and validation.

**Audience:** AI agents testing the data sync process

**Scope:** Core entities migrated during bigbang sync

---

## 🔑 Key Concepts

### MongoDB Timestamps (CMG)
- `{ timestamps: true }` adds: `createdAt`, `updatedAt` (camelCase, auto-managed)
- **Exception:** `User` collection uses `createDate` (custom field, not auto-managed)

### PostgreSQL Timestamps (Bioloop)
- Snake_case: `created_at`, `updated_at`
- `@default(now())` and `@updatedAt` decorators

### Field Name Patterns
- **CMG:** camelCase (MongoDB convention)
- **Bioloop:** snake_case (PostgreSQL convention)
- **Migration:** Converts camelCase → snake_case automatically

### Provenance Tracking
- All migrated entities have `cmg_id` field in Bioloop (stores MongoDB `_id` as string)
- Used for idempotency and linking back to CMG source

---

## 📊 Entity-by-Entity Field Mapping

### **1. User**

| CMG Field (`users` collection) | Bioloop Field (`user` table) | Notes |
|-------------------------------|------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `username` | `username` | Direct copy |
| `username` | `cas_id` | **Same value** - CMG uses username as CAS ID |
| `fullname` | `name` | Field rename |
| `email` | `email` | Direct copy |
| `createDate` | `created_at` | **Special:** CMG uses `createDate` not `createdAt` |
| `active` | `is_deleted` | **Inverted:** `!active` → `is_deleted` |
| `roles` (Array) | `user_role[]` (join table) | Mapped via `mapCMGRolesToBioloop()` |
| `lastLogin` | - | **NOT migrated** |
| `notifications` | - | **NOT migrated** |
| `prefs` | - | **NOT migrated** |
| `hash`, `salt` | - | **NOT migrated** (Bioloop uses CAS auth) |
| - | `metadata` (JSON) | Bioloop-only field |

**Role Mapping:**
- CMG `admin` → Bioloop `operator`
- CMG `god` → Bioloop `admin`
- CMG `user` → Bioloop `user`
- CMG `guest` → Bioloop `user`

---

### **2. Dataset (RAW_DATA)**

| CMG Field (`datasets` collection) | Bioloop Field (`dataset` table) | Notes |
|-----------------------------------|--------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `name` | `name` | Direct copy |
| - | `type` | **Hardcoded:** `'RAW_DATA'` |
| `description` | `description` | Direct copy |
| `size` | `size` | BigInt conversion |
| `du_size` | `du_size` | BigInt conversion |
| `files` | `num_files` | Field rename |
| `directories` | `num_directories` | Field rename |
| `paths.origin` | `origin_path` | Nested object → flat field |
| `paths.archive` | `archive_path` | Nested object → flat field |
| `paths.staged` | `staged_path` | **Always set to `null`** (not migrated) |
| `staged` (Boolean) | `is_staged` | **Always set to `false`** (not migrated) |
| `createdAt` | `created_at` | Timestamp conversion |
| `updatedAt` | `updated_at` | Timestamp conversion |
| `events[]` | `dataset_audit[]` | Parsed and mapped via `parseCMGEventToAction()` |
| `inspected`, `archived`, `validated`, `converted` | - | **NOT migrated** (use `dataset_state` table) |
| `errored`, `taken`, `takenAt` | - | **NOT migrated** (worker state) |
| `source_node`, `cbcls`, `checksums` | - | **NOT migrated** |

**Special Handling:**
- Empty name → generates `"UNKNOWN"` or `"UNKNOWN-N"`
- Duplicate names → generates `"DUPLICATE-{originalName}-N"`

---

### **3. DataProduct (DATA_PRODUCT)**

| CMG Field (`dataproducts` collection) | Bioloop Field (`dataset` table) | Notes |
|---------------------------------------|--------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `name` | `name` | Direct copy |
| - | `type` | **Hardcoded:** `'DATA_PRODUCT'` |
| `size` | `size` | BigInt conversion, **nullable** |
| `file_type` | `file_type` | Direct copy |
| `file_type` | `analysis_type_id` | **Lookup:** matched against `analysis_type` table |
| `paths.archive` | `archive_path` | Nested object → flat field |
| `paths.staged` | `staged_path` | **Always set to `null`** (not migrated) |
| `staged` (Boolean) | `is_staged` | **Always set to `false`** (not migrated) |
| `genomeType` | `dataset_genomic_attributes.genome_type` | Related table |
| `genomeValue` | `dataset_genomic_attributes.genome_value` | Related table |
| `genome` | `dataset_genomic_attributes.genome_value` | **Fallback** for genome_value |
| `genome_type` | `dataset_genomic_attributes.genome_type` | **Fallback** (usually empty) |
| `dataset` (ObjectId) | `dataset_hierarchy.source_id` | Parent-child relationship |
| `conversion` (ObjectId) | `conversion_derived_dataset` | Link to conversion |
| `upload` (ObjectId) | `dataset_import_log` | Reverse lookup (one-to-many) |
| `createdAt` | `created_at` | Timestamp conversion |
| `updatedAt` | `updated_at` | Timestamp conversion |
| `events[]` | `dataset_audit[]` | Parsed and mapped |
| `files[]`, `groups[]`, `users[]` | - | **NOT migrated** |
| `lastStaged`, `requested`, `errored`, `taken`, `takenAt`, `notify`, `upload_to_s3`, `celery_workflow_id`, `visible`, `disable_archive` | - | **NOT migrated** |

**Important:**
- `genomeType`/`genomeValue` (camelCase) are actively used
- `genome_type`/`genome` (snake_case) are usually empty - used as fallback

---

### **4. Conversion**

| CMG Field (`conversions` collection) | Bioloop Field (`conversion` table) | Notes |
|--------------------------------------|-----------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `user` (ObjectId) | `initiator_id` | Foreign key to `user` table |
| `dataset` (ObjectId) | `dataset_id` | Foreign key to source `dataset` |
| `pipeline` | `definition_id` | **Lookup:** matched against `conversion_definition.name` |
| `options[]` (before "wildcards") | `argument_values[]` | **Parsed:** matched to `argument` definitions |
| `options[]` (after "wildcards") | `additional_args` (JSON) | Ad-hoc CLI flags |
| `samplesheet` | `argument_values[]` | **Special:** stored as `--sample-sheet` argument value |
| `createdAt` | `initiated_at` | Timestamp conversion + field rename |
| `updatedAt` | - | **NOT migrated** |
| - | `workflow_id` | **NOT migrated from CMG** (set by conversion logs sync) |
| `worker`, `staged`, `output_path`, `status` | - | **NOT migrated** |

**Option Array Parsing:**
- Format: `["no-lane-splitting", "barcode-mismatches 1", "wildcards", "--custom-flag", "value"]`
- Before "wildcards": predefined args → `argument_values` table
- After "wildcards": additional args → `additional_args` JSON
- Adds `--` prefix if missing
- Handles space-separated values: `"key value"` → `{argument_name: "--key", value: "value"}`

**Sample Sheet:**
- CMG stores as string in conversion document
- Bioloop stores in `argument_value` linked to `--sample-sheet` argument
- Full CSV content preserved

---

### **5. Project**

| CMG Field (`projects` collection) | Bioloop Field (`project` table) | Notes |
|-----------------------------------|--------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `name` | `name` | Direct copy |
| `name` | `slug` | **Derived:** normalized version of name |
| `description` | `description` | Direct copy |
| `browser` | `browser_enabled` | Field rename |
| `dataproducts[]` | `project_dataset[]` | Join table linking to datasets |
| `users[]` | `project_user[]` | Join table linking to users |
| `groups[]` | - | **Expanded:** groups resolved to users → `project_user[]` |
| `createdAt` | `created_at` | Timestamp conversion |
| `updatedAt` | `updated_at` | Timestamp conversion |
| `size` | - | **NOT migrated** (aggregated field) |
| - | `funding` | Bioloop-only field |
| - | `metadata` (JSON) | Bioloop-only field |

**Group Expansion:**
- CMG groups are expanded to individual users during migration
- All group members added to `project_user` join table

---

### **6. Genome Browser Session**

| CMG Field (`sessions` collection) | Bioloop Field (`genome_browser_session` table) | Notes |
|----------------------------------|----------------------------------------------|-------|
| `_id` (ObjectId) | `cmg_id` (String) | Provenance tracking |
| `user` (ObjectId) | `user_id` | Foreign key to `user` table |
| `title` | `title` | Direct copy |
| `genome` | `genome` | Direct copy |
| `genome_type` | `genome_type` | Direct copy |
| `access_count` | `access_count` | Direct copy |
| `createdAt` | `created_at` | Timestamp conversion |
| `updatedAt` | `updated_at` | Auto-managed by Bioloop |
| `tracks[]` | - | **NOT migrated** (users create tracks in Bioloop UI) |
| `tracks[]` | `metadata.datasets` | **Track info:** CMG dataproduct IDs stored for reference |
| `internal_share[]`, `external_share[]` | - | **NOT migrated** |
| `staging.*` | - | **NOT migrated** (CMG-specific fields) |
| - | `is_public` | Bioloop-only field |
| - | `metadata` (JSON) | Bioloop-only field |

**Migration Filter:**
- Only sessions with non-empty `tracks[]` array are migrated
- Empty sessions are skipped (not useful in Bioloop)

---

### **7. Upload → Import Log**

| CMG Field (`uploads` collection) | Bioloop Field (`dataset_import_log` table) | Notes |
|----------------------------------|-------------------------------------------|-------|
| `_id` (ObjectId) | - | Stored in `metadata.cmg_upload_id` |
| `user` (ObjectId) | `user_id` | Foreign key to `user` table |
| `dataset` (ObjectId) | - | Stored in `metadata.cmg_source_dataset` |
| `dataproduct` (ObjectId) | - | Stored in `metadata.cmg_source_dataproduct` |
| **Reverse lookup:** | `dataset_id` | **Find dataproduct where `upload == cmgUpload._id`** |
| `createdAt` | `timestamp` | Timestamp conversion + field rename |
| `file_type` | `file_type` | Direct copy (stored in metadata) |
| `genomeType` | `genome_type` | Direct copy (stored in metadata) |
| `genomeValue` | `genome_value` | Direct copy (stored in metadata) |
| `path`, `notes`, `worker`, `status`, `selected`, `file_count`, `projects`, `new_project`, `groups` | `metadata` | Stored in JSON (filtered, no ObjectIds) |
| - | `action` | **Hardcoded:** `'created'` |
| - | `create_method` | **Hardcoded:** `'IMPORT'` |
| - | `source_run` | **Derived:** from import log state |

**Key Insight:**
- CMG uploads **create** new dataproducts (not reference existing ones)
- Migration uses reverse lookup: find all dataproducts where `dataproduct.upload == upload._id`
- One upload can create multiple dataproducts → multiple import log entries

---

## 🔄 Field Transformations

### **Type Conversions**

| CMG Type | Bioloop Type | Conversion |
|----------|-------------|------------|
| ObjectId | String | `.toString()` |
| Number | BigInt | `BigInt(value)` |
| Boolean | Boolean | Direct (sometimes inverted) |
| Date | DateTime | Direct (timezone preserved) |
| Array | JSON | Some arrays → JSON field, others → join tables |
| Mixed/Object | JSON | Stored in `metadata` or `additional_args` |

### **Naming Conventions**

| Pattern | Example CMG | Example Bioloop |
|---------|-------------|----------------|
| Nested objects | `paths.archive` | `archive_path` (flattened) |
| Boolean flags | `staged` | `is_staged` |
| Counts | `files` | `num_files` |
| Foreign keys | `user` | `user_id` |
| Arrays | `roles[]` | `user_role[]` (join table) |

---

## ⚠️ Special Cases & Gotchas

### **1. Staging State NOT Migrated**

**Why:** Bioloop workflows manage staging lifecycle independently
```
CMG: paths.staged = "/path/to/staged"  ❌ NOT copied
CMG: staged = true                     ❌ NOT copied
     ↓
Bioloop: staged_path = null            ✓ Always null after bigbang
Bioloop: is_staged = false             ✓ Always false after bigbang
```

**Rationale:** Bioloop's `stage` workflow sets these fields based on actual operations

---

### **2. Size Fields NOT Overwritten by Pollers**

**Bigbang:** Sets initial values from CMG
```
dataset.size: cmgItem.size → BigInt
dataset.du_size: cmgItem.du_size → BigInt (RAW_DATA only)
dataset.num_files: cmgItem.files → Integer (RAW_DATA only)
dataset.num_directories: cmgItem.directories → Integer
```

**Pollers:** Do NOT update these fields
- `dataset_metadata_poller` only updates `description` and `file_type`
- Size/count fields are computed by Bioloop's `inspect_dataset` worker

---

### **3. Genomic Attributes Field Name Variations**

CMG has **inconsistent** field names across collections:

**Dataproducts (actively used):**
- `genomeType` (camelCase) ✓
- `genomeValue` (camelCase) ✓

**Dataproducts (fallback, usually empty):**
- `genome_type` (snake_case)
- `genome` (different field)

**Migration uses `extractGenomicAttributes()` helper:**
```javascript
// Checks all variations in priority order:
genome_type: cmgDoc.genome_type || cmgDoc.genomeType || null
genome_value: cmgDoc.genome_value || cmgDoc.genomeValue || cmgDoc.genome || null
```

**Bioloop Storage:**
```
dataset_genomic_attributes table:
- dataset_id (FK)
- genome_type (String)
- genome_value (String)
```

---

### **4. Conversion Arguments Three-Way Split**

CMG stores all arguments in single array: `conversion.options[]`

**Bioloop uses three storage locations:**

```
CMG: options: ["no-lane-splitting", "barcode-mismatches 1", "wildcards", "--custom-flag", "value"]
                     ↓                       ↓                    ↓              ↓
Bioloop:
1. argument_values table:
   - "--no-lane-splitting" (linked to argument definition)
   - "--barcode-mismatches" with value "1" (linked to argument definition)

2. additional_args JSON:
   - [{"argument_name": "--custom-flag", "value": "value"}]

3. argument_values table (separate):
   - "--sample-sheet" with CSV content (from conversion.samplesheet field)
```

**Parsing Rules:**
- Everything before "wildcards" → predefined args → `argument_values`
- Everything after "wildcards" → additional args → `additional_args` JSON
- Sample sheet from separate field → `argument_values`

---

### **5. Project ACL Expansion**

CMG stores users AND groups:
```
project.users: [ObjectId1, ObjectId2]
project.groups: [GroupObjectId]
    ↓
Group.users: [ObjectId3, ObjectId4, ObjectId5]
```

Bioloop flattens to users only:
```
project_user records:
- {project_id, user_id: User1}
- {project_id, user_id: User2}
- {project_id, user_id: User3}  ← From group
- {project_id, user_id: User4}  ← From group
- {project_id, user_id: User5}  ← From group
```

**Testing:** Verify group members are properly associated with projects

---

### **6. Dataset Audit Logs (Event Deduplication)**

CMG stores all workflow lifecycle events:
```
dataset.events: [
  { stamp: Date, description: "Stage - start" },
  { stamp: Date, description: "Stage - finish" }
]
```

Bioloop consolidates to completion events only:
```
dataset_audit record:
- action: "staged"
- timestamp: finish event timestamp
```

**Mapping via `parseCMGEventToAction()`:**
- "Stage - finish" → `action: 'staged'`
- "Archive - finish" → `action: 'archived'`
- Start events are **discarded**

---

## 🎯 Testing Scenarios

### **Scenario 1: Verify User Migration**

```javascript
// CMG Query
db.users.findOne({ username: "testuser" })

// Expected Bioloop Record
{
  username: "testuser",
  cas_id: "testuser",     // Same as username
  name: cmg.fullname,
  email: cmg.email,
  is_deleted: !cmg.active, // Inverted
  created_at: cmg.createDate, // Note: createDate, not createdAt
  cmg_id: cmg._id.toString()
}
```

### **Scenario 2: Verify RAW_DATA Dataset**

```javascript
// CMG Query
db.datasets.findOne({ name: "ILMN_123_Sample" })

// Expected Bioloop Record
{
  name: "ILMN_123_Sample",
  type: "RAW_DATA",
  size: BigInt(cmg.size),
  du_size: BigInt(cmg.du_size),
  num_files: cmg.files,
  num_directories: cmg.directories,
  origin_path: cmg.paths.origin,
  archive_path: cmg.paths.archive,
  staged_path: null,        // Always null
  is_staged: false,         // Always false
  cmg_id: cmg._id.toString()
}
```

### **Scenario 3: Verify DATA_PRODUCT with Genomic Attributes**

```javascript
// CMG Query
db.dataproducts.findOne({ name: "Sample_BAM" })

// Expected Bioloop Records
dataset record:
{
  name: "Sample_BAM",
  type: "DATA_PRODUCT",
  size: BigInt(cmg.size) or null,
  file_type: cmg.file_type,
  analysis_type_id: <lookup by file_type>,
  cmg_id: cmg._id.toString()
}

dataset_genomic_attributes record (if genomeType/genomeValue exist):
{
  dataset_id: <dataset.id>,
  genome_type: cmg.genomeType,    // camelCase, NOT genome_type
  genome_value: cmg.genomeValue   // camelCase, NOT genome
}
```

### **Scenario 4: Verify Conversion with Arguments**

```javascript
// CMG Query
db.conversions.findOne({ pipeline: "bcl2fastq" })

// Expected Bioloop Records
conversion record:
{
  definition_id: <lookup by pipeline name>,
  dataset_id: <lookup by CMG dataset ObjectId>,
  initiator_id: <lookup by CMG user ObjectId>,
  initiated_at: cmg.createdAt,
  additional_args: [...ad-hoc args after "wildcards"],
  cmg_id: cmg._id.toString()
}

argument_value records:
- One per predefined arg (before "wildcards")
- One for sample sheet (from cmg.samplesheet field)

conversion_derived_dataset records:
- Links to all dataproducts where dataproduct.conversion == cmg._id
```

### **Scenario 5: Verify Project with Group Expansion**

```javascript
// CMG Query
cmgProject = db.projects.findOne({ name: "Test Project" })
cmgGroup = db.groups.findOne({ _id: cmgProject.groups[0] })

// Expected Bioloop project_user Records
For each user in cmgProject.users:
  - project_user record

For each group in cmgProject.groups:
  For each user in group.users:
    - project_user record

// Total: direct users + all group members
```

---

## 📝 Fields That Don't Migrate

### **Completely Excluded (All Entities)**

- Worker state: `taken`, `takenAt`, `worker`
- Boolean flags: `inspected`, `archived`, `validated`, `converted`, `requested`
- Error tracking: `errored`
- Notification preferences: `notify`, `notifications`
- Authentication: `hash`, `salt`, `lastLogin`

**Why:** These are runtime/workflow state in CMG. Bioloop uses:
- `workflow` table for workflow tracking
- `dataset_state` table for lifecycle states
- CAS authentication (no passwords)

---

## 🔍 Validation Queries

### **Check User Count Match**

```sql
-- CMG
db.users.count({ active: true })

-- Bioloop
SELECT COUNT(*) FROM "user" WHERE is_deleted = false;
```

### **Check Dataset Count by Type**

```sql
-- CMG
db.datasets.count()
db.dataproducts.count()

-- Bioloop
SELECT type, COUNT(*) FROM dataset GROUP BY type;
-- Expected: RAW_DATA count ≈ CMG datasets
--           DATA_PRODUCT count ≈ CMG dataproducts
```

### **Check Genomic Attributes**

```sql
-- CMG (dataproducts with genomic attributes)
db.dataproducts.count({ $or: [
  { genomeType: { $exists: true, $ne: null } },
  { genomeValue: { $exists: true, $ne: null } }
]})

-- Bioloop
SELECT COUNT(*) FROM dataset_genomic_attributes;
-- Should be similar (allowing for fallback to genome_type/genome)
```

### **Check Conversion Arguments**

```sql
-- Bioloop conversion with arguments
SELECT 
  c.id,
  c.cmg_id,
  COUNT(av.id) as predefined_args_count,
  CASE WHEN c.additional_args IS NOT NULL 
    THEN jsonb_array_length(c.additional_args) 
    ELSE 0 
  END as additional_args_count
FROM conversion c
LEFT JOIN argument_value av ON av.conversion_id = c.id
WHERE c.cmg_id IS NOT NULL
GROUP BY c.id;
```

---

## 📚 Reference Files

### **CMG Schema Location**
`data_sync/src/legacy_app_schema/*.js` - Mongoose schemas

### **Bioloop Schema Location**
`api/prisma/schema.prisma` - Prisma schema

### **Migration Code Location**
`data_sync/src/sync/bigbang/*.js` - Sync modules

### **Key Utilities**
- `extractGenomicAttributes()` - `data_sync/src/sync/utils/cmg_helpers.js`
- `mapCMGRolesToBioloop()` - `data_sync/src/sync/utils/role_mapper.js`
- `parseCMGEventToAction()` - `data_sync/src/sync/utils/event_parser.js`
- `parseOptionsArray()` - `data_sync/src/sync/bigbang/sync_conversions.js`

---

**Last Updated:** 2026-01-27

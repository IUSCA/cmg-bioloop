# Current CMG-to-Bioloop Migration Process: Dependency Analysis
**Date:** 2026-01-03  
**Purpose:** Document the current Python migration process to inform the new JavaScript-based sync strategy

---

## Overview

The current migration process (`db_conversion/src/convert/scripts/convert.py`) performs a **full database rebuild** from CMG MongoDB to Bioloop PostgreSQL. Understanding its dependencies and order of operations is critical for designing the incremental sync process.

---

## Execution Order & Dependencies

### Phase 1: Database Cleanup & Schema Recreation

```python
# 1. Drop existing Bioloop structures
drop_bioloop_enums(pg_cursor)
drop_bioloop_tables(pg_cursor)
drop_bioloop_workflow_documents(rhythm_db)  # Clear rhythm MongoDB workflows

# 2. Recreate schema
create_bioloop_enums(pg_cursor)
create_bioloop_tables(pg_cursor)
create_bioloop_workflow_documents(rhythm_db)  # Recreate workflow structures
```

**Key Points:**
- Tables must be dropped in reverse dependency order (child tables first)
- Enums must be dropped before tables that use them
- Rhythm workflow documents (in separate MongoDB) are also cleared

---

### Phase 2: Data Conversion Sequence

The conversion follows a strict dependency order:

```python
# Order matters! Each step depends on previous steps
create_roles(pg_cursor)                           # 1. Roles first
convert_users(pg_cursor, mongo_db)                # 2. Users (reference roles)
convert_all_datasets(pg_cursor, mongo_db)         # 3. Datasets
events_to_audit_logs(pg_cursor, mongo_db)         # 4. Audit logs (reference datasets)
convert_dataset_hierarchies(pg_cursor, mongo_db)  # 5. Hierarchies (reference datasets)
convert_projects(pg_cursor, mongo_db)             # 6. Projects
convert_conversions(pg_cursor, mongo_db)          # 7. Conversions
convert_sessions(pg_cursor, mongo_db)             # 8. Sessions (genome browser)
# create_workflows_for_past_stagings(...)        # 9. Workflows (COMMENTED OUT)
```

---

## Step-by-Step Dependency Analysis

### Step 1: create_roles()

**Dependencies:** None (always first)

**Purpose:** Create hardcoded Bioloop roles

**Source:** `db_conversion/src/convert/constants/bioloop.py`

```python
bioloop_roles = [
  {'name': 'user', 'description': 'Basic user'},
  {'name': 'operator', 'description': 'Can manage datasets'},
  {'name': 'admin', 'description': 'Full system access'}
]
```

**Bioloop Schema:**
```sql
CREATE TABLE role (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50),
  description VARCHAR(255)
);
```

**Critical for:** All user role assignments

---

### Step 2: convert_users()

**Dependencies:** `role` table must exist

**CMG Source:** `users` collection

**Special Additions:**
- Adds a synthetic `cmguser` user (from constants)
- Maps CMG roles to Bioloop roles via `role_mapping`

**Role Mapping:**
```python
role_mapping = {
  'admin': 'operator',      # CMG admin → Bioloop operator
  'god': 'admin',          # CMG god → Bioloop admin
  'user': 'user',          # CMG user → Bioloop user
  'guest': 'user'          # CMG guest → Bioloop user
}
```

**Process:**
1. Insert user into `user` table with `cmg_id` tracking
2. Look up role IDs from `role` table
3. Create `user_role` associations for each user

**Bioloop Tables:**
- `user` (username, email, name, cas_id, cmg_id)
- `user_role` (user_id → role_id)

**Critical for:** All subsequent operations that reference users (projects, audit logs, etc.)

---

### Step 3: convert_all_datasets()

**Dependencies:** `user` table (for audit logs, ownership)

**CMG Sources:**
- `datasets` collection → `RAW_DATA` type
- `dataproducts` collection → `DATA_PRODUCT` type

**Process:**
1. Convert CMG `datasets` to Bioloop `dataset` with type='RAW_DATA'
2. Convert CMG `dataproducts` to Bioloop `dataset` with type='DATA_PRODUCT'
3. For each dataproduct, insert `dataset_genomic_attributes` (genome_type, genome_value)

**Duplicate Handling:**
- CMG allows duplicate names; Bioloop has UNIQUE constraint on (name, type, is_deleted)
- Solution: Prefix duplicates with `DUPLICATE_`, `DUPLICATE_2_`, etc.
- CMG datasets without names get `UNKNOWN`, `UNKNOWN-2`, etc.

**Bioloop Tables:**
- `dataset` (name, type, cmg_id, size, paths, is_staged, etc.)
- `dataset_genomic_attributes` (dataset_id, genome_type, genome_value)

**Critical for:** Projects, conversions, hierarchies, audit logs

---

### Step 4: events_to_audit_logs()

**Dependencies:** `dataset` and `user` tables

**CMG Source:** Embedded `events` arrays within `datasets` and `dataproducts`

**CMG Schema:**
```javascript
{
  events: [
    {
      stamp: ISODate("2021-10-05T19:39:40.126Z"),
      description: "stage - start"
    },
    {
      stamp: ISODate("2021-10-05T19:45:10.300Z"),
      description: "stage - finish"
    }
  ]
}
```

**Process:**
1. For each dataset/dataproduct in CMG
2. Extract `events` array
3. Create `dataset_audit` records with action=description, timestamp=stamp

**Bioloop Table:**
- `dataset_audit` (dataset_id, action, timestamp, user_id, old_data, new_data)

**Note:** Current process doesn't map `user_id` from events; could be enhanced

---

### Step 5: convert_dataset_hierarchies()

**Dependencies:** `dataset` table (both source and derived datasets must exist)

**CMG Logic:** `dataproducts.dataset` field references parent `datasets._id`

**Process:**
1. For each CMG dataproduct with a `dataset` field
2. Find corresponding Bioloop DATA_PRODUCT (via cmg_id)
3. Find corresponding Bioloop RAW_DATA (via dataset field → cmg_id lookup)
4. Create `dataset_hierarchy` record: source_id=RAW_DATA, derived_id=DATA_PRODUCT

**Bioloop Table:**
- `dataset_hierarchy` (source_id, derived_id, assigned_at)
- Primary key: (source_id, derived_id)

**Represents:** "This DATA_PRODUCT was derived from this RAW_DATA"

---

### Step 6: convert_projects()

**Dependencies:** `dataset` and `user` tables

**CMG Source:** `projects` collection

**CMG Schema:**
```javascript
{
  _id: ObjectId("..."),
  name: "My Project",
  description: "Project description",
  browser: true,
  dataproducts: [ObjectId("..."), ObjectId("...")],  // Array of dataproduct IDs
  users: [ObjectId("..."), ObjectId("...")],         // Array of user IDs
  groups: [ObjectId("...")]                          // Array of group IDs
}
```

**Group Expansion:**
- CMG has `groups` which contain `users` arrays
- Process expands groups to individual users
- Bioloop has no groups concept; all users are directly associated

**Process:**
1. Insert projects into `project` table (with generated UUID and slug)
2. For each project's `dataproducts` array:
   - Find corresponding Bioloop dataset
   - Create `project_dataset` association
3. For each project's `users` + expanded group users:
   - Find corresponding Bioloop user
   - Create `project_user` association

**Bioloop Tables:**
- `project` (id=UUID, name, slug, description, browser_enabled, cmg_id)
- `project_dataset` (project_id, dataset_id)
- `project_user` (project_id, user_id)

**Slug Generation:**
```python
def generate_slug(name, cmg_id):
    # name → lowercase → replace spaces with hyphens → remove non-alphanumeric
    slug = name.lower().replace(' ', '-')
    slug = ''.join(c for c in slug if c.isalnum() or c == '-')
    # Ensure uniqueness by appending part of cmg_id if needed
    return slug[:50]
```

---

### Step 7: convert_conversions()

**Dependencies:** `conversion_definition`, `cmd_line_program`, `argument`, `dataset`, `user` tables

**CMG Source:** `conversions` collection

**Pre-Requisite: Pipeline Definitions**

Before converting conversions, the process populates pipeline infrastructure:

```python
def _populate_pipeline_definitions(pg_cursor):
    # Insert cmd_line_programs (bcl2fastq, cellranger, etc.)
    for program in CMD_LINE_PROGRAMS:
        INSERT INTO cmd_line_program (name, executable_path, ...)
    
    # Insert conversion_definitions
    for definition in CONVERSION_DEFINITIONS:
        INSERT INTO conversion_definition (name, description, program_id, ...)
    
    # Insert arguments
    for arg in ARGUMENT_DATA:
        INSERT INTO argument (name, value_type, program_id, ...)
```

**Constants Source:** `db_conversion/src/convert/constants/common.py`

**CMD_LINE_PROGRAMS:**
- bcl2fastq, bcl-convert, cellranger (multiple versions), spaceranger, etc.
- Each has `executable_path` and `allow_additional_args` flag

**CONVERSION_DEFINITIONS:**
- Match CMD_LINE_PROGRAMS by name
- Define which `dataset_types` they can run on (e.g., ['RAW_DATA'])
- Link to `program_id`

**ARGUMENT_DATA:**
- Arguments like `--no-lane-splitting`, `--barcode-mismatches`, etc.
- Link to `program_id`
- bcl2fastq gets all arguments
- Other programs share some arguments

**Conversion Process:**
1. Call `_populate_pipeline_definitions()` to seed conversion infrastructure
2. For each CMG conversion:
   - Map `pipeline` field to `conversion_definition` by name
   - Map `dataset` field to Bioloop dataset (source RAW_DATA)
   - Map `user` field to Bioloop user (initiator)
   - Create `conversion` record
3. For each CMG dataproduct with `conversion` field:
   - Find corresponding Bioloop conversion
   - Find corresponding Bioloop dataset (derived DATA_PRODUCT)
   - Create `conversion_derived_dataset` association

**Bioloop Tables:**
- `cmd_line_program` (name, executable_path, allow_additional_args)
- `conversion_definition` (name, description, program_id, dataset_types, enabled)
- `argument` (name, value_type, program_id, is_required, default_value)
- `conversion` (cmg_id, definition_id, dataset_id, initiator_id, initiated_at)
- `conversion_derived_dataset` (conversion_id, dataset_id)

---

### Step 8: convert_sessions()

**Dependencies:** `user`, `dataset` tables

**CMG Source:** `sessions` collection (genome browser sessions)

**Note:** Implementation details not fully visible in current code, but follows similar pattern

---

### Step 9: create_workflows_for_past_stagings() [COMMENTED OUT]

**Dependencies:** `dataset` table, Rhythm MongoDB

**Purpose:** Recreate workflow history for past stagings

**Rhythm Integration:**
- Bioloop stores workflow ID in PostgreSQL `workflow` table
- Actual workflow metadata stored in Rhythm MongoDB (`workflow_meta` collection)
- Individual task results stored in Rhythm MongoDB (`celery_taskmeta` collection)

**Process (when enabled):**
1. For each CMG dataproduct:
   - Create `workflow_meta` document in Rhythm MongoDB
   - Create `celery_taskmeta` documents for each workflow step (stage, validate, setup_download)
   - Link workflow to dataset in Bioloop PostgreSQL
2. Extract timestamps from CMG dataproduct `events` array
3. Mark all steps as SUCCESS retroactively

**Why Commented Out:** 
- Likely too time-consuming or not needed for initial sync
- Workflows for future operations will be created by Bioloop's live workflow system

---

## Key Reusable Values & Patterns

### 1. cmg_id as Anchor

**Every Bioloop table** has a `cmg_id` field storing CMG's MongoDB `_id`:

```python
cmg_id = str(cmg_document['_id'])  # Convert ObjectId to string

# Later, find existing record:
existing = prisma.user.findFirst(where={'cmg_id': cmg_id})
```

**Critical for incremental sync:** This enables idempotent upsert operations

---

### 2. Role Mapping

**Must be applied consistently:**

```python
role_mapping = {
    'admin': 'operator',
    'god': 'admin',
    'user': 'user',
    'guest': 'user'
}
```

**Process:**
1. Get CMG user's `roles` array
2. Map each to Bioloop role name
3. Look up Bioloop role ID
4. Create `user_role` associations

---

### 3. Duplicate Name Handling

**Bioloop constraint:** `UNIQUE (name, type, is_deleted)`

**Solution:**
```python
def handle_duplicate_name(original_name, dataset_type, is_deleted):
    new_name = f"DUPLICATE_{original_name}"
    count = 1
    while name_exists(new_name, dataset_type, is_deleted):
        count += 1
        new_name = f"DUPLICATE_{count}_{original_name}"
    return new_name
```

**For incremental sync:** Use same logic to avoid conflicts

---

### 4. Group Expansion

**CMG has groups; Bioloop doesn't:**

```python
def get_cmg_users_from_project_groups(mongo_db, cmg_group_ids):
    users = set()
    for group in mongo_db.groups.find({'_id': {'$in': cmg_group_ids}}):
        users.update(group.get('users', []))
    return list(users)

# Combine direct users + group users
all_project_users = cmg_project['users'] + get_cmg_users_from_project_groups(...)
```

**For incremental sync:** Must expand groups every time

---

### 5. Batch Insertions

**Current process uses batch inserts for performance:**

```python
# Collect all associations
associations = []
for item in items:
    associations.append((project_id, dataset_id))

# Single batch insert
pg_cursor.executemany(
    "INSERT INTO project_dataset (project_id, dataset_id) VALUES (%s, %s)",
    associations
)
```

**For incremental sync:** May not be as critical (fewer changes per run)

---

### 6. Genomic Attributes Extraction

**For DATA_PRODUCT datasets:**

```python
genome_type = cmg_dataproduct.get('genome_type') or cmg_dataproduct.get('genomeType')
genome_value = cmg_dataproduct.get('genome_value') or cmg_dataproduct.get('genomeValue') or cmg_dataproduct.get('genome')

if genome_type or genome_value:
    INSERT INTO dataset_genomic_attributes (dataset_id, genome_type, genome_value)
```

**Note:** CMG has inconsistent field names; must check multiple variants

---

### 7. Path Extraction

**CMG nested paths:**

```python
origin_path = cmg_dataset.get('paths', {}).get('origin', None)
archive_path = cmg_dataset.get('paths', {}).get('archive', None)
staged_path = cmg_dataset.get('paths', {}).get('staged', None)
```

**For incremental sync:** Check for `paths` object existence

---

### 8. Conversion Definition Lookup

**Must match by name:**

```python
pipeline_name = cmg_conversion['pipeline']  # e.g., "bcl2fastq"

definition = prisma.conversion_definition.findFirst(
    where={'name': pipeline_name}
)

if not definition:
    logger.warn(f"No definition for pipeline: {pipeline_name}")
    continue
```

**For incremental sync:** Skip conversions with unknown pipelines

---

## Special Considerations for Incremental Sync

### 1. Pipeline Definitions (One-Time Seeding)

**Don't sync these every time:**
- `cmd_line_program`
- `conversion_definition`
- `argument`

**Reason:** These are hardcoded constants, not CMG data

**Strategy:** 
- Populate once during initial setup
- Only update if constants change (rare)

---

### 2. Audit Logs (Append-Only)

**Don't delete old audit logs**

**Strategy:**
- Query CMG for new events since last sync
- Compare with existing Bioloop `dataset_audit` records
- Insert only new events

**Challenge:** CMG events are embedded in dataset/dataproduct documents, not timestamped at collection level

**Solution:** 
- When a dataset is updated, re-check its `events` array
- Use event timestamp to determine if already synced

---

### 3. Project Associations (Diff-Based)

**Project users/datasets can be added or removed**

**Strategy:**
1. Get current associations from CMG
2. Get existing associations from Bioloop
3. Calculate diff:
   - Add new associations
   - Remove deleted associations

**Example:**
```javascript
const cmgUsers = new Set(cmgProject.users.map(id => id.toString()));
const bioloopUsers = await prisma.project_user.findMany({
    where: { project_id: bioloopProjectId }
});

// Add new users
for (const cmgUserId of cmgUsers) {
    const user = await prisma.user.findFirst({ where: { cmg_id: cmgUserId } });
    if (!bioloopUsers.some(pu => pu.user_id === user.id)) {
        await prisma.project_user.create({ /* ... */ });
    }
}

// Remove deleted users
for (const bioloopUser of bioloopUsers) {
    const user = await prisma.user.findUnique({ where: { id: bioloopUser.user_id } });
    if (!cmgUsers.has(user.cmg_id)) {
        await prisma.project_user.delete({ /* ... */ });
    }
}
```

---

### 4. Dataset Hierarchies (Stable Once Created)

**Once a DATA_PRODUCT is linked to a RAW_DATA, it doesn't change**

**Strategy:**
- Use upsert (idempotent)
- Composite key prevents duplicates: `@@id([source_id, derived_id])`

---

### 5. Conversion Derived Datasets (Append-Only)

**Once created, associations don't change**

**Strategy:**
- Use upsert (idempotent)
- Composite key prevents duplicates: `@@id([conversion_id, dataset_id])`

---

## Immutable vs Mutable Fields

### Fields That Should NOT Change After Initial Creation

**User:**
- `id` (Bioloop-generated)
- `cmg_id`
- `created_at`

**Dataset:**
- `id` (Bioloop-generated)
- `cmg_id`
- `type` (raw_data vs data_product)
- `created_at`

**Project:**
- `id` (Bioloop UUID)
- `slug` (changing breaks URLs)
- `cmg_id`
- `created_at`

**Conversion:**
- `id` (Bioloop-generated)
- `cmg_id`
- `definition_id` (pipeline doesn't change)
- `dataset_id` (source dataset doesn't change)
- `initiated_at`

### Fields That CAN Change (Update on Every Sync)

**User:**
- `username`, `name`, `email`
- `is_deleted` (maps from `active` field)
- `updated_at`
- `user_role` associations

**Dataset:**
- `is_staged`, `staged_path`
- `archive_path`
- `size`, `du_size`
- `description`
- `updated_at`

**Project:**
- `name`, `description`
- `browser_enabled`
- `updated_at`
- `project_user`, `project_dataset` associations

**Conversion:**
- (Immutable once created)

---

## Summary: Critical Takeaways for New Sync Process

1. **Strict Dependency Order:**
   - Roles → Users → Datasets → Everything Else
   - Can't create projects without users and datasets
   - Can't create conversions without pipeline definitions

2. **cmg_id is the Anchor:**
   - Every lookup uses `cmg_id` to find existing records
   - Enables idempotent upserts

3. **Role Mapping Must Be Consistent:**
   - CMG roles != Bioloop roles
   - Use same mapping every time

4. **Pipeline Definitions are Constants:**
   - Seed once, not every sync
   - Source: `constants/common.py`, NOT CMG MongoDB

5. **Groups Must Be Expanded:**
   - CMG groups → individual user IDs
   - Bioloop has no groups

6. **Duplicate Handling:**
   - CMG allows duplicate names; Bioloop doesn't
   - Use consistent prefixing strategy

7. **Audit Logs are Append-Only:**
   - Extract from embedded `events` arrays
   - Only add new events, don't delete old ones

8. **Project Associations are Diff-Based:**
   - Calculate additions and removals
   - Don't just overwrite

9. **Some Tables are Append-Only:**
   - `dataset_hierarchy`, `conversion_derived_dataset`
   - Use upsert for idempotency

10. **Workflow Creation is Optional:**
    - Currently commented out in migration
    - Can be deferred or handled separately

---

**Next:** See `BIOLOOP_WORKFLOW_ARCHITECTURE.md` for understanding how Bioloop uses workflows for staging/archival/conversion operations


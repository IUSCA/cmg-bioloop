# CMG-to-Bioloop Database Synchronization Strategy
**Version:** 1.0  
**Date:** 2026-01-03  
**For:** AI Agents implementing database sync between CMG (MongoDB) and Bioloop (PostgreSQL)

---

## Table of Contents
1. [Overview & Constraints](#overview--constraints)
2. [Synchronization Strategy](#synchronization-strategy)
3. [Initial Population Process](#initial-population-process)
4. [Incremental Update Process](#incremental-update-process)
5. [Business Object Mapping](#business-object-mapping)
6. [Change Detection Strategy](#change-detection-strategy)
7. [Implementation Guide](#implementation-guide)
8. [Workflow Event Detection](#workflow-event-detection)
9. [Error Handling & Recovery](#error-handling--recovery)
10. [Execution Schedule](#execution-schedule)

---

## 1. Overview & Constraints

### Project Context
- **CMG**: Legacy system using MongoDB 4.0.28 for genomic data management
- **Bioloop**: New system using PostgreSQL with Prisma ORM
- **Goal**: Keep Bioloop's data synchronized with CMG until CMG is retired
- **Environment**: Low-traffic (~200 CMG users, ~4 Bioloop users initially)

### Critical Constraints
1. ✅ **NO ID regeneration**: Bioloop IDs must remain stable across sync runs
2. ✅ **NO CMG source code modification**: Cannot modify CMG's API or workers
3. ✅ **Use Prisma/JavaScript only**: No Python for sync logic (except initial migration)
4. ✅ **Quick implementation**: Prioritize working solution over perfection
5. ✅ **Iterative improvement**: Design for incremental enhancements
6. ✅ **Acceptable delay**: Up to a few minutes lag is tolerable
7. ❌ **NO workflow table sync**: Exclude for now (handled by Bioloop's workflow microservice)
8. ❌ **NO dataset_file population**: Too large; handle separately later

### Key Innovation: cmg_id Field
Every Bioloop table has a `cmg_id` field (String) that stores the CMG MongoDB `_id`. This is the **anchor** for all synchronization logic.

---

## 2. Synchronization Strategy

### Approach: Incremental Sync with Polling

**Initial Population** (One-time):
1. Drop and recreate Bioloop tables (just like current Python script)
2. Populate core business objects with `cmg_id` tracking
3. This establishes the baseline

**Incremental Updates** (Recurring):
1. Poll CMG MongoDB every N minutes
2. Detect changes using `updatedAt` timestamps
3. Upsert changed records in Bioloop using `cmg_id` as lookup key
4. **Preserve Bioloop IDs**: Never delete/recreate, only update

### Why Not Change Data Capture (CDC)?
- MongoDB 4.0.28 lacks Change Streams (requires 3.6+ with replica sets)
- No indexes guaranteed in CMG beyond schema-defined ones
- Polling is simpler, reliable, and acceptable given low traffic

---

## 3. Initial Population Process

### Run Once: Full Database Initialization

**Purpose**: Establish baseline Bioloop data with `cmg_id` tracking

**Script Location**: `api/src/scripts/sync/initialPopulation.js`

**Process**:
```javascript
// 1. Connect to both databases
const prisma = require('@/db');
const { MongoClient } = require('mongodb');

// 2. Drop Bioloop tables (preserve structure, clear data)
await prisma.$transaction(async (tx) => {
  await tx.conversion_derived_dataset.deleteMany();
  await tx.conversion.deleteMany();
  await tx.conversion_definition.deleteMany();
  await tx.cmd_line_program.deleteMany();
  await tx.argument.deleteMany();
  await tx.project_dataset.deleteMany();
  await tx.project_user.deleteMany();
  await tx.project.deleteMany();
  await tx.dataset_hierarchy.deleteMany();
  await tx.dataset_genomic_attributes.deleteMany();
  await tx.dataset_audit.deleteMany();
  await tx.dataset.deleteMany();
  await tx.user_role.deleteMany();
  await tx.user.deleteMany();
  await tx.role.deleteMany();
});

// 3. Populate tables in dependency order
await populateRoles(prisma);
await populateUsers(prisma, cmgDb);
await populateDatasets(prisma, cmgDb);
await populateDatasetGenomicAttributes(prisma, cmgDb);
await populateDatasetAudits(prisma, cmgDb);
await populateDatasetHierarchies(prisma, cmgDb);
await populateProjects(prisma, cmgDb);
await populateConversionInfrastructure(prisma, cmgDb);
await populateConversions(prisma, cmgDb);
// NOTE: Omit sessions, workflows, dataset_files for now
```

**Tables to Populate**:
- ✅ `role`: Hardcoded (user, operator, admin)
- ✅ `user`: From CMG `users` collection
- ✅ `dataset`: From CMG `datasets` + `dataproducts` collections
- ✅ `dataset_genomic_attributes`: Extract from CMG `dataproducts`
- ✅ `dataset_audit`: From CMG `events` collection
- ✅ `dataset_hierarchy`: From CMG `dataproducts.dataset` relationship
- ✅ `project`: From CMG `projects` collection
- ✅ `project_user`: From CMG `projects.users` array
- ✅ `project_dataset`: From CMG `projects.dataproducts` array
- ✅ `conversion_definition`, `cmd_line_program`, `argument`: From constants (like seed.js)
- ✅ `conversion`: From CMG `conversions` collection
- ✅ `conversion_derived_dataset`: From CMG `dataproducts.conversion` relationship

**Tables to SKIP**:
- ❌ `dataset_file`: Too large (~hundreds of TB); populate separately on-demand
- ❌ `workflow`: Bioloop-specific; not in CMG
- ❌ `genome_browser_session`, `track`, `session_track`: Bioloop-specific

---

## 4. Incremental Update Process

### Run Periodically: Detect and Apply Changes

**Purpose**: Keep Bioloop synchronized with CMG changes

**Script Location**: `api/src/scripts/sync/incrementalSync.js`

**Frequency**: Every 2-5 minutes (configurable)

**Process Overview**:
```javascript
// 1. Get last sync timestamp from Bioloop
const lastSync = await getLastSyncTimestamp();

// 2. Query CMG for records updated since lastSync
const updatedUsers = await cmgDb.collection('users').find({
  updatedAt: { $gt: lastSync }
}).toArray();

// 3. Upsert each changed record
for (const cmgUser of updatedUsers) {
  await upsertUser(prisma, cmgUser);
}

// 4. Update sync timestamp
await updateLastSyncTimestamp(new Date());
```

**Change Detection Query Pattern**:
```javascript
// MongoDB query for changes since last sync
{
  $or: [
    { updatedAt: { $gt: lastSyncTimestamp } },
    { createdAt: { $gt: lastSyncTimestamp } }
  ]
}
```

---

## 5. Business Object Mapping

### 5.1 Users

**CMG Collection**: `users`  
**Bioloop Table**: `user`

**Fields**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `_id` | `cmg_id` | Tracking field |
| `username` | `username` | Unique identifier |
| `fullname` | `name` | |
| `email` | `email` | |
| `active` | `is_deleted` | Invert boolean |
| `createDate` | `created_at` | |
| `roles[]` | `user_role` relation | Map to role IDs |

**Upsert Logic**:
```javascript
async function upsertUser(prisma, cmgUser) {
  const cmgId = cmgUser._id.toString();
  
  // Find existing by cmg_id
  const existing = await prisma.user.findFirst({
    where: { cmg_id: cmgId }
  });
  
  const userData = {
    username: cmgUser.username,
    name: cmgUser.fullname || null,
    email: cmgUser.email,
    is_deleted: !cmgUser.active,
    cmg_id: cmgId,
    updated_at: new Date(),
  };
  
  if (existing) {
    // Update existing record (PRESERVE ID)
    await prisma.user.update({
      where: { id: existing.id },
      data: userData
    });
  } else {
    // Create new record
    const newUser = await prisma.user.create({
      data: {
        ...userData,
        created_at: cmgUser.createDate || new Date(),
      }
    });
    
    // Assign roles
    await syncUserRoles(prisma, newUser.id, cmgUser.roles || []);
  }
}
```

**Fields NOT to Update After Initial Population**:
- ❌ `id`: Bioloop-generated, never change
- ❌ `created_at`: Set once, never update

**Fields to Update on Every Sync**:
- ✅ `username`, `name`, `email`, `is_deleted`, `updated_at`

---

### 5.2 Datasets

**CMG Collections**: `datasets` (RAW_DATA) + `dataproducts` (DATA_PRODUCT)  
**Bioloop Table**: `dataset`

**Type Mapping**:
- CMG `datasets` → Bioloop `dataset` with `type='raw_data'`
- CMG `dataproducts` → Bioloop `dataset` with `type='data_product'`

**Fields (datasets)**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `_id` | `cmg_id` | Tracking field |
| `name` | `name` | |
| `'raw_data'` | `type` | Hardcoded |
| `size` | `size` | |
| `du_size` | `du_size` | |
| `files` | `num_files` | |
| `directories` | `num_directories` | |
| `paths.origin` | `origin_path` | |
| `paths.archive` | `archive_path` | |
| `paths.staged` | `staged_path` | |
| `staged` | `is_staged` | |
| `description` | `description` | |
| `createdAt` | `created_at` | |

**Fields (dataproducts)**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `_id` | `cmg_id` | Tracking field |
| `name` | `name` | |
| `'data_product'` | `type` | Hardcoded |
| `size` | `size` | |
| `file_type` | `file_type` | |
| `paths.archive` | `archive_path` | |
| `paths.staged` | `staged_path` | |
| `staged` | `is_staged` | |
| `genome_type` / `genomeType` | via genomic_details | |
| `genome_value` / `genomeValue` | via genomic_details | |
| `createdAt` | `created_at` | |

**Upsert Logic**:
```javascript
async function upsertDataset(prisma, cmgDataset, type) {
  const cmgId = cmgDataset._id.toString();
  
  const existing = await prisma.dataset.findFirst({
    where: { cmg_id: cmgId }
  });
  
  const datasetData = {
    name: cmgDataset.name,
    type: type, // 'raw_data' or 'data_product'
    size: cmgDataset.size ? BigInt(cmgDataset.size) : null,
    du_size: cmgDataset.du_size ? BigInt(cmgDataset.du_size) : null,
    num_files: cmgDataset.files || null,
    num_directories: cmgDataset.directories || null,
    origin_path: cmgDataset.paths?.origin || null,
    archive_path: cmgDataset.paths?.archive || null,
    staged_path: cmgDataset.paths?.staged || null,
    is_staged: cmgDataset.staged || false,
    description: cmgDataset.description || null,
    file_type: cmgDataset.file_type || null,
    cmg_id: cmgId,
    updated_at: new Date(),
  };
  
  if (existing) {
    await prisma.dataset.update({
      where: { id: existing.id },
      data: datasetData
    });
  } else {
    await prisma.dataset.create({
      data: {
        ...datasetData,
        created_at: cmgDataset.createdAt || new Date(),
      }
    });
  }
}
```

**Special Handling: metadata.stage_alias**:
```javascript
// For data_products that are staged, construct stage_alias
if (cmgDataProduct.staged && cmgDataProduct.paths?.staged) {
  const stageAlias = cmgDataProduct.paths.staged;
  datasetData.metadata = {
    stage_alias: stageAlias
  };
}
```

**Fields NOT to Update After Initial Population**:
- ❌ `id`: Bioloop-generated
- ❌ `created_at`: Set once

**Fields to Update on Every Sync**:
- ✅ `is_staged`, `staged_path`, `archive_path`, `size`, `updated_at`

---

### 5.3 Dataset Genomic Attributes

**CMG Collection**: `dataproducts` (embedded fields)  
**Bioloop Table**: `dataset_genomic_attributes`

**Fields**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `genome_type` or `genomeType` | `genome_type` | |
| `genome_value` or `genomeValue` | `genome_value` | |

**Upsert Logic**:
```javascript
async function upsertDatasetGenomicAttributes(prisma, cmgDataProduct, bioloopDatasetId) {
  const genomeType = cmgDataProduct.genome_type || cmgDataProduct.genomeType;
  const genomeValue = cmgDataProduct.genome_value || cmgDataProduct.genomeValue || cmgDataProduct.genome;
  
  if (!genomeType && !genomeValue) return; // Skip if no genomic data
  
  await prisma.dataset_genomic_attributes.upsert({
    where: { dataset_id: bioloopDatasetId },
    update: {
      genome_type: genomeType || null,
      genome_value: genomeValue || null,
    },
    create: {
      dataset_id: bioloopDatasetId,
      genome_type: genomeType || null,
      genome_value: genomeValue || null,
    }
  });
}
```

---

### 5.4 Dataset Audit Logs

**CMG Collection**: `events`  
**Bioloop Table**: `dataset_audit`

**Fields**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `description` | `action` | |
| `stamp` | `timestamp` | |

**Logic**:
```javascript
async function syncDatasetAuditLogs(prisma, cmgDb, bioloopDatasetId, cmgDatasetId) {
  // CMG stores events in embedded array within dataset/dataproduct
  const cmgDataset = await cmgDb.collection('datasets').findOne({ _id: cmgDatasetId });
  
  if (!cmgDataset || !cmgDataset.events) return;
  
  for (const event of cmgDataset.events) {
    await prisma.dataset_audit.upsert({
      where: {
        dataset_id: bioloopDatasetId,
        timestamp: event.stamp,
      },
      update: {
        action: event.description,
        updated_at: new Date(),
      },
      create: {
        dataset_id: bioloopDatasetId,
        action: event.description,
        timestamp: event.stamp,
      }
    });
  }
}
```

**Note**: For incremental sync, only add **new** events (not in Bioloop yet).

---

### 5.5 Dataset Hierarchies

**CMG Logic**: `dataproducts.dataset` (ObjectId reference)  
**Bioloop Table**: `dataset_hierarchy`

**Relationship**: CMG dataproduct derived from CMG dataset

**Logic**:
```javascript
async function upsertDatasetHierarchy(prisma, cmgDataProduct) {
  if (!cmgDataProduct.dataset) return; // No source dataset
  
  // Find source dataset in Bioloop
  const sourceDataset = await prisma.dataset.findFirst({
    where: { cmg_id: cmgDataProduct.dataset.toString() }
  });
  
  // Find derived dataset in Bioloop
  const derivedDataset = await prisma.dataset.findFirst({
    where: { cmg_id: cmgDataProduct._id.toString() }
  });
  
  if (!sourceDataset || !derivedDataset) return;
  
  // Upsert hierarchy relationship
  await prisma.dataset_hierarchy.upsert({
    where: {
      source_id_derived_id: {
        source_id: sourceDataset.id,
        derived_id: derivedDataset.id,
      }
    },
    update: {}, // No fields to update
    create: {
      source_id: sourceDataset.id,
      derived_id: derivedDataset.id,
    }
  });
}
```

---

### 5.6 Projects

**CMG Collection**: `projects`  
**Bioloop Table**: `project`

**Fields**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `_id` | `cmg_id` | Tracking field |
| `name` | `name` | Also use for slug generation |
| `description` | `description` | |
| `browser` | `browser_enabled` | |
| `createdAt` | `created_at` | |

**Upsert Logic**:
```javascript
async function upsertProject(prisma, cmgProject) {
  const cmgId = cmgProject._id.toString();
  
  const existing = await prisma.project.findFirst({
    where: { cmg_id: cmgId }
  });
  
  const slug = existing?.slug || generateSlug(cmgProject.name);
  
  const projectData = {
    name: cmgProject.name,
    slug,
    description: cmgProject.description || null,
    browser_enabled: cmgProject.browser || false,
    cmg_id: cmgId,
    updated_at: new Date(),
  };
  
  if (existing) {
    await prisma.project.update({
      where: { id: existing.id },
      data: projectData
    });
  } else {
    await prisma.project.create({
      data: {
        ...projectData,
        id: generateUuid(), // UUID for Bioloop projects
        created_at: cmgProject.createdAt || new Date(),
      }
    });
  }
}

function generateSlug(name) {
  return name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
}
```

**Fields NOT to Update**:
- ❌ `id`: Bioloop UUID
- ❌ `slug`: Set once (changing breaks URLs)
- ❌ `created_at`

---

### 5.7 Project Users

**CMG Logic**: `projects.users[]` (array of ObjectId)  
**Bioloop Table**: `project_user`

**Logic**:
```javascript
async function syncProjectUsers(prisma, cmgDb, bioloopProjectId, cmgProject) {
  if (!cmgProject.users || cmgProject.users.length === 0) return;
  
  // Get current project users in Bioloop
  const existingUsers = await prisma.project_user.findMany({
    where: { project_id: bioloopProjectId }
  });
  
  const existingUserIds = new Set(existingUsers.map(pu => pu.user_id));
  
  // Add new users
  for (const cmgUserId of cmgProject.users) {
    const bioloopUser = await prisma.user.findFirst({
      where: { cmg_id: cmgUserId.toString() }
    });
    
    if (!bioloopUser) continue;
    
    if (!existingUserIds.has(bioloopUser.id)) {
      await prisma.project_user.create({
        data: {
          project_id: bioloopProjectId,
          user_id: bioloopUser.id,
        }
      });
    }
  }
  
  // Remove users not in CMG anymore
  const cmgUserIdsSet = new Set(cmgProject.users.map(id => id.toString()));
  
  for (const existing of existingUsers) {
    const userCmgId = await prisma.user.findUnique({
      where: { id: existing.user_id },
      select: { cmg_id: true }
    });
    
    if (userCmgId && !cmgUserIdsSet.has(userCmgId.cmg_id)) {
      await prisma.project_user.delete({
        where: {
          project_id_user_id: {
            project_id: bioloopProjectId,
            user_id: existing.user_id,
          }
        }
      });
    }
  }
}
```

---

### 5.8 Project Datasets

**CMG Logic**: `projects.dataproducts[]` (array of ObjectId)  
**Bioloop Table**: `project_dataset`

**Logic**: Same pattern as Project Users (add new, remove deleted)

```javascript
async function syncProjectDatasets(prisma, bioloopProjectId, cmgProject) {
  // Similar to syncProjectUsers but for datasets
  // ...
}
```

---

### 5.9 Conversions

**CMG Collection**: `conversions`  
**Bioloop Table**: `conversion`

**Fields**:
| CMG Field | Bioloop Field | Notes |
|-----------|---------------|-------|
| `_id` | `cmg_id` | Tracking field |
| `pipeline` | `definition_id` | Map to conversion_definition |
| `dataset` | `dataset_id` | Map via cmg_id |
| `user` | `initiator_id` | Map via cmg_id |
| `createdAt` | `initiated_at` | |

**Upsert Logic**:
```javascript
async function upsertConversion(prisma, cmgDb, cmgConversion) {
  const cmgId = cmgConversion._id.toString();
  
  const existing = await prisma.conversion.findFirst({
    where: { cmg_id: cmgId }
  });
  
  // Map definition
  const definition = await prisma.conversion_definition.findFirst({
    where: { name: cmgConversion.pipeline }
  });
  
  if (!definition) {
    console.warn(`No conversion definition for pipeline: ${cmgConversion.pipeline}`);
    return;
  }
  
  // Map dataset
  let datasetId = null;
  if (cmgConversion.dataset) {
    const dataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgConversion.dataset.toString() }
    });
    datasetId = dataset?.id || null;
  }
  
  // Map initiator
  let initiatorId = null;
  if (cmgConversion.user) {
    const user = await prisma.user.findFirst({
      where: { cmg_id: cmgConversion.user.toString() }
    });
    initiatorId = user?.id || null;
  }
  
  const conversionData = {
    definition_id: definition.id,
    dataset_id: datasetId,
    initiator_id: initiatorId,
    cmg_id: cmgId,
  };
  
  if (existing) {
    await prisma.conversion.update({
      where: { id: existing.id },
      data: conversionData
    });
  } else {
    await prisma.conversion.create({
      data: {
        ...conversionData,
        initiated_at: cmgConversion.createdAt || new Date(),
      }
    });
  }
}
```

---

### 5.10 Conversion Derived Datasets

**CMG Logic**: `dataproducts.conversion` (ObjectId reference)  
**Bioloop Table**: `conversion_derived_dataset`

**Logic**:
```javascript
async function syncConversionDerivedDatasets(prisma, cmgDb) {
  // Find all dataproducts with conversion field
  const dataproducts = await cmgDb.collection('dataproducts').find({
    conversion: { $exists: true, $ne: null }
  }).toArray();
  
  for (const dp of dataproducts) {
    const bioloopConversion = await prisma.conversion.findFirst({
      where: { cmg_id: dp.conversion.toString() }
    });
    
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: dp._id.toString() }
    });
    
    if (!bioloopConversion || !bioloopDataset) continue;
    
    await prisma.conversion_derived_dataset.upsert({
      where: {
        conversion_id_dataset_id: {
          conversion_id: bioloopConversion.id,
          dataset_id: bioloopDataset.id,
        }
      },
      update: {},
      create: {
        conversion_id: bioloopConversion.id,
        dataset_id: bioloopDataset.id,
      }
    });
  }
}
```

---

## 6. Change Detection Strategy

### Last Sync Timestamp Storage

**Bioloop Table**: Create a new `sync_metadata` table

```prisma
model sync_metadata {
  id            Int      @id @default(autoincrement())
  last_sync_at  DateTime
  sync_type     String   // 'users', 'datasets', 'projects', etc.
  status        String   // 'success', 'failed'
  error_message String?
}
```

**Usage**:
```javascript
async function getLastSyncTimestamp(syncType) {
  const lastSync = await prisma.sync_metadata.findFirst({
    where: { sync_type: syncType, status: 'success' },
    orderBy: { last_sync_at: 'desc' }
  });
  
  return lastSync?.last_sync_at || new Date(0); // Epoch if never synced
}

async function updateLastSyncTimestamp(syncType, status, error = null) {
  await prisma.sync_metadata.create({
    data: {
      sync_type: syncType,
      last_sync_at: new Date(),
      status,
      error_message: error,
    }
  });
}
```

### MongoDB Change Query

```javascript
async function getChangedRecords(collection, lastSyncTimestamp) {
  return await collection.find({
    $or: [
      { updatedAt: { $gt: lastSyncTimestamp } },
      { createdAt: { $gt: lastSyncTimestamp } }
    ]
  }).toArray();
}
```

### Sync Order (Respect Dependencies)

```javascript
async function runIncrementalSync() {
  const lastSync = await getLastSyncTimestamp('full');
  
  // Order matters: sync parents before children
  await syncUsers(lastSync);
  await syncDatasets(lastSync);
  await syncDatasetGenomicAttributes(lastSync);
  await syncDatasetAudits(lastSync);
  await syncDatasetHierarchies(lastSync);
  await syncProjects(lastSync);
  await syncProjectUsers(lastSync);
  await syncProjectDatasets(lastSync);
  await syncConversions(lastSync);
  await syncConversionDerivedDatasets(lastSync);
  
  await updateLastSyncTimestamp('full', 'success');
}
```

---

## 7. Implementation Guide

### File Structure

```
api/src/scripts/sync/
├── config.js                    # MongoDB connection, sync settings
├── initialPopulation.js         # One-time full population
├── incrementalSync.js           # Recurring sync runner
├── lib/
│   ├── connection.js            # MongoDB + Prisma connections
│   ├── syncMetadata.js          # Last sync timestamp helpers
│   ├── users.js                 # User sync logic
│   ├── datasets.js              # Dataset sync logic
│   ├── projects.js              # Project sync logic
│   ├── conversions.js           # Conversion sync logic
│   └── utils.js                 # Shared utilities
└── README.md                    # Usage instructions
```

### Connection Management

```javascript
// api/src/scripts/sync/lib/connection.js
const { MongoClient } = require('mongodb');
const prisma = require('@/db');

let mongoClient = null;

async function connectMongo() {
  if (mongoClient) return mongoClient;
  
  const uri = `mongodb://${process.env.CMG_MONGO_HOST}:${process.env.CMG_MONGO_PORT}/${process.env.CMG_MONGO_DB}`;
  mongoClient = new MongoClient(uri, {
    auth: {
      username: process.env.CMG_MONGO_USER,
      password: process.env.CMG_MONGO_PASSWORD,
    },
    authSource: process.env.CMG_MONGO_AUTH_SOURCE || 'admin',
  });
  
  await mongoClient.connect();
  return mongoClient;
}

async function closeMongo() {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
  }
}

module.exports = { connectMongo, closeMongo, prisma };
```

### Environment Variables

Add to `api/.env`:
```bash
# CMG MongoDB Connection
CMG_MONGO_HOST=localhost
CMG_MONGO_PORT=27017
CMG_MONGO_DB=cmg_database
CMG_MONGO_USER=cmguser
CMG_MONGO_PASSWORD=cmgpassword
CMG_MONGO_AUTH_SOURCE=admin

# Sync Configuration
SYNC_INTERVAL_MINUTES=2
SYNC_ENABLED=true
```

### Running Initial Population

```bash
cd api
node src/scripts/sync/initialPopulation.js
```

### Running Incremental Sync

**Option 1: Cron Job**
```bash
# Run every 2 minutes
*/2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js >> /var/log/bioloop-sync.log 2>&1
```

**Option 2: PM2 with cron (recommended for development)**
```javascript
// ecosystem.config.js
module.exports = {
  apps: [
    {
      name: 'bioloop-sync',
      script: 'src/scripts/sync/incrementalSync.js',
      cwd: '/opt/sca/cmg-bioloop/api',
      cron_restart: '*/2 * * * *', // Every 2 minutes
      watch: false,
      autorestart: false, // Don't restart on crash
    }
  ]
};
```

**Option 3: Node.js setInterval (for Docker)**
```javascript
// api/src/scripts/sync/continuousSync.js
const { runIncrementalSync } = require('./incrementalSync');

const INTERVAL_MS = parseInt(process.env.SYNC_INTERVAL_MINUTES || '2') * 60 * 1000;

async function runContinuously() {
  console.log(`Starting continuous sync (every ${INTERVAL_MS}ms)`);
  
  while (true) {
    try {
      await runIncrementalSync();
    } catch (error) {
      console.error('Sync failed:', error);
    }
    
    await new Promise(resolve => setTimeout(resolve, INTERVAL_MS));
  }
}

runContinuously();
```

---

## 8. Workflow Event Detection

### Problem
Bioloop needs to know when CMG:
1. Registers a new dataset
2. Starts/completes staging
3. Starts/completes validation
4. Starts/completes conversion

### Solution: Poll CMG Dataset/DataProduct Status Fields

**CMG Fields to Monitor**:
- `datasets.staged` (Boolean)
- `datasets.archived` (Boolean)
- `datasets.validated` (Boolean)
- `datasets.converted` (Boolean)
- `dataproducts.staged` (Boolean)
- `dataproducts.requested` (Boolean)

**Detection Logic**:
```javascript
async function detectWorkflowEvents(prisma, cmgDb) {
  const lastSync = await getLastSyncTimestamp('workflow_events');
  
  // Find datasets that changed state recently
  const changedDatasets = await cmgDb.collection('datasets').find({
    updatedAt: { $gt: lastSync }
  }).toArray();
  
  for (const cmgDataset of changedDatasets) {
    const bioloopDataset = await prisma.dataset.findFirst({
      where: { cmg_id: cmgDataset._id.toString() }
    });
    
    if (!bioloopDataset) continue;
    
    // Check for staging completion
    if (cmgDataset.staged && !bioloopDataset.is_staged) {
      console.log(`[EVENT] Dataset ${bioloopDataset.name} staged in CMG`);
      // Update Bioloop
      await prisma.dataset.update({
        where: { id: bioloopDataset.id },
        data: { is_staged: true, staged_path: cmgDataset.paths?.staged }
      });
      
      // Optionally: Trigger Bioloop-side actions (notifications, etc.)
    }
    
    // Check for archival completion
    if (cmgDataset.archived && !bioloopDataset.archive_path) {
      console.log(`[EVENT] Dataset ${bioloopDataset.name} archived in CMG`);
      await prisma.dataset.update({
        where: { id: bioloopDataset.id },
        data: { archive_path: cmgDataset.paths?.archive }
      });
    }
  }
  
  await updateLastSyncTimestamp('workflow_events', 'success');
}
```

**Logging Events**:
```javascript
// Create a separate log table for detected events
model sync_event_log {
  id         Int      @id @default(autoincrement())
  timestamp  DateTime @default(now())
  event_type String   // 'staging_complete', 'archival_complete', etc.
  dataset_id Int
  dataset    dataset  @relation(fields: [dataset_id], references: [id])
  metadata   Json?
}
```

---

## 9. Error Handling & Recovery

### Retry Logic

```javascript
async function withRetry(fn, maxRetries = 3, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      console.error(`Attempt ${attempt}/${maxRetries} failed:`, error.message);
      
      if (attempt === maxRetries) throw error;
      
      await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
    }
  }
}
```

### Partial Sync Failure

```javascript
async function runIncrementalSync() {
  const syncTasks = [
    { name: 'users', fn: syncUsers },
    { name: 'datasets', fn: syncDatasets },
    { name: 'projects', fn: syncProjects },
    // ...
  ];
  
  const results = [];
  
  for (const task of syncTasks) {
    try {
      await withRetry(() => task.fn(lastSync));
      results.push({ task: task.name, status: 'success' });
    } catch (error) {
      console.error(`Sync task ${task.name} failed:`, error);
      results.push({ task: task.name, status: 'failed', error: error.message });
      // Continue with other tasks
    }
  }
  
  // Log overall result
  const allSuccess = results.every(r => r.status === 'success');
  await updateLastSyncTimestamp('full', allSuccess ? 'success' : 'partial', 
    JSON.stringify(results));
}
```

### Database Connection Failures

```javascript
async function runIncrementalSync() {
  let mongoClient;
  
  try {
    mongoClient = await connectMongo();
    const cmgDb = mongoClient.db(process.env.CMG_MONGO_DB);
    
    // ... sync logic ...
    
  } catch (error) {
    console.error('Sync failed:', error);
    await updateLastSyncTimestamp('full', 'failed', error.message);
    throw error;
  } finally {
    if (mongoClient) {
      await closeMongo();
    }
  }
}
```

---

## 10. Execution Schedule

### Initial Setup (One-Time)

**Day 1**:
```bash
# 1. Run initial population
cd /opt/sca/cmg-bioloop/api
node src/scripts/sync/initialPopulation.js

# 2. Verify data
psql -U bioloopuser -d bioloop_db
SELECT COUNT(*) FROM "user";
SELECT COUNT(*) FROM dataset;
SELECT COUNT(*) FROM project;

# 3. Start incremental sync
pm2 start ecosystem.config.js --only bioloop-sync
# OR
crontab -e
# Add: */2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js
```

### Recurring Sync (Automated)

**Every 2-5 Minutes**:
- Poll CMG MongoDB for changes
- Upsert changed records in Bioloop
- Log sync status

**Frequency Rationale**:
- 2 minutes: Good responsiveness, low overhead
- 5 minutes: More conservative, still acceptable delay
- Avoid <1 minute: Unnecessary load for low-traffic environment

### Monitoring

**Check sync health**:
```sql
-- Last successful sync
SELECT * FROM sync_metadata 
WHERE status = 'success' 
ORDER BY last_sync_at DESC 
LIMIT 1;

-- Recent failures
SELECT * FROM sync_metadata 
WHERE status = 'failed' 
ORDER BY last_sync_at DESC 
LIMIT 10;

-- Detected workflow events
SELECT * FROM sync_event_log 
ORDER BY timestamp DESC 
LIMIT 20;
```

---

## Summary: Quick Start Checklist

### Phase 1: Initial Setup (Day 1)
- [ ] Create `api/src/scripts/sync/` directory structure
- [ ] Implement connection helpers (`lib/connection.js`)
- [ ] Implement initial population script
- [ ] Run initial population to baseline Bioloop data
- [ ] Verify all tables populated correctly

### Phase 2: Incremental Sync (Day 2-3)
- [ ] Create `sync_metadata` table in Prisma schema
- [ ] Implement incremental sync for each business object type
- [ ] Add retry logic and error handling
- [ ] Test incremental sync manually
- [ ] Set up automated execution (cron/PM2)

### Phase 3: Monitoring & Workflow Detection (Day 4-5)
- [ ] Create `sync_event_log` table
- [ ] Implement workflow event detection
- [ ] Add logging for detected events
- [ ] Create monitoring queries/dashboard
- [ ] Test end-to-end: Make change in CMG → Verify in Bioloop

### Phase 4: Production Deployment (Day 6-7)
- [ ] Deploy sync scripts to production server
- [ ] Configure environment variables
- [ ] Start automated sync process
- [ ] Monitor for first week
- [ ] Document any issues and iterate

---

## Key Principles Recap

1. **✅ Preserve Bioloop IDs**: Never delete/recreate records, only update
2. **✅ Use cmg_id as anchor**: All lookups use this field
3. **✅ Respect dependencies**: Sync parents before children
4. **✅ Fail gracefully**: Log errors, continue with other tasks
5. **✅ Idempotent operations**: Upserts are safe to run repeatedly
6. **✅ Simple over perfect**: Polling is fine for this scale
7. **✅ Monitor continuously**: Check sync_metadata regularly

---

**End of Instructions**

This document provides a complete strategy for syncing CMG to Bioloop. The AI agent implementing this should follow the patterns established here, using Prisma and JavaScript exclusively. The design prioritizes stability (no ID changes), simplicity (polling-based), and maintainability (clear separation of concerns).

For questions or clarifications, refer to:
- Bioloop Prisma schema: `api/prisma/schema.prisma`
- CMG models: `db_conversion/cmg_models/*.js`
- Current Python conversion: `db_conversion/src/convert/`


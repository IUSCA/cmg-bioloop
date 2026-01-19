# CMG-to-Bioloop Sync: Implementation Roadmap
**Date:** 2026-01-03  
**Purpose:** Step-by-step guide for AI agent to implement the sync system

---

## Overview

This roadmap breaks down the implementation into manageable phases, each with clear deliverables and testing criteria.

**Total Estimated Time:** 3-4 days  
**Approach:** Iterative (each phase builds on previous)  
**Testing Strategy:** Test after each phase before proceeding

---

## Phase 0: Preparation (2 hours)

### Tasks

1. **Update Prisma Schema**
   - ✅ Add `sync_metadata` table
   - ✅ Add `sync_event_log` table
   - ✅ Add relation to `dataset` model
   - Run migration: `npx prisma migrate dev --name add_sync_tables`

2. **Create Directory Structure**
   ```bash
   mkdir -p api/src/scripts/sync/lib
   ```

3. **Install Dependencies**
   ```bash
   cd api
   npm install mongodb
   ```

4. **Add Environment Variables**
   Add to `api/.env`:
   ```bash
   CMG_MONGO_HOST=localhost
   CMG_MONGO_PORT=27017
   CMG_MONGO_DB=cmg_database
   CMG_MONGO_USER=cmguser
   CMG_MONGO_PASSWORD=cmgpassword
   CMG_MONGO_AUTH_SOURCE=admin
   SYNC_INTERVAL_MINUTES=2
   SYNC_ENABLED=true
   ```

### Deliverables
- ✅ Prisma schema updated
- ✅ Database migrated
- ✅ Directory structure created
- ✅ Dependencies installed
- ✅ Environment variables configured

### Testing
```bash
# Verify migration
npx prisma migrate status

# Verify tables exist
psql -U bioloopuser -d bioloop_db -c "\dt sync_*"
```

---

## Phase 1: Connection & Utilities (3 hours)

### File: `api/src/scripts/sync/lib/connection.js`

**Purpose:** Manage MongoDB and Prisma connections

```javascript
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
  console.log('[SYNC] Connected to CMG MongoDB');
  return mongoClient;
}

async function closeMongo() {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
    console.log('[SYNC] Closed CMG MongoDB connection');
  }
}

function getCmgDb() {
  if (!mongoClient) throw new Error('MongoDB not connected');
  return mongoClient.db(process.env.CMG_MONGO_DB);
}

module.exports = { connectMongo, closeMongo, getCmgDb, prisma };
```

### File: `api/src/scripts/sync/lib/syncMetadata.js`

**Purpose:** Track last sync timestamps

```javascript
const { prisma } = require('./connection');

async function getLastSyncTimestamp(syncType) {
  const lastSync = await prisma.sync_metadata.findFirst({
    where: { sync_type: syncType, status: 'success' },
    orderBy: { last_sync_at: 'desc' }
  });
  
  return lastSync?.last_sync_at || new Date(0); // Epoch if never synced
}

async function updateSyncMetadata(syncType, status, error = null) {
  await prisma.sync_metadata.create({
    data: {
      sync_type: syncType,
      last_sync_at: new Date(),
      status,
      error_message: error,
    }
  });
}

module.exports = { getLastSyncTimestamp, updateSyncMetadata };
```

### File: `api/src/scripts/sync/lib/utils.js`

**Purpose:** Shared utility functions

```javascript
function generateSlug(name) {
  return name
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .substring(0, 50);
}

async function withRetry(fn, maxRetries = 3, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      console.error(`[RETRY] Attempt ${attempt}/${maxRetries} failed:`, error.message);
      
      if (attempt === maxRetries) throw error;
      
      await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
    }
  }
}

function safeToString(objectId) {
  return objectId ? objectId.toString() : null;
}

module.exports = { generateSlug, withRetry, safeToString };
```

### Deliverables
- ✅ Connection management implemented
- ✅ Sync metadata tracking implemented
- ✅ Utility functions implemented

### Testing
```bash
# Test MongoDB connection
node -e "
  require('dotenv').config();
  const { connectMongo, closeMongo } = require('./api/src/scripts/sync/lib/connection');
  (async () => {
    await connectMongo();
    console.log('✅ MongoDB connected');
    await closeMongo();
    console.log('✅ MongoDB closed');
  })();
"
```

---

## Phase 2: User Sync (4 hours)

### File: `api/src/scripts/sync/lib/users.js`

**Purpose:** Sync users from CMG to Bioloop

```javascript
const { prisma, getCmgDb } = require('./connection');
const { safeToString } = require('./utils');

async function syncUsers(lastSyncTimestamp) {
  console.log('[USERS] Starting user sync...');
  
  const cmgDb = getCmgDb();
  
  // Get changed users from CMG
  const changedUsers = await cmgDb.collection('users').find({
    $or: [
      { updatedAt: { $gt: lastSyncTimestamp } },
      { createDate: { $gt: lastSyncTimestamp } }
    ]
  }).toArray();
  
  console.log(`[USERS] Found ${changedUsers.length} changed users`);
  
  for (const cmgUser of changedUsers) {
    await upsertUser(cmgUser);
  }
  
  console.log('[USERS] User sync complete');
}

async function upsertUser(cmgUser) {
  const cmgId = safeToString(cmgUser._id);
  
  // Find existing by cmg_id
  const existing = await prisma.user.findFirst({
    where: { cmg_id: cmgId }
  });
  
  const userData = {
    username: cmgUser.username,
    name: cmgUser.fullname || null,
    email: cmgUser.email,
    cas_id: cmgUser.username, // CMG uses username as CAS ID
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
    console.log(`[USERS] Updated user: ${cmgUser.username}`);
  } else {
    // Create new record
    const newUser = await prisma.user.create({
      data: {
        ...userData,
        created_at: cmgUser.createDate || new Date(),
      }
    });
    
    // Assign roles
    await syncUserRoles(newUser.id, cmgUser.roles || []);
    console.log(`[USERS] Created user: ${cmgUser.username}`);
  }
}

async function syncUserRoles(userId, cmgRoles) {
  // Map CMG roles to Bioloop roles
  const roleMap = {
    'admin': 'admin',
    'god': 'admin',
    'technologist': 'operator',
    'researcher': 'user',
    'user': 'user',
  };
  
  const bioloopRoleNames = [...new Set(
    cmgRoles.map(r => roleMap[r] || 'user')
  )];
  
  // Get role IDs
  const roles = await prisma.role.findMany({
    where: { name: { in: bioloopRoleNames } }
  });
  
  // Delete existing roles
  await prisma.user_role.deleteMany({
    where: { user_id: userId }
  });
  
  // Assign new roles
  for (const role of roles) {
    await prisma.user_role.create({
      data: {
        user_id: userId,
        role_id: role.id,
      }
    });
  }
}

module.exports = { syncUsers };
```

### Deliverables
- ✅ User sync logic implemented
- ✅ Role mapping implemented
- ✅ Upsert pattern working

### Testing
```bash
# Create test script: api/src/scripts/sync/testUserSync.js
node api/src/scripts/sync/testUserSync.js

# Verify in database
psql -U bioloopuser -d bioloop_db -c "SELECT id, username, cmg_id, is_deleted FROM \"user\" LIMIT 10;"
```

---

## Phase 3: Dataset Sync (6 hours)

### File: `api/src/scripts/sync/lib/datasets.js`

**Purpose:** Sync datasets and dataproducts from CMG to Bioloop

```javascript
const { prisma, getCmgDb } = require('./connection');
const { safeToString } = require('./utils');

async function syncDatasets(lastSyncTimestamp) {
  console.log('[DATASETS] Starting dataset sync...');
  
  const cmgDb = getCmgDb();
  
  // Sync raw_data datasets
  await syncRawDatasets(cmgDb, lastSyncTimestamp);
  
  // Sync data_product datasets
  await syncDataProducts(cmgDb, lastSyncTimestamp);
  
  console.log('[DATASETS] Dataset sync complete');
}

async function syncRawDatasets(cmgDb, lastSyncTimestamp) {
  const changedDatasets = await cmgDb.collection('datasets').find({
    $or: [
      { updatedAt: { $gt: lastSyncTimestamp } },
      { createdAt: { $gt: lastSyncTimestamp } }
    ]
  }).toArray();
  
  console.log(`[DATASETS] Found ${changedDatasets.length} changed raw datasets`);
  
  for (const cmgDataset of changedDatasets) {
    await upsertDataset(cmgDataset, 'raw_data');
  }
}

async function syncDataProducts(cmgDb, lastSyncTimestamp) {
  const changedDataProducts = await cmgDb.collection('dataproducts').find({
    $or: [
      { updatedAt: { $gt: lastSyncTimestamp } },
      { createdAt: { $gt: lastSyncTimestamp } }
    ]
  }).toArray();
  
  console.log(`[DATASETS] Found ${changedDataProducts.length} changed data products`);
  
  for (const cmgDataProduct of changedDataProducts) {
    await upsertDataset(cmgDataProduct, 'data_product');
    await upsertDatasetGenomicAttributes(cmgDataProduct);
  }
}

async function upsertDataset(cmgDataset, type) {
  const cmgId = safeToString(cmgDataset._id);
  
  const existing = await prisma.dataset.findFirst({
    where: { cmg_id: cmgId }
  });
  
  const datasetData = {
    name: cmgDataset.name,
    type: type,
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
  
  // Add metadata for data_products
  if (type === 'data_product' && cmgDataset.staged && cmgDataset.paths?.staged) {
    datasetData.metadata = {
      stage_alias: cmgDataset.paths.staged
    };
  }
  
  if (existing) {
    await prisma.dataset.update({
      where: { id: existing.id },
      data: datasetData
    });
    console.log(`[DATASETS] Updated dataset: ${cmgDataset.name}`);
  } else {
    await prisma.dataset.create({
      data: {
        ...datasetData,
        created_at: cmgDataset.createdAt || new Date(),
      }
    });
    console.log(`[DATASETS] Created dataset: ${cmgDataset.name}`);
  }
}

async function upsertDatasetGenomicAttributes(cmgDataProduct) {
  const cmgId = safeToString(cmgDataProduct._id);
  
  const dataset = await prisma.dataset.findFirst({
    where: { cmg_id: cmgId }
  });
  
  if (!dataset) return;
  
  const genomeType = cmgDataProduct.genome_type || cmgDataProduct.genomeType;
  const genomeValue = cmgDataProduct.genome_value || cmgDataProduct.genomeValue || cmgDataProduct.genome;
  
  if (!genomeType && !genomeValue) return;
  
  await prisma.dataset_genomic_attributes.upsert({
    where: { dataset_id: dataset.id },
    update: {
      genome_type: genomeType || null,
      genome_value: genomeValue || null,
    },
    create: {
      dataset_id: dataset.id,
      genome_type: genomeType || null,
      genome_value: genomeValue || null,
    }
  });
}

module.exports = { syncDatasets };
```

### Deliverables
- ✅ Raw dataset sync implemented
- ✅ Data product sync implemented
- ✅ Genomic attributes sync implemented
- ✅ metadata.stage_alias handling

### Testing
```bash
# Test dataset sync
node api/src/scripts/sync/testDatasetSync.js

# Verify in database
psql -U bioloopuser -d bioloop_db -c "
  SELECT id, name, type, is_staged, cmg_id 
  FROM dataset 
  LIMIT 10;
"

psql -U bioloopuser -d bioloop_db -c "
  SELECT * FROM dataset_genomic_attributes LIMIT 10;
"
```

---

## Phase 4: Project & Conversion Sync (4 hours)

### File: `api/src/scripts/sync/lib/projects.js`

**Purpose:** Sync projects and relationships

```javascript
const { prisma, getCmgDb } = require('./connection');
const { safeToString, generateSlug } = require('./utils');
const { v4: uuidv4 } = require('uuid');

async function syncProjects(lastSyncTimestamp) {
  console.log('[PROJECTS] Starting project sync...');
  
  const cmgDb = getCmgDb();
  
  const changedProjects = await cmgDb.collection('projects').find({
    $or: [
      { updatedAt: { $gt: lastSyncTimestamp } },
      { createdAt: { $gt: lastSyncTimestamp } }
    ]
  }).toArray();
  
  console.log(`[PROJECTS] Found ${changedProjects.length} changed projects`);
  
  for (const cmgProject of changedProjects) {
    await upsertProject(cmgProject);
    await syncProjectUsers(cmgProject);
    await syncProjectDatasets(cmgProject);
  }
  
  console.log('[PROJECTS] Project sync complete');
}

async function upsertProject(cmgProject) {
  const cmgId = safeToString(cmgProject._id);
  
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
    console.log(`[PROJECTS] Updated project: ${cmgProject.name}`);
  } else {
    await prisma.project.create({
      data: {
        ...projectData,
        id: uuidv4(),
        created_at: cmgProject.createdAt || new Date(),
      }
    });
    console.log(`[PROJECTS] Created project: ${cmgProject.name}`);
  }
}

async function syncProjectUsers(cmgProject) {
  const cmgId = safeToString(cmgProject._id);
  
  const project = await prisma.project.findFirst({
    where: { cmg_id: cmgId }
  });
  
  if (!project || !cmgProject.users) return;
  
  // Get current project users
  const existingUsers = await prisma.project_user.findMany({
    where: { project_id: project.id }
  });
  
  const existingUserIds = new Set(existingUsers.map(pu => pu.user_id));
  
  // Add new users
  for (const cmgUserId of cmgProject.users) {
    const user = await prisma.user.findFirst({
      where: { cmg_id: safeToString(cmgUserId) }
    });
    
    if (!user || existingUserIds.has(user.id)) continue;
    
    await prisma.project_user.create({
      data: {
        project_id: project.id,
        user_id: user.id,
      }
    });
  }
  
  // Remove users not in CMG anymore
  const cmgUserIdsSet = new Set(cmgProject.users.map(id => safeToString(id)));
  
  for (const existing of existingUsers) {
    const user = await prisma.user.findUnique({
      where: { id: existing.user_id },
      select: { cmg_id: true }
    });
    
    if (user && !cmgUserIdsSet.has(user.cmg_id)) {
      await prisma.project_user.delete({
        where: {
          project_id_user_id: {
            project_id: project.id,
            user_id: existing.user_id,
          }
        }
      });
    }
  }
}

async function syncProjectDatasets(cmgProject) {
  // Similar logic to syncProjectUsers but for datasets
  // Implementation omitted for brevity
}

module.exports = { syncProjects };
```

### File: `api/src/scripts/sync/lib/conversions.js`

**Purpose:** Sync conversions

```javascript
// Similar pattern to users/datasets
// Implementation details in full instructions document
```

### Deliverables
- ✅ Project sync implemented
- ✅ Project-user relationships synced
- ✅ Project-dataset relationships synced
- ✅ Conversion sync implemented

### Testing
```bash
# Test project sync
node api/src/scripts/sync/testProjectSync.js

# Verify
psql -U bioloopuser -d bioloop_db -c "
  SELECT p.id, p.name, p.slug, COUNT(pu.user_id) as user_count
  FROM project p
  LEFT JOIN project_user pu ON p.id = pu.project_id
  GROUP BY p.id
  LIMIT 10;
"
```

---

## Phase 5: Initial Population Script (2 hours)

### File: `api/src/scripts/sync/initialPopulation.js`

**Purpose:** One-time full population

```javascript
const { connectMongo, closeMongo, prisma } = require('./lib/connection');
const { updateSyncMetadata } = require('./lib/syncMetadata');
const { syncUsers } = require('./lib/users');
const { syncDatasets } = require('./lib/datasets');
const { syncProjects } = require('./lib/projects');
const { syncConversions } = require('./lib/conversions');

async function runInitialPopulation() {
  console.log('[INIT] Starting initial population...');
  
  try {
    await connectMongo();
    
    // Drop existing data (preserve structure)
    console.log('[INIT] Clearing existing data...');
    await clearBioloopData();
    
    // Populate in dependency order
    const epoch = new Date(0);
    
    console.log('[INIT] Syncing users...');
    await syncUsers(epoch);
    
    console.log('[INIT] Syncing datasets...');
    await syncDatasets(epoch);
    
    console.log('[INIT] Syncing projects...');
    await syncProjects(epoch);
    
    console.log('[INIT] Syncing conversions...');
    await syncConversions(epoch);
    
    // Mark as successful
    await updateSyncMetadata('initial_population', 'success');
    
    console.log('[INIT] Initial population complete!');
  } catch (error) {
    console.error('[INIT] Initial population failed:', error);
    await updateSyncMetadata('initial_population', 'failed', error.message);
    throw error;
  } finally {
    await closeMongo();
  }
}

async function clearBioloopData() {
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
    // Don't delete roles (they're hardcoded)
  });
}

// Run if executed directly
if (require.main === module) {
  runInitialPopulation()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}

module.exports = { runInitialPopulation };
```

### Deliverables
- ✅ Initial population script complete
- ✅ Data clearing logic implemented
- ✅ Dependency order respected

### Testing
```bash
# Run initial population
cd /opt/sca/cmg-bioloop/api
node src/scripts/sync/initialPopulation.js

# Verify all tables populated
psql -U bioloopuser -d bioloop_db -c "
  SELECT 
    'users' as table_name, COUNT(*) as count FROM \"user\"
  UNION ALL
  SELECT 'datasets', COUNT(*) FROM dataset
  UNION ALL
  SELECT 'projects', COUNT(*) FROM project
  UNION ALL
  SELECT 'conversions', COUNT(*) FROM conversion;
"
```

---

## Phase 6: Incremental Sync Script (2 hours)

### File: `api/src/scripts/sync/incrementalSync.js`

**Purpose:** Recurring sync runner

```javascript
const { connectMongo, closeMongo } = require('./lib/connection');
const { getLastSyncTimestamp, updateSyncMetadata } = require('./lib/syncMetadata');
const { withRetry } = require('./lib/utils');
const { syncUsers } = require('./lib/users');
const { syncDatasets } = require('./lib/datasets');
const { syncProjects } = require('./lib/projects');
const { syncConversions } = require('./lib/conversions');

async function runIncrementalSync() {
  console.log('[SYNC] Starting incremental sync...');
  
  try {
    await connectMongo();
    
    const lastSync = await getLastSyncTimestamp('full');
    console.log(`[SYNC] Last sync: ${lastSync.toISOString()}`);
    
    const syncTasks = [
      { name: 'users', fn: () => syncUsers(lastSync) },
      { name: 'datasets', fn: () => syncDatasets(lastSync) },
      { name: 'projects', fn: () => syncProjects(lastSync) },
      { name: 'conversions', fn: () => syncConversions(lastSync) },
    ];
    
    const results = [];
    
    for (const task of syncTasks) {
      try {
        await withRetry(task.fn);
        results.push({ task: task.name, status: 'success' });
      } catch (error) {
        console.error(`[SYNC] Task ${task.name} failed:`, error);
        results.push({ task: task.name, status: 'failed', error: error.message });
      }
    }
    
    const allSuccess = results.every(r => r.status === 'success');
    await updateSyncMetadata(
      'full',
      allSuccess ? 'success' : 'partial',
      JSON.stringify(results)
    );
    
    console.log('[SYNC] Incremental sync complete');
    console.log('[SYNC] Results:', results);
    
  } catch (error) {
    console.error('[SYNC] Incremental sync failed:', error);
    await updateSyncMetadata('full', 'failed', error.message);
    throw error;
  } finally {
    await closeMongo();
  }
}

// Run if executed directly
if (require.main === module) {
  runIncrementalSync()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}

module.exports = { runIncrementalSync };
```

### Deliverables
- ✅ Incremental sync script complete
- ✅ Retry logic implemented
- ✅ Partial failure handling

### Testing
```bash
# Make a change in CMG MongoDB
# Then run incremental sync
node api/src/scripts/sync/incrementalSync.js

# Verify change reflected in Bioloop
psql -U bioloopuser -d bioloop_db -c "
  SELECT * FROM sync_metadata 
  ORDER BY last_sync_at DESC 
  LIMIT 5;
"
```

---

## Phase 7: Automation & Monitoring (2 hours)

### Setup Cron Job

```bash
# Edit crontab
crontab -e

# Add line (run every 2 minutes)
*/2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js >> /var/log/bioloop-sync.log 2>&1
```

### Create Monitoring Queries

**File:** `api/src/scripts/sync/monitorSync.js`

```javascript
const { prisma } = require('./lib/connection');

async function monitorSync() {
  // Last successful sync
  const lastSync = await prisma.sync_metadata.findFirst({
    where: { status: 'success' },
    orderBy: { last_sync_at: 'desc' }
  });
  
  console.log('Last successful sync:', lastSync?.last_sync_at);
  
  // Recent failures
  const failures = await prisma.sync_metadata.findMany({
    where: { status: 'failed' },
    orderBy: { last_sync_at: 'desc' },
    take: 5
  });
  
  console.log('Recent failures:', failures.length);
  
  // Sync lag
  if (lastSync) {
    const lag = Date.now() - lastSync.last_sync_at.getTime();
    const lagMinutes = Math.floor(lag / 60000);
    console.log(`Sync lag: ${lagMinutes} minutes`);
    
    if (lagMinutes > 10) {
      console.warn('⚠️  WARNING: Sync lag exceeds 10 minutes');
    }
  }
}

if (require.main === module) {
  monitorSync()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}

module.exports = { monitorSync };
```

### Deliverables
- ✅ Cron job configured
- ✅ Monitoring script created
- ✅ Logging configured

### Testing
```bash
# Wait 2 minutes, check if cron ran
tail -f /var/log/bioloop-sync.log

# Run monitoring
node api/src/scripts/sync/monitorSync.js
```

---

## Phase 8: Documentation & Handoff (1 hour)

### Create README

**File:** `api/src/scripts/sync/README.md`

```markdown
# CMG-Bioloop Database Sync

## Quick Start

### Initial Setup (One-time)
```bash
# 1. Configure environment variables in api/.env
# 2. Run initial population
node src/scripts/sync/initialPopulation.js

# 3. Start automated sync (cron)
crontab -e
# Add: */2 * * * * cd /opt/sca/cmg-bioloop/api && node src/scripts/sync/incrementalSync.js
```

### Monitoring
```bash
# Check sync health
node src/scripts/sync/monitorSync.js

# View sync logs
tail -f /var/log/bioloop-sync.log

# Query sync metadata
psql -U bioloopuser -d bioloop_db -c "SELECT * FROM sync_metadata ORDER BY last_sync_at DESC LIMIT 10;"
```

### Troubleshooting
See: /Users/ripandey/dev/cmg-bioloop/CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md
```

### Deliverables
- ✅ README created
- ✅ Usage instructions documented
- ✅ Troubleshooting guide linked

---

## Testing Checklist

### End-to-End Test

1. **Initial Population**
   - [ ] Run initial population script
   - [ ] Verify all tables populated
   - [ ] Check cmg_id fields populated
   - [ ] Verify relationships (project_user, project_dataset, etc.)

2. **Incremental Sync**
   - [ ] Make change in CMG (update user email)
   - [ ] Run incremental sync
   - [ ] Verify change reflected in Bioloop
   - [ ] Verify Bioloop ID unchanged

3. **ID Stability**
   - [ ] Note Bioloop user ID
   - [ ] Run incremental sync multiple times
   - [ ] Verify ID remains the same

4. **Workflow Event Detection**
   - [ ] Stage a dataset in CMG
   - [ ] Run incremental sync
   - [ ] Verify is_staged updated in Bioloop
   - [ ] Check sync_event_log for event

5. **Error Handling**
   - [ ] Temporarily break MongoDB connection
   - [ ] Run incremental sync
   - [ ] Verify error logged in sync_metadata
   - [ ] Fix connection, verify recovery

6. **Automation**
   - [ ] Wait for cron to run automatically
   - [ ] Check logs
   - [ ] Verify sync_metadata updated

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

## Timeline Summary

| Phase | Duration | Cumulative |
|-------|----------|------------|
| 0. Preparation | 2 hours | 2 hours |
| 1. Connection & Utilities | 3 hours | 5 hours |
| 2. User Sync | 4 hours | 9 hours |
| 3. Dataset Sync | 6 hours | 15 hours |
| 4. Project & Conversion Sync | 4 hours | 19 hours |
| 5. Initial Population Script | 2 hours | 21 hours |
| 6. Incremental Sync Script | 2 hours | 23 hours |
| 7. Automation & Monitoring | 2 hours | 25 hours |
| 8. Documentation & Handoff | 1 hour | 26 hours |

**Total:** ~26 hours (~3-4 days)

---

**Next Steps:** Begin with Phase 0 (Preparation)


# CMG to Bioloop Database Conversion Process

## Overview

This document provides a comprehensive guide to the MongoDB-to-PostgreSQL conversion process for migrating data from the CMG (Clinical Microbiome Genomics) project to the Bioloop application. This conversion transforms a document-based genomic data management system into a modern relational database architecture.

## Project Paths

- **Source CMG Project**: `/Users/ripandey/dev/cmg` (MongoDB-based)
- **Target Bioloop Project**: `/Users/ripandey/dev/cmg-bioloop-2` (PostgreSQL-based)

## Architecture Overview

### CMG (Source) - MongoDB
CMG is a genomic data management system built on MongoDB with the following key collections:

- **Users** - Authentication and authorization with roles, CAS integration
- **Datasets** - Raw sequencing data with file paths, checksums, staging status
- **Dataproducts** - Processed/derived data products from raw datasets
- **Projects** - Grouping mechanism for datasets and users
- **Conversions** - Pipeline execution records (genomic processing)
- **Events** - Audit trail and system events
- **Groups** - User grouping for project access

### Bioloop (Target) - PostgreSQL
Bioloop is a modern PostgreSQL-based system with enhanced structure:

- **Users** - Enhanced user management with metadata and settings
- **Datasets** - Unified table for both raw data and data products with type differentiation
- **Projects** - Project management with user and dataset associations
- **Workflows** - Task execution and monitoring
- **Audit Logs** - Comprehensive audit trail
- **File Management** - Detailed file tracking with hierarchies

## Conversion Process Flow

### 1. Database Setup Phase
```python
# Drop existing Bioloop tables and enums
drop_bioloop_enums(pg_cursor)
drop_bioloop_tables(pg_cursor)

# Recreate schema
create_bioloop_enums(pg_cursor)
create_bioloop_tables(pg_cursor)

# Clear workflow documents from Rhythm MongoDB
drop_all_workflow_documents(rhythm_db)
```

### 2. Data Conversion Sequence
The conversion follows this systematic order:

1. **Roles** - Create standard roles (admin, operator, user)
2. **Users** - Convert CMG users with role mapping
3. **Datasets** - Convert both raw datasets and dataproducts
4. **Audit Logs** - Convert CMG events to audit logs
5. **Dataset Hierarchies** - Map dataproduct → dataset relationships
6. **Dataset Files** - Extract file information from dataproducts
7. **Projects** - Convert with user and dataset associations
8. **Conversions** - Convert pipeline execution records
9. **Workflows** - Create workflow metadata in Rhythm MongoDB

## Key Conversion Mappings

### Entity Mappings
- `CMG.users` → `Bioloop.user` + `user_role`
- `CMG.datasets` → `Bioloop.dataset` (type: "RAW_DATA")
- `CMG.dataproducts` → `Bioloop.dataset` (type: "DATA_PRODUCT")
- `CMG.projects` → `Bioloop.project` + `project_user` + `project_dataset`
- `CMG.events` → `Bioloop.dataset_audit`
- `CMG.conversions` → `Bioloop.conversion`

### Role Mapping
```python
role_mapping = {
    'admin': 'admin',
    'operator': 'operator', 
    'user': 'user'
}
```

## Data Transformation Strategies

### 1. Duplicate Handling
- CMG datasets with duplicate names get prefixed with "DUPLICATE_" or "DUPLICATE_N_"
- Unknown datasets get "UNKNOWN" prefix
- Maintains referential integrity while avoiding conflicts

### 2. Dataset Type Differentiation
- CMG `datasets` → Bioloop `dataset` with `type = "RAW_DATA"`
- CMG `dataproducts` → Bioloop `dataset` with `type = "DATA_PRODUCT"`

### 3. File Management
- CMG dataproduct `files` array → Bioloop `dataset_file` table
- Preserves file paths, sizes, and MD5 checksums

## File Structure

### Conversion Scripts Location
```
db_conversion/
├── cmg_models/           # CMG MongoDB schema definitions
│   ├── user.js
│   ├── dataset.js
│   ├── dataproduct.js
│   ├── project.js
│   ├── conversion.js
│   ├── event.js
│   └── ...
├── src/convert/
│   ├── scripts/
│   │   └── convert.py    # Main conversion script
│   ├── entity/           # Entity conversion modules
│   │   ├── user.py
│   │   ├── dataset.py
│   │   ├── project.py
│   │   ├── conversion.py
│   │   ├── audit_log.py
│   │   ├── workflow.py
│   │   └── ...
│   ├── operations/
│   │   └── ddl.py        # Database schema operations
│   ├── constants/
│   │   ├── cmg.py        # CMG-specific constants
│   │   ├── bioloop.py    # Bioloop-specific constants
│   │   └── common.py     # Shared constants
│   └── pg_queries/
│       └── queries.py    # PostgreSQL DDL queries
```

### Target Schema Location
```
api/prisma/schema.prisma   # Bioloop PostgreSQL schema definition
```

## Key CMG Data Models

### User Model
```javascript
{
  username: String (unique),
  createDate: Date,
  lastLogin: Date,
  roles: [String],
  primary_role: String,
  fullname: String,
  email: String,
  notifications: Boolean,
  active: Boolean,
  prefs: Mixed,
  hash: String,
  salt: String
}
```

### Dataset Model
```javascript
{
  name: String (unique),
  paths: {
    origin: String,
    archive: String,
    staged: String
  },
  source_node: String,
  size: Number,
  du_size: Number,
  description: String,
  files: Number,
  cbcls: Number,
  checksums: [{
    path: String,
    md5: String
  }],
  directories: Number,
  inspected: Boolean,
  archived: Boolean,
  staged: Boolean,
  validated: Boolean,
  errored: Mixed,
  taken: ObjectId (ref: "worker"),
  takenAt: Date,
  events: [{
    stamp: Date,
    description: String
  }]
}
```

### Dataproduct Model
```javascript
{
  name: String,
  files: [{
    path: String,
    size: Number,
    md5: String
  }],
  paths: {
    archive: String,
    staged: String
  },
  file_type: String,
  size: Number,
  genome: String,
  genome_type: String,
  dataset: ObjectId (ref: "dataset"),
  conversion: ObjectId (ref: "conversion"),
  upload: ObjectId (ref: "upload"),
  groups: [ObjectId (ref: "Group")],
  users: [ObjectId (ref: "User")],
  staged: Boolean,
  visible: Boolean,
  lastStaged: Date,
  requested: Boolean,
  errored: Mixed,
  disable_archive: Boolean,
  taken: ObjectId (ref: "worker"),
  takenAt: Date,
  notify: [String],
  upload_to_s3: Boolean,
  celery_workflow_id: String,
  events: [{
    stamp: Date,
    description: String
  }],
  genomeType: String,
  genomeValue: String
}
```

### Project Model
```javascript
{
  name: String,
  description: String,
  size: Number,
  dataproducts: [ObjectId (ref: "dataproduct")],
  groups: [ObjectId (ref: "Group")],
  users: [ObjectId (ref: "User")],
  browser: Boolean
}
```

### Conversion Model
```javascript
{
  user: ObjectId (ref: "User"),
  dataset: ObjectId (ref: "dataset"),
  pipeline: String,
  options: [String],
  samplesheet: String,
  worker: ObjectId (ref: "worker"),
  staged: Boolean,
  output_path: String,
  status: String
}
```

### Event Model
```javascript
{
  user: ObjectId (ref: "User"),
  action: String,
  details: String,
  route: String,
  dataproduct: ObjectId (ref: "dataproduct"),
  dataset: ObjectId (ref: "dataset"),
  project: ObjectId (ref: "project"),
  worker: ObjectId (ref: "worker")
}
```

## Key Bioloop Tables

### User Table
```sql
CREATE TABLE "user" (
  "id" SERIAL PRIMARY KEY,
  "username" VARCHAR(100) UNIQUE NOT NULL,
  "name" VARCHAR(100),
  "email" VARCHAR(100) UNIQUE NOT NULL,
  "cas_id" VARCHAR(100) UNIQUE,
  "notes" TEXT,
  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "is_deleted" BOOLEAN NOT NULL DEFAULT false
);
```

### Dataset Table
```sql
CREATE TABLE "dataset" (
  "id" SERIAL PRIMARY KEY,
  "cmg_id" TEXT UNIQUE,
  "name" TEXT NOT NULL,
  "type" TEXT NOT NULL,
  "num_directories" INTEGER,
  "num_files" INTEGER,
  "du_size" BIGINT,
  "size" BIGINT,
  "bundle_size" BIGINT,
  "description" TEXT,
  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "origin_path" TEXT,
  "archive_path" TEXT,
  "staged_path" TEXT,
  "is_deleted" BOOLEAN NOT NULL DEFAULT false,
  "is_staged" BOOLEAN NOT NULL DEFAULT false,
  "metadata" JSONB,
  UNIQUE ("name", "type", "is_deleted")
);
```

## Running the Conversion

### Prerequisites
1. MongoDB instances running (CMG and Rhythm)
2. PostgreSQL database running (Bioloop)
3. Python environment with required packages
4. Environment variables configured

### Environment Variables
```bash
# CMG MongoDB
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=cmg_database
MONGO_AUTH_SOURCE=admin
MONGO_USERNAME=username
MONGO_PASSWORD=password

# Rhythm MongoDB
RHYTHM_MONGO_HOST=localhost
RHYTHM_MONGO_PORT=27018
RHYTHM_MONGO_DB=rhythm_database
RHYTHM_MONGO_AUTH_SOURCE=admin
RHYTHM_MONGO_USERNAME=username
RHYTHM_MONGO_PASSWORD=password

# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=bioloop
PG_USER=username
PG_PASSWORD=password
```

### Execution
```bash
cd /Users/ripandey/dev/cmg-bioloop-2/db_conversion
python -m src.convert.scripts.convert
```

## Key Challenges and Solutions

### 1. Data Integrity Challenges
- **Referential Integrity**: CMG uses ObjectId references that need mapping to PostgreSQL integer IDs
- **Duplicate Names**: Complex logic to handle datasets with identical names
- **Missing Data**: Graceful handling of datasets without names or timestamps

### 2. Schema Differences
- **Flexible vs Structured**: MongoDB's flexible schema vs PostgreSQL's rigid structure
- **Nested Data**: CMG's nested objects (paths, events) need flattening
- **Data Types**: MongoDB's mixed types need PostgreSQL type conversion

### 3. Performance Considerations
- **Batch Operations**: Uses `executemany()` for bulk inserts
- **Transaction Management**: Autocommit mode for error visibility
- **Memory Usage**: Processes large datasets in batches

### 4. Workflow Integration
- **Dual Database**: Maintains both PostgreSQL (Bioloop) and MongoDB (Rhythm) for workflows
- **Task Metadata**: Creates Celery task metadata in Rhythm MongoDB
- **Workflow Steps**: Generates standard workflow steps (inspect, archive, stage, validate)

### 5. Error Handling
- **Graceful Degradation**: Continues processing even if individual records fail
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Rollback Capability**: Transaction rollback on critical failures

## Conversion Script Architecture

### Entity Converters
Each data type has its own conversion module:
- `user.py` - User and role conversion
- `dataset.py` - Dataset and dataproduct conversion
- `project.py` - Project and association conversion
- `conversion.py` - Pipeline conversion records
- `audit_log.py` - Event to audit log conversion
- `workflow.py` - Workflow metadata creation

### Common Utilities
- `common.py` - Shared functions for dataset/user lookups
- `constants/` - Centralized configuration for roles and mappings
- `operations/ddl.py` - DDL operations for schema management

### Error Handling
- Comprehensive exception handling and logging
- Graceful degradation for individual record failures
- Transaction rollback on critical failures

## Important Notes

1. **Conversion vs Pipeline**: This document refers to the database migration process (MongoDB → PostgreSQL), not the genomic pipeline conversion feature within both applications.

2. **Dual Database Architecture**: The conversion maintains both PostgreSQL (Bioloop) and MongoDB (Rhythm) databases for different purposes.

3. **Data Preservation**: All original CMG data is preserved with `cmg_id` fields maintaining references to original MongoDB ObjectIds.

4. **Incremental Updates**: The conversion process is designed to be run as a complete migration, not for incremental updates.

5. **Validation**: Post-conversion validation should be performed to ensure data integrity and completeness.

## Troubleshooting

### Common Issues
1. **Connection Failures**: Verify MongoDB and PostgreSQL connections
2. **Duplicate Key Errors**: Check for existing data in target database
3. **Memory Issues**: Process data in smaller batches
4. **Referential Integrity**: Ensure all referenced records exist before creating relationships

### Logging
The conversion process provides comprehensive logging at multiple levels:
- Progress indicators for each conversion step
- Warning messages for data issues
- Error details for debugging
- Summary statistics for validation

This README serves as a complete reference for understanding and executing the CMG to Bioloop database conversion process.


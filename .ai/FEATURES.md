# Feature Index

This file provides a quick reference to all feature changelogs in the `.ai/features/` directory.

---

## Active Features

### 1. CMG Database Migration
**File:** `features/cmg-database-migration.md`  
**Scope:** Migrating CMG's MongoDB database to Bioloop's PostgreSQL database with ongoing synchronization  
**Status:** In Progress  
**Key Components:**
- Big-Bang script (one-time initial population)
- Poller script (continuous incremental sync)
- 5 concurrent pollers with cursor-based tracking
- PM2-managed in API container

**Related Docs:**
- `/CMG_BIOLOOP_DATABASE_SYNC---POLLING.md`
- `/data_sync/` directory

---

### 2. Conversions
**File:** `features/conversions.md`  
**Scope:** Genomic data conversion pipelines that transform datasets (e.g., FASTQ → BAM → VCF)  
**Status:** Implemented  
**Key Components:**
- Conversion definitions (registry of pipelines)
- Command-line programs (executable tools)
- Arguments (parameter validation)
- Celery worker execution
- UI for pipeline configuration

**Related Docs:**
- `/genome-conversion-pipelines-2026-01-11.md`
- `/workers/README_conversion_tools.md`

---

### 3. Sessions & Tracks
**File:** `features/sessions-tracks.md`  
**Scope:** Genome browser sessions and tracks for visualizing genomic data in IGV and WashU browsers  
**Status:** Implemented  
**Key Components:**
- Session management (multi-track visualization)
- Track configuration (from dataset files)
- File serving with cookie auth
- IGV and WashU browser integration
- Session staging

**Related Docs:**
- `/genome-browser-sessions-tracks-conversions-2026-01-03.md`
- `/genome-browser-igv-washu-implementation-2026-01-03.md`
- `/SESSION_STAGING_ENHANCEMENT_COMPLETE.md`

---

### 4. Staging in Production Testing
**File:** `features/staging-prod-testing.md`  
**Scope:** Issues encountered and resolved while testing staging functionality in production  
**Status:** In Progress  
**Key Components:**
- Permission errors on network filesystem mounts
- Dataset path reference corrections
- Download setup task debugging
- Production environment constraints

**Related Docs:**
- `.cursorrules` (Production restrictions)
- `workers/workers/tasks/stage.py`
- `workers/workers/tasks/download.py`

---

## Feature Status Legend

- **In Progress**: Active development, frequent changes
- **Implemented**: Core functionality complete, maintenance mode
- **On Hold**: Paused temporarily
- **Planned**: Not yet started

---

## Adding New Features

To add a new feature changelog:

1. Create `features/<feature-name>.md`
2. Use the template format from existing features
3. Add entry to this index file
4. Commit both files

---

**Last Updated:** 2026-01-16


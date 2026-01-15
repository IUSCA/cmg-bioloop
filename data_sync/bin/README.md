# Data Sync Scripts

This directory contains shell scripts for managing the CMG to Bioloop database migration and synchronization.

## 📁 Scripts Overview

### 🎯 Main User-Facing Scripts

#### `migrate.sh` - Interactive Migration Wrapper ⭐ **RECOMMENDED**
**Purpose:** Interactive menu-driven wrapper for database migration.

**When to use:** Primary entry point for all migration tasks. Provides user-friendly menu to choose between one-time migration or migration + continuous sync.

**Usage:**
```bash
# Interactive mode (recommended)
./bin/migrate.sh

# With options
./bin/migrate.sh --target-db app --clear-target-db
./bin/migrate.sh --skip-sessions
```

**Options:**
- `--target-db [sandbox|app|custom]` - Target database (default: sandbox)
- `--clear-target-db` - Clear existing data before migration
- `--clear-locks` - Release stuck process locks
- `--skip-sessions` - Skip genome browser sessions
- `-h, --help` - Show detailed help

**Interactive Menu:**
```
1) Populate database only (one-time historical migration)
2) Populate database + start continuous sync pollers
3) Exit
```

---

#### `bigbang.sh` - One-Time Historical Migration
**Purpose:** Executes the big-bang migration script (historical data population).

**When to use:** 
- When you want to run only the migration without starting pollers
- For testing/development environments
- When you want manual control over when to start continuous sync

**Usage:**
```bash
# Migrate to sandbox (default)
./bin/bigbang.sh

# Migrate to production app database
./bin/bigbang.sh --target-db app

# Clear and re-migrate
./bin/bigbang.sh --clear-target-db

# Skip sessions for faster migration
./bin/bigbang.sh --skip-sessions
```

**What it does:**
1. Creates roles (admin, operator, user)
2. Creates CMG system user
3. Populates pipeline definitions
4. Migrates users
5. Migrates datasets
6. Migrates audit logs
7. Migrates import logs
8. Migrates dataset hierarchies
9. Migrates projects
10. Migrates conversions
11. Migrates genome browser sessions
12. Initializes poller cursors

**Exit Codes:**
- `0` - Success
- `1` - Migration failed
- `2` - Another bigbang process is already running

---

#### `start_pollers.sh` - Continuous Sync Pollers
**Purpose:** Starts continuous polling processes that watch CMG for changes.

**When to use:**
- After bigbang migration completes
- To resume continuous sync after stopping pollers
- For production environments that need real-time sync

**Prerequisites:**
- Bigbang migration must have been run first (to initialize cursors)
- Cursor positions must exist in `cmg_sync_cursor` table

**Usage:**
```bash
# Start pollers for sandbox (default)
./bin/start_pollers.sh

# Start pollers for production app database
./bin/start_pollers.sh --target-db app
```

**Active Pollers (6 total):**
1. `user_roles` - User role assignments
2. `project_acl` - Project access control lists
3. `dataset_activity` - Dataset lifecycle flags
4. `dataset_metadata` - Dataset metadata changes
5. `project_metadata` - Project metadata changes
6. `session_metadata` - Genome browser session metadata

**Stopping:**
- Press `Ctrl+C` to gracefully stop all pollers
- Cursor positions are saved automatically
- Pollers can resume from where they left off

**Exit Codes:**
- `0` - Pollers stopped gracefully (Ctrl+C)
- `1` - Error during startup or runtime
- `2` - Another poller process is already running
- `3` - Cursors not initialized (run bigbang first)

---

### 🔧 Utility Scripts

#### `pull_logs_from_prod.sh` - Log Retrieval Tool
**Purpose:** Securely pulls sync logs from production host to local machine.

**Usage:**
```bash
# Pull latest log
./bin/pull_logs_from_prod.sh

# Pull last 3 logs
./bin/pull_logs_from_prod.sh --last 3

# Pull all logs
./bin/pull_logs_from_prod.sh --all

# List available logs
./bin/pull_logs_from_prod.sh --list
```

**Options:**
- `-h, --host HOST` - Production host (default: cmg-bioloop)
- `-o, --output DIR` - Local output directory (default: ./logs_from_prod)
- `-n, --last N` - Download last N log files (default: 1)
- `--all` - Download all log files
- `-l, --list` - List available logs without downloading

---

#### `entrypoint.sh` - Docker Container Entrypoint
**Purpose:** Docker container initialization script.

**When to use:** Automatically used by Docker containers. Not typically run manually.

---

## 🚀 Quick Start Guide

### First Time Setup (Sandbox)
```bash
# 1. Interactive migration (recommended for first time)
./bin/migrate.sh

# Choose option 1 or 2 from the menu
```

### First Time Setup (Production)
```bash
# 1. Migrate to production database with continuous sync
./bin/migrate.sh --target-db app

# Choose option 2 from the menu
```

### Re-running Migration (Clear and Start Fresh)
```bash
# Clear existing data and re-migrate
./bin/migrate.sh --target-db sandbox --clear-target-db
```

### Starting Pollers After Manual Bigbang
```bash
# If you ran bigbang separately, start pollers
./bin/start_pollers.sh --target-db sandbox
```

### Troubleshooting Stuck Locks
```bash
# Clear process locks if previous run crashed
./bin/migrate.sh --clear-locks
# or
./bin/bigbang.sh --clear-locks
```

### Checking Logs
```bash
# Local development
ls -lh logs/

# Production (pull logs first)
./bin/pull_logs_from_prod.sh --last 5
ls -lh logs_from_prod/
```

---

## 📊 Script Relationships

```
migrate.sh (Interactive Wrapper)
    │
    ├─── Option 1: Populate Only
    │    └─── bigbang.sh
    │         └─── src/bigbang_sync.js
    │
    └─── Option 2: Populate + Sync
         ├─── bigbang.sh
         │    └─── src/bigbang_sync.js
         └─── start_pollers.sh
              └─── src/poller_sync.js
```

---

## 🗂️ Log Files

**Location:**
- **Local development:** `data_sync/logs/`
- **Production host:** `/tmp/data_sync_logs/`
- **Pulled from prod:** `data_sync/logs_from_prod/`

**Naming:**
- `bigbang_sync_YYYY-MM-DDTHH-MM-SS.log`
- `poller_sync_YYYY-MM-DDTHH-MM-SS.log`

**Log Levels:**
- `info` - Normal operation messages
- `warn` - Warnings (e.g., skipped records, missing references)
- `error` - Errors (e.g., failed operations, validation errors)
- `debug` - Detailed debugging information

---

## 🔐 Database Targeting

### Sandbox Database (default)
```bash
./bin/migrate.sh --target-db sandbox
```
- Uses database defined in `docker-compose.sandbox.yml`
- Isolated from production
- Safe for testing

### Application Database
```bash
./bin/migrate.sh --target-db app
```
- Reads `DATABASE_URL` from `api/.env`
- Production database
- **Use with caution**

### Custom Database
```bash
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
./bin/migrate.sh --target-db custom
```
- Uses `DATABASE_URL` from environment
- For advanced configurations

---

## ⚠️ Important Notes

1. **Idempotency:** All scripts are designed to be idempotent - they can be safely re-run multiple times without creating duplicates.

2. **Process Locks:** Scripts use database-level locking to prevent concurrent runs. If a script crashes, use `--clear-locks` to release the lock.

3. **Cursor Handoff:** Bigbang initializes cursor positions to the max `updatedAt` from CMG. Pollers seamlessly continue from there.

4. **Memory Usage:** Bigbang processes large datasets in batches to avoid out-of-memory errors. Default batch size: 50 datasets at a time.

5. **Network Isolation:** Always ensure sync scripts run in the correct Docker network. See `SETUP_GUIDE.md` for details.

6. **Password Safety:** MongoDB passwords are sanitized in all log outputs.

---

## 📖 Additional Documentation

- **Setup Guide:** `../SETUP_GUIDE.md`
- **Bigbang Usage:** `../BIGBANG_SYNC_USAGE.md`
- **Poller Usage:** `../POLLER_SYNC_USAGE.md`
- **Logs Guide:** `../LOGS.md`
- **Database Targeting:** `../TARGET_DATABASE_CONFIGURATION.md`

---

**Last Updated:** 2026-01-14


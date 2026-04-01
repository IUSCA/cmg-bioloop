# Sandbox Isolation - What Changed

## Summary

The `db_sandbox` container has been **completely isolated** from the main `docker-compose.yml` to prevent it from affecting your main application containers.

## Changes Made

### 1. New Isolated Compose File
- **Created**: `data_sync/docker-compose.sandbox.yml`
- **Isolated network**: `bioloop_sandbox`
- **Isolated volume**: `bioloop_sandbox_pgdata`  
- **Container name**: `bioloop_db_sandbox`

### 2. Removed from Main Compose
- Removed `db_sandbox` service from `docker-compose.yml`
- Removed `db_sandbox_pgdata` volume from `docker-compose.yml`
- No more `--profile sync` needed

### 3. Updated All Documentation
- **README.md**: Updated startup and management commands
- **SETUP_GUIDE.md**: Updated all docker compose and docker exec commands
- **BIGBANG_SYNC_USAGE.md**: Updated script paths and container references
- **POLLER_SYNC_USAGE.md**: Updated script paths
- **Script files**: Updated help text and examples

## New Commands

### Start/Stop Sandbox (ISOLATED)
```bash
# Start (from data_sync directory)
cd data_sync
docker compose -f docker-compose.sandbox.yml up -d

# Check logs
docker compose -f docker-compose.sandbox.yml logs -f

# Stop
docker compose -f docker-compose.sandbox.yml down

# Stop and remove volume
docker compose -f docker-compose.sandbox.yml down -v
```

### Access Container
```bash
# Execute commands
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash

# Run bigbang script
docker compose -f docker-compose.sandbox.yml exec db_sandbox node src/bigbang_cmg_sync.js

# Run poller script
docker compose -f docker-compose.sandbox.yml exec db_sandbox node src/poller_sync.js

# Access PostgreSQL
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync
```

### Restart After Changes
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml restart
```

## Benefits

1. ✅ **No interference** with main application containers
2. ✅ **Isolated network** - can't accidentally affect main services
3. ✅ **Isolated volume** - completely separate PostgreSQL data
4. ✅ **Explicit control** - must explicitly start/stop sandbox
5. ✅ **Safe testing** - experiment without breaking main app

## Database Access

**External tools (DBeaver, pgAdmin, etc.):**
- **Host**: `localhost`
- **Port**: `5434` (different from main app's 5432)
- **Database**: `bioloop_sync`
- **Username**: `appuser`
- **Password**: `example`
- **Connection String**: `postgresql://appuser:example@localhost:5434/bioloop_sync`

## Important Notes

- The sandbox still uses the **same Prisma schema** from `api/prisma/schema.prisma` (mounted read-only)
- The sandbox still uses the **same migrations** from `api/prisma/migrations/` (mounted read-only)
- Only the **runtime environment** and **database** are isolated


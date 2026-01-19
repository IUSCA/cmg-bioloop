# Data Sync Setup Guide

## Overview

The `data_sync` container runs CMG-to-Bioloop database synchronization with:
- **Shared Prisma Schema**: Uses `api/prisma/schema.prisma` as single source of truth
- **Environment Variables**: Follows project `.env.default` + `.env` pattern
- **Isolated PostgreSQL**: Separate database inside container for safe testing
- **Standalone Compose File**: Uses `docker-compose.sandbox.yml` to avoid affecting main app

## ⚠️ CRITICAL: Network Isolation - Do NOT Modify

### The Problem

On production hosts, there's a tendency to add the main application's network to `docker-compose.sandbox.yml`. **This is wrong.**

### ❌ What NOT to Do

```yaml
# WRONG - Do not add main app's network
services:
  db_sandbox:
    networks:
      - default  # Don't add this
      - cmg-bioloop-2_default  # Don't add this
```

```yaml
# WRONG - Do not remove networks section
services:
  db_sandbox:
    # ... no networks defined
```

### ✅ Correct Configuration

```yaml
services:
  db_sandbox:
    networks:
      - sandbox_network  # ← Only this network

networks:
  sandbox_network:
    name: bioloop_sandbox
    driver: bridge
```

### Why Isolation Matters

1. **Safety** - Sandbox failures don't affect production
2. **Testing** - Test migrations without risk
3. **Isolation** - Independent PostgreSQL instance
4. **Clean Teardown** - Destroy sandbox without affecting main app

### How to Sync to Production

**Use the `--target-db=app` flag** instead of modifying networks:

```bash
node src/bigbang_sync.js --target-db=app
```

This reads credentials from `../api/.env` and connects to the main database via hostname (works despite network isolation).

See [TARGET_DATABASE_CONFIGURATION.md](TARGET_DATABASE_CONFIGURATION.md) for complete database targeting guide.

---

## Architecture

### Network Configuration

The sandbox container connects to **two networks**:

1. **sandbox_network** - Isolated network for the sandbox's own PostgreSQL (port 5434)
2. **bioloop_network** - Production network to access main app's PostgreSQL (172.19.0.3:5432)

```yaml
# docker-compose.sandbox.yml
services:
  db_sandbox:
    networks:
      - sandbox_network      # Own isolated DB
      - bioloop_prod_network # Access to production DB
    volumes:
      - /tmp/data_sync_logs:/tmp:rw  # Log files accessible from host

networks:
  sandbox_network:
    name: bioloop_sandbox
    driver: bridge
  bioloop_prod_network:
    external: true
    name: bioloop_network  # Production network
```

This dual-network setup allows:
- ✅ Testing migrations safely in sandbox
- ✅ Writing to production database when ready
- ✅ No interference between sandbox and production
- ✅ DataGrip access to sandbox on `localhost:5434`

### Log File Access

Sync logs are automatically written to the host filesystem:

**Container Path:** `/tmp/bigbang_sync_*.log` or `/tmp/poller_sync_*.log`  
**Host Path:** `/tmp/data_sync_logs/bigbang_sync_*.log`

```bash
# View logs from host
ls -lh /tmp/data_sync_logs/

# Tail latest bigbang log
tail -f /tmp/data_sync_logs/bigbang_sync_*.log

# View latest poller log
tail -f /tmp/data_sync_logs/poller_sync_*.log
```

This makes it easy to monitor sync progress and debug issues without needing to exec into the container.

**Cleanup:**
```bash
# Clean up old log files when no longer needed
rm /tmp/data_sync_logs/bigbang_sync_*.log
rm /tmp/data_sync_logs/poller_sync_*.log

# Or delete all logs
rm -rf /tmp/data_sync_logs/*
```

### Schema Sharing (Prisma 5.20)

```
Main App                          Sync Container
────────                          ──────────────
api/prisma/schema.prisma    →    /opt/sca/api/prisma/schema.prisma (mounted read-only)
api/prisma/migrations/      →    /opt/sca/api/prisma/migrations/ (mounted read-only)
                                       ↓
                                 package.json points via --schema flag
                                       ↓
                                 Prisma Client generated at runtime
                                       ↓
                                 Migrations applied from main app
                                       ↓
                                 Sandbox PostgreSQL (localhost:5432)
```

**Why This Works:**
- Prisma 5.20 supports `--schema` flag to point to custom location
- Schema mounted read-only prevents accidental modifications
- Single source of truth - schemas can't drift apart
- Migrations automatically applied from main app's history

**Key Configuration:**

```json
// package.json
{
  "prisma": {
    "schema": "/opt/sca/api/prisma/schema.prisma"
  },
  "scripts": {
    "prisma:generate": "prisma generate --schema=/opt/sca/api/prisma/schema.prisma",
    "prisma:migrate": "prisma migrate deploy --schema=/opt/sca/api/prisma/schema.prisma"
  }
}
```

```yaml
# docker-compose.yml
volumes:
  - ./data_sync:/opt/sca/app
  - ./api/prisma:/opt/sca/api/prisma:ro  # Read-only mount
```

### Environment Variables

Follows the same pattern as other services (`api`, `postgres`, `workers`):

```
Loading Order:
1. data_sync/.env.default  ← Defaults (committed to git)
2. data_sync/.env          ← Overrides (gitignored, create manually)

Result: Merged configuration
```

**docker-compose.yml:**
```yaml
db_sandbox:
  env_file:
    - data_sync/.env.default
    - data_sync/.env
```

Values in `.env` override matching values from `.env.default`.

### Isolated PostgreSQL

- PostgreSQL 15 runs **inside** the db_sandbox container
- Not shared with main Bioloop database
- Safe for testing migrations before production
- Data persists in Docker volume: `db_sandbox_pgdata`
- Exposed on host port **5434** for external DB tools (DBeaver, pgAdmin, etc.)

## Configuration Files

### `.env.default` (Committed)

Default values safe for development:

```bash
# Local PostgreSQL (inside container)
SYNC_PG_HOST=localhost
SYNC_PG_PORT=5432
SYNC_PG_USER=appuser
SYNC_PG_PASSWORD=example
SYNC_PG_DATABASE=bioloop_sync
DATABASE_URL=postgresql://appuser:example@localhost:5432/bioloop_sync?schema=public

# CMG MongoDB (source)
CMG_MONGO_HOST=commons3.sca.iu.edu
CMG_MONGO_PORT=27017
CMG_MONGO_DB=cmg
CMG_MONGO_USERNAME=
CMG_MONGO_PASSWORD=

NODE_ENV=development
```

### `.env` (Gitignored)

Create manually with actual credentials:

```bash
# data_sync/.env
CMG_MONGO_USERNAME=your_actual_username
CMG_MONGO_PASSWORD=your_actual_password
```

### `package.json`

Points to main app's schema:

```json
{
  "name": "cmg-bioloop-data-sync",
  "prisma": {
    "schema": "/opt/sca/api/prisma/schema.prisma"
  },
  "scripts": {
    "bigbang": "node src/bigbang_sync.js",
    "poller": "node src/poller_sync.js",
    "prisma:generate": "prisma generate --schema=/opt/sca/api/prisma/schema.prisma",
    "prisma:migrate": "prisma migrate deploy --schema=/opt/sca/api/prisma/schema.prisma"
  }
}
```

### `bin/entrypoint.sh`

Generates Prisma Client at runtime:

```bash
# Generate from main app's schema
npx prisma generate --schema=/opt/sca/api/prisma/schema.prisma

# Apply migrations from main app
npx prisma migrate deploy --schema=/opt/sca/api/prisma/schema.prisma
```

## Setup Instructions

### 1. Create Environment File

```bash
cd data_sync

# Create .env with CMG credentials
cat > .env << 'EOF'
CMG_MONGO_USERNAME=your_cmg_username
CMG_MONGO_PASSWORD=your_cmg_password
EOF

chmod 600 .env
```

### 2. Start Container

**IMPORTANT:** Use the isolated compose file to avoid affecting your main application.

```bash
# From data_sync directory
cd data_sync
docker compose -f docker-compose.sandbox.yml up -d

# Check logs
docker compose -f docker-compose.sandbox.yml logs -f
```

**Expected output:**
- PostgreSQL started
- Prisma Client generated
- Migrations applied
- "db_sandbox container is ready!"

### 3. Verify Setup

```bash
# Access container
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash

# Check schema is accessible
cat /opt/sca/api/prisma/schema.prisma | head -20

# Check migrations directory
ls -la /opt/sca/api/prisma/migrations/

# Check Prisma Client generated
ls -la node_modules/.prisma/client/

# Test database connection
psql -U appuser -d bioloop_sync -c '\dt'
```

## Usage

### One-Time Migration (Big-Bang)

The bigbang sync can write to either the **sandbox database** (for testing) or the **app's production database**.

#### Test in Sandbox (Recommended First)

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node src/bigbang_sync.js --target-db=sandbox --clear-locks
```

#### Sync to Production Database

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node src/bigbang_sync.js --target-db=app --skip-sessions --clear-locks
```

**Options:**
- `--target-db=<target>` - Target database: `sandbox` (default), `app`, or `custom`
  - `sandbox`: Writes to isolated test database
  - `app`: Reads config from `../api/.env` and writes to production database
  - `custom`: Uses `DATABASE_URL` from environment
- `--skip-sessions` - Skip genome browser sessions
- `--clear-locks` - Clear stale process locks
- `--cmg-uri=<uri>` - Override CMG MongoDB URI

**See:** `TARGET_DATABASE_CONFIGURATION.md` and `BIGBANG_SYNC_USAGE.md` for detailed documentation

### Continuous Sync (Poller)

Continuously polls CMG for updates. Like bigbang, can target sandbox or production:

```bash
cd data_sync

# Sandbox (testing)
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node src/poller_sync.js --target-db=sandbox

# Production (actual sync)
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  node src/poller_sync.js --target-db=app
```

**Pollers:**
- `user_roles` - User role changes
- `project_acl` - Project access control
- `dataset_activity` - Dataset paths and flags
- `dataset_metadata` - Dataset metadata updates
- `project_metadata` - Project metadata updates
- `session_metadata` - Session metadata updates

**See:** `TARGET_DATABASE_CONFIGURATION.md` and `POLLER_SYNC_USAGE.md` for detailed documentation

## Common Tasks

### When Main App's Schema Changes

```bash
# 1. Developer updates api/prisma/schema.prisma
# 2. Developer runs: cd api && npx prisma migrate dev --name change_name

# 3. Restart db_sandbox to apply changes
docker compose -f docker-compose.sandbox.yml restart db_sandbox

# That's it! New schema and migrations auto-applied
```

### Update CMG Credentials

```bash
# Edit .env
nano data_sync/.env

# Restart
docker compose -f docker-compose.sandbox.yml restart db_sandbox
```

### View Logs

```bash
# Follow logs
docker compose -f docker-compose.sandbox.yml logs -f db_sandbox

# Last 100 lines
docker compose -f docker-compose.sandbox.yml logs --tail 100 db_sandbox
```

### Stop Container

```bash
# Stop (keeps data)
docker compose -f docker-compose.sandbox.yml stop db_sandbox

# Remove (keeps data in volume)
docker compose -f docker-compose.sandbox.yml down

# Remove including data
docker compose -f docker-compose.sandbox.yml down -v
```

### Access Container Shell

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash

# Once inside:
node src/bigbang_sync.js --help
node src/poller_sync.js --help
npx prisma studio --schema=/opt/sca/api/prisma/schema.prisma
```

### Check Database

**From inside container:**
```bash
cd data_sync

# Via psql
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync

# Via Prisma Studio (opens on localhost:5555)
docker compose -f docker-compose.sandbox.yml exec db_sandbox npx prisma studio --schema=/opt/sca/api/prisma/schema.prisma
```

**From external DB tools (DBeaver, pgAdmin, DataGrip, etc.):**
- **Host:** `localhost` or `127.0.0.1`
- **Port:** `5434`
- **Database:** `bioloop_sync`
- **Username:** `appuser`
- **Password:** `example`

**Connection URL:**
```
postgresql://appuser:example@localhost:5434/bioloop_sync
```

## Environment Variables Reference

### PostgreSQL (Sandbox)

| Variable | Default | Description |
|----------|---------|-------------|
| `SYNC_PG_HOST` | `localhost` | Host (inside container) |
| `SYNC_PG_PORT` | `5432` | Port |
| `SYNC_PG_USER` | `appuser` | Database user |
| `SYNC_PG_PASSWORD` | `example` | Database password |
| `SYNC_PG_DATABASE` | `bioloop_sync` | Database name |
| `DATABASE_URL` | (constructed) | Full connection string |

### CMG MongoDB (Source)

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `CMG_MONGO_HOST` | `commons3.sca.iu.edu` | Yes | MongoDB host |
| `CMG_MONGO_PORT` | `27017` | Yes | MongoDB port |
| `CMG_MONGO_DB` | `cmg` | Yes | Database name |
| `CMG_MONGO_USERNAME` | (empty) | **Yes** | Set in `.env` |
| `CMG_MONGO_PASSWORD` | (empty) | **Yes** | Set in `.env` |

### Rhythm MongoDB (Optional)

### Check Current Values

```bash
# Inside container
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox env | grep -E '(SYNC_|CMG_)'

# From host
cd data_sync
cat .env.default .env 2>/dev/null | grep -v '^#' | grep -v '^$'
```

## Troubleshooting

### Container Won't Start

**Check logs:**
```bash
docker compose -f docker-compose.sandbox.yml logs db_sandbox
```

**Common causes:**
- PostgreSQL initialization failed
- Port 5432 already in use (unlikely, it's inside container)
- Volume permissions issues

**Fix:**
```bash
# Rebuild container
docker compose -f docker-compose.sandbox.yml build --no-cache db_sandbox
docker compose -f docker-compose.sandbox.yml up -d db_sandbox
```

### Schema Not Found

**Verify mount:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox ls -la /opt/sca/api/prisma/
```

**Should show:**
- `schema.prisma`
- `migrations/` directory

**If missing:**
- Check `docker-compose.yml` has the mount: `- ./api/prisma:/opt/sca/api/prisma:ro`
- Restart container

### Can't Connect to CMG MongoDB

**Check credentials:**
```bash
cat data_sync/.env | grep CMG_MONGO
```

**Test connection:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox mongosh "mongodb://$CMG_MONGO_USERNAME:$CMG_MONGO_PASSWORD@$CMG_MONGO_HOST:$CMG_MONGO_PORT/$CMG_MONGO_DB"
```

**Common causes:**
- Wrong username/password in `.env`
- Network issues (firewall, DNS)
- MongoDB server down

### Prisma Client Not Generated

**Regenerate manually:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox npm run prisma:generate
```

**If that fails:**
```bash
cd data_sync

# Check schema is accessible
docker compose -f docker-compose.sandbox.yml exec db_sandbox cat /opt/sca/api/prisma/schema.prisma

# Rebuild container
docker compose -f docker-compose.sandbox.yml build db_sandbox
docker compose -f docker-compose.sandbox.yml up -d db_sandbox
```

### Migrations Fail

**Check database:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync -c '\dt'
```

**Reset database:**
```bash
# WARNING: Deletes all data
cd data_sync
docker compose -f docker-compose.sandbox.yml down -v
docker compose -f docker-compose.sandbox.yml up -d db_sandbox
```

**Check migration history:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox npx prisma migrate status --schema=/opt/sca/api/prisma/schema.prisma
```

### Environment Variables Not Loading

**Check files exist:**
```bash
ls -la data_sync/.env*
```

**Check docker-compose:**
```bash
grep -A 3 "env_file:" docker-compose.yml | grep data_sync
```

**Restart to reload:**
```bash
docker compose -f docker-compose.sandbox.yml restart db_sandbox
```

### Process Already Running Error

**Clear locks:**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox node src/bigbang_sync.js --clear-locks
# OR
docker compose -f docker-compose.sandbox.yml exec db_sandbox node src/poller_sync.js --clear-locks
```

## File Reference

### Configuration
- `package.json` - Dependencies, schema path
- `.env.default` - Default environment variables
- `.env` - Override environment variables (create manually)
- `config/default.json` - Node config defaults
- `config/custom-environment-variables.json` - Env var mappings

### Scripts
- `src/bigbang_sync.js` - One-time migration
- `src/poller_sync.js` - Continuous sync orchestrator

### Sync Modules
- `src/sync/bigbang/*.js` - One-time migration modules
- `src/sync/pollers/*.js` - Continuous sync pollers
- `src/sync/utils/*.js` - Utility functions
- `src/sync/cursor_manager.js` - Polling cursor management
- `src/sync/error_logger.js` - Error logging
- `src/sync/process_lock_manager.js` - Process-level locking

### Docker
- `Dockerfile` - Container definition
- `bin/entrypoint.sh` - Container startup script
- `docker-compose.yml` (root) - Service configuration

### Documentation
- `README.md` - Quick overview
- `SETUP_GUIDE.md` - This file
- `BIGBANG_SYNC_USAGE.md` - Big-bang script details
- `POLLER_SYNC_USAGE.md` - Poller script details

## Production Deployment

### Setup

```bash
# 1. Navigate to production directory
cd /opt/sca/cmg/data_sync

# 2. Create .env with production credentials
nano .env
chmod 600 .env

# 3. Start container
docker compose -f docker-compose.sandbox.yml up -d db_sandbox

# 4. Verify
docker compose -f docker-compose.sandbox.yml logs -f db_sandbox
```

### Production `.env`

```bash
# data_sync/.env (production)
CMG_MONGO_HOST=prod-mongo-host.example.com
CMG_MONGO_USERNAME=prod_user
CMG_MONGO_PASSWORD=secure_prod_password

NODE_ENV=production
```

### Run Initial Migration

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox node src/bigbang_sync.js
```

### Setup Continuous Poller

**Option A: PM2 (inside container)**
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
npm install -g pm2
pm2 start src/poller_sync.js --name cmg-poller
pm2 save
```

**Option B: Docker restart policy**
```yaml
# docker-compose.yml
db_sandbox:
  restart: unless-stopped
  command: ["node", "src/poller_sync.js"]
```

### Monitoring

```bash
cd data_sync

# Check logs
docker compose -f docker-compose.sandbox.yml logs -f db_sandbox

# Check database
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync

# Check sync status
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync -c \
  "SELECT * FROM cmg_sync_cursor ORDER BY last_started_at DESC;"
```

## Best Practices

1. **Never commit `.env`** - Contains secrets
2. **Always test in dev first** - Use sandbox to test migrations
3. **Monitor logs** - Watch for errors and warnings
4. **Back up `.env`** - Store securely (password manager, vault)
5. **Use strong passwords** - Especially in production
6. **Restrict file permissions** - `chmod 600 .env`
7. **Document changes** - When modifying sync logic
8. **Test schema changes** - In sandbox before main app

## Differences from Other Services

| Aspect | api / workers | db_sandbox |
|--------|--------------|------------|
| Database | Shared PostgreSQL | Isolated PostgreSQL in container |
| Schema | Own schema file | Uses api's schema (mounted) |
| Startup | Always starts | Profile-based (optional) |
| Purpose | Run application | Sync CMG data |
| Environment | Multiple .env files | Single .env + .env.default |

## Future Upgrades

### Prisma 6.x+ (When Available)

When the project upgrades to Prisma 6.12+, you can use `prisma.config.ts`:

```typescript
// data_sync/prisma.config.ts
import { defineConfig } from "prisma/config";
import path from "node:path";

export default defineConfig({
  schema: path.join("..", "api", "prisma", "schema.prisma"),
  migrations: {
    path: path.join("..", "api", "prisma", "migrations"),
  },
});
```

This eliminates the need for `--schema` flags in commands.

## Additional Resources

- **CMG Documentation** - Original CMG system docs
- **Bioloop Documentation** - Main application docs
- **Prisma Docs** - https://www.prisma.io/docs
- **Node Config** - https://github.com/node-config/node-config


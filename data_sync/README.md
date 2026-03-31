# CMG/Xenium to Bioloop Data Sync

This directory contains the standalone legacy-data synchronization system for both CMG and Xenium sources. It runs in its own container (`db_sandbox`) with an isolated PostgreSQL instance.

## ⚠️ CRITICAL WARNING

**DO NOT modify the `networks` section in `docker-compose.sandbox.yml`**

To sync to production, use `--target-db=app` instead. See [SETUP_GUIDE.md](SETUP_GUIDE.md#-critical-network-isolation---do-not-modify) for details.

---

## Directory Structure

```
data_sync/
├── src/
│   ├── bigbang_cmg_sync.js       # CMG one-time migration script
│   ├── bigbang_xenium_sync.js    # Xenium one-time migration script
│   ├── poller_cmg_sync.js        # CMG continuous sync script
│   ├── poller_xenium_sync.js     # Xenium continuous sync script
│   ├── logger.js                 # Winston logger configuration
│   └── sync/                     # Sync modules
│       ├── cmg/                  # CMG source modules
│       ├── xenium/               # Xenium source modules
│       └── shared/               # Shared cursor/error/lock utilities
├── config/                       # Configuration files (node-config)
├── prisma/                       # Prisma schema and migrations
├── bin/
│   ├── init.sh                   # Canonical orchestrator (recommended)
│   ├── bigbang_cmg.sh
│   ├── bigbang_xenium.sh
│   ├── start_pollers_cmg.sh
│   ├── start_pollers_xenium.sh
│   └── entrypoint.sh             # Container entrypoint script
├── populate_bundles.js           # Standalone bundle population script (run on host with HSI)
├── Dockerfile                    # Container definition
├── package.json                  # Node.js dependencies (schema points to ../api/prisma)
├── .env.default                  # Default environment variables
├── .env                          # Override environment variables (create manually, gitignored)
├── README.md                     # This file
├── SETUP_GUIDE.md                # Complete setup and configuration guide
├── BIGBANG_SYNC_USAGE.md         # Big-bang script documentation
├── POLLER_SYNC_USAGE.md          # Poller script documentation
├── CONVERSION_LOGS_SYNC_USAGE.md # Conversion logs sync documentation
└── POPULATE_BUNDLES_USAGE.md     # Bundle population script documentation

Note: NO local prisma/ directory - uses main app's schema at /opt/sca/api/prisma/

##  Setup

### 1. Start the db_sandbox Container (ISOLATED)

**IMPORTANT:** The sandbox uses its own Docker Compose file to avoid interfering with your main application containers.

```bash
# From the data_sync directory
cd data_sync
docker compose -f docker-compose.sandbox.yml up -d
```

### 2. Check Container Status

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml ps
```

### 3. Run Prisma Migrations

```bash
npx prisma migrate deploy
```

### 4. Configure Environment Variables

Default values are in `.env.default`. To override (e.g., add actual CMG credentials), create a `.env` file:

```bash
# In data_sync/ directory on host
cat > .env << 'EOF'
CMG_MONGO_USERNAME=your_username
CMG_MONGO_PASSWORD=your_password
EOF
```

The `.env` file will override values from `.env.default`.

## Usage

### Canonical Orchestration (Recommended)

Use `data_sync/bin/init.sh` for day-to-day operations.

```bash
# Bigbang both sources to sandbox
./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db sandbox

# Start both pollers in managed mode
./bin/init.sh --cmg-start-pollers --xenium-start-pollers --target-db app

# Stop both pollers
./bin/init.sh --cmg-stop-pollers --xenium-stop-pollers
```

Key points:
- `--target-db` is shared across all selected actions in a run
- poller lifecycle is explicit (`start|stop|restart`) per app
- use `--dry-run` for preflight command resolution

See `bin/README.md` for the full current flag reference.

### Direct Script Usage (Advanced)

Use direct scripts when you intentionally want single-source control without the orchestrator.

#### CMG Bigbang

```bash
# Test in sandbox (recommended first)
node src/bigbang_cmg_sync.js --target-db=sandbox --clear-locks

# Sync to production database (after testing)
node src/bigbang_cmg_sync.js --target-db=app --skip-sessions --clear-locks
```

**Target Options:**
- `--target-db=sandbox` - Writes to isolated test database (default)
- `--target-db=app` - Reads from `../api/.env` and writes to production database
- `--target-db=custom` - Uses `DATABASE_URL` from environment

#### Xenium Bigbang

```bash
node src/bigbang_xenium_sync.js --target-db=sandbox --clear-locks
```

#### CMG Poller

```bash
# Sandbox (testing)
node src/poller_cmg_sync.js --target-db=sandbox

# Production (actual sync)
node src/poller_cmg_sync.js --target-db=app
```

#### Xenium Poller

```bash
node src/poller_xenium_sync.js --target-db=sandbox
```

### Bundle Population (Separate Utility)

**Note:** This is a **standalone script**, NOT part of bigbang/poller. Run directly on the remote host where HSI is available.

```bash
# On production host with HSI access (e.g., cmg-new-service1.sca.iu.edu)
cd /opt/sca/cmg/data_sync

# Dry run to see what would happen
node populate_bundles.js --target-db=app --dry-run

# Populate bundles for legacy archived datasets
node populate_bundles.js --target-db=app
```

See **POPULATE_BUNDLES_USAGE.md** for full documentation.

## Key Features

- **Shared Schema**: Uses main app's Prisma schema (single source of truth)
- **Isolated PostgreSQL**: Separate database inside container for safe testing
- **Flexible Targeting**: Write to sandbox or production database via `--target-db` flag
- **Environment Variables**: Follows project `.env.default` + `.env` pattern
- **Process Locking**: Prevents multiple instances from running simultaneously
- **Cursor-Based Sync**: Tracks last synced position for incremental updates
- **Error Logging**: Comprehensive error tracking and retry mechanisms
- **Host Log Access**: Logs automatically written to `/tmp/data_sync_logs/` on host
- **Credential Sanitization**: Database passwords hidden in all logs

## Documentation

- **SETUP_GUIDE.md** - Complete setup and configuration (includes network isolation warning)
- **BIGBANG_SYNC_USAGE.md** - One-time migration documentation
- **POLLER_SYNC_USAGE.md** - Continuous sync documentation
- **bin/README.md** - Current orchestrator and script entrypoints
- **CMG_BIOLOOP_FIELD_MAPPING.md** - Field mapping reference for testing and validation
- **POPULATE_BUNDLES_USAGE.md** - Bundle population script for legacy archived datasets
- **TARGET_DATABASE_CONFIGURATION.md** - Choose between sandbox and production databases
- **LOGS.md** - Log locations, accessing logs, pulling from production

## Quick Reference

### Check Status
```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml logs -f
docker compose -f docker-compose.sandbox.yml exec db_sandbox psql -U appuser -d bioloop_sync -c '\dt'
```

### Connect to Database
**From external tools (DBeaver, pgAdmin, etc.):**
- Host: `localhost`, Port: `5434`, Database: `bioloop_sync`, User: `appuser`, Password: `example`

### Update Credentials
```bash
nano data_sync/.env
cd data_sync
docker compose -f docker-compose.sandbox.yml restart
```

### When Schema Changes
```bash
# Main app updates api/prisma/schema.prisma
# You just need to restart:
cd data_sync
docker compose -f docker-compose.sandbox.yml restart
```

For detailed instructions, see **SETUP_GUIDE.md**.


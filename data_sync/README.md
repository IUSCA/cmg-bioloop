# CMG to Bioloop Data Sync

This directory contains the standalone CMG to Bioloop database synchronization system. It runs in its own container (`db_sandbox`) with an isolated PostgreSQL instance.

## Directory Structure

```
data_sync/
├── src/
│   ├── bigbang_sync.js          # One-time migration script
│   ├── poller_sync.js            # Continuous sync script
│   ├── logger.js                 # Winston logger configuration
│   └── sync/                     # Sync modules
│       ├── bigbang/              # Big-bang migration modules
│       ├── pollers/              # Continuous sync pollers
│       ├── utils/                # Utility functions
│       ├── connections.js        # Database connection management
│       ├── cursor_manager.js     # Polling cursor management
│       ├── error_logger.js       # Error logging
│       └── process_lock_manager.js # Process-level locking
├── config/                       # Configuration files (node-config)
├── prisma/                       # Prisma schema and migrations
├── bin/
│   └── entrypoint.sh            # Container entrypoint script
├── Dockerfile                    # Container definition
├── package.json                  # Node.js dependencies (schema points to ../api/prisma)
├── .env.default                  # Default environment variables
├── .env                          # Override environment variables (create manually, gitignored)
├── README.md                     # This file
├── SETUP_GUIDE.md                # Complete setup and configuration guide
├── BIGBANG_SYNC_USAGE.md         # Big-bang script documentation
└── POLLER_SYNC_USAGE.md          # Poller script documentation

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

### One-Time Migration (Big-Bang)

```bash
# Inside the container
node src/bigbang_sync.js --clear-locks
```

### Continuous Sync (Poller)

```bash
# Inside the container
node src/poller_sync.js
```

## Key Features

- **Shared Schema**: Uses main app's Prisma schema (single source of truth)
- **Isolated PostgreSQL**: Separate database inside container for safe testing
- **Environment Variables**: Follows project `.env.default` + `.env` pattern
- **Process Locking**: Prevents multiple instances from running simultaneously
- **Cursor-Based Sync**: Tracks last synced position for incremental updates
- **Error Logging**: Comprehensive error tracking and retry mechanisms

## Documentation

- **SETUP_GUIDE.md** - Complete setup and configuration guide
- **BIGBANG_SYNC_USAGE.md** - One-time migration documentation
- **POLLER_SYNC_USAGE.md** - Continuous sync documentation

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


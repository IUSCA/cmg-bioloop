# Target Database Configuration

The sync scripts (bigbang and poller) can write to either:
- **Sandbox DB** - Isolated PostgreSQL for testing
- **App DB** - Main application database (production or development)
- **Custom DB** - Any PostgreSQL database you specify

## How It Works

The scripts support a `--target-db` flag that automatically determines which database to use:

| Flag Value | Database | How DATABASE_URL is determined |
|------------|----------|-------------------------------|
| `sandbox` (default) | Sandbox PostgreSQL | Uses `data_sync/.env` or defaults to `localhost:5432/bioloop_sync` |
| `app` | Main app database | Automatically reads from `../api/.env` |
| `custom` | Custom database | Uses `DATABASE_URL` from current environment |

---

## Method 1: Using `--target-db` Flag (Recommended)

### **Scenario 1: Test in Sandbox (Default)**

```bash
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Explicit sandbox (same as no flag)
node src/bigbang_sync.js --target-db=sandbox

# Or just omit the flag (defaults to sandbox)
node src/bigbang_sync.js
```

Output:
```
[OK] Target database: sandbox
[OK] Database URL: postgresql://appuser:***@localhost:5432/bioloop_sync
[OK] Successfully connected to Bioloop PostgreSQL
```

### **Scenario 2: Sync to App Database**

```bash
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Automatically reads DATABASE_URL from ../api/.env
node src/bigbang_sync.js --target-db=app
```

Output:
```
[OK] Target database: app
[OK] Database URL: postgresql://appuser:***@postgres:5432/app
[OK] Successfully connected to Bioloop PostgreSQL
```

**What happens:**
1. Script reads `../api/.env` file at runtime
2. Extracts `DATABASE_URL` or constructs it from `DATABASE_USER`, `DATABASE_PASSWORD`, etc.
3. Uses it to connect to the app's database

### **Scenario 3: Custom Database**

```bash
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Set DATABASE_URL manually
export DATABASE_URL="postgresql://user:pass@custom-host:5432/custom_db?schema=public"
node src/bigbang_sync.js --target-db=custom
```

---

## Method 2: Environment Variable (Legacy)

You can still set `DATABASE_URL` directly in `.env`:

```bash
# In data_sync/.env
DATABASE_URL=postgresql://appuser:password@postgres:5432/app?schema=public

# Run without --target-db flag (uses .env value)
node src/bigbang_sync.js
```

---

## Complete Examples

### Example 1: Full Sandbox Test

```bash
cd data_sync

# Start sandbox
docker compose -f docker-compose.sandbox.yml up -d

# Enter container
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Run to sandbox (default)
node --max-old-space-size=6144 src/bigbang_sync.js \
  --cmg-uri="mongodb://user:pass@cmg-host:27017/cmg"
```

### Example 2: Sync CMG to App's Production DB

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Reads DATABASE_URL from ../api/.env automatically
node --max-old-space-size=6144 src/bigbang_sync.js \
  --target-db=app \
  --cmg-uri="mongodb://prod_user:prod_pass@prod-cmg-host:27017/cmg"
```

### Example 3: Run Poller Against App DB

```bash
cd data_sync
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
cd /opt/sca/app

# Poller will continuously sync to app's database
node src/poller_sync.js --target-db=app
```

### Example 4: Test Complete Flow

```bash
# Step 1: Test in sandbox
node src/bigbang_sync.js --target-db=sandbox

# Step 2: Verify data in sandbox
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  psql -U appuser -d bioloop_sync -c "SELECT COUNT(*) FROM dataset;"

# Step 3: If successful, sync to app DB
node src/bigbang_sync.js --target-db=app

# Step 4: Verify data in app DB
docker compose exec postgres \
  psql -U appuser -d app -c "SELECT COUNT(*) FROM dataset;"
```

---

## Network Connectivity

Despite network isolation (see [SETUP_GUIDE.md](SETUP_GUIDE.md#-critical-network-isolation---do-not-modify)), the sandbox **can reach the main database**:

```bash
# Test from inside sandbox container
docker compose -f docker-compose.sandbox.yml exec db_sandbox bash
psql -h postgres -U appuser -d app -c "SELECT 1;"
```

This works because Docker allows service name resolution across compose projects.

---

## Verification Commands

### Check Which DB Will Be Used

```bash
# Test sandbox connection
node -e "const {setDatabaseUrl} = require('./src/utils/db_config'); console.log('Sandbox DB:', setDatabaseUrl('sandbox'));"

# Test app connection
node -e "const {setDatabaseUrl} = require('./src/utils/db_config'); console.log('App DB:', setDatabaseUrl('app'));"
```

### Verify After Running Sync

```bash
# Check sandbox DB
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  psql -U appuser -d bioloop_sync -c "SELECT COUNT(*) as total_datasets, MAX(updated_at) as last_update FROM dataset;"

# Check app DB
docker compose exec postgres \
  psql -U appuser -d app -c "SELECT COUNT(*) as total_datasets, MAX(updated_at) as last_update FROM dataset;"
```

---

## Safety Best Practices

### 1. Always Test in Sandbox First

```bash
# Step 1: Test migration logic
node src/bigbang_sync.js --target-db=sandbox --skip-sessions

# Step 2: Verify data quality
docker compose -f docker-compose.sandbox.yml exec db_sandbox \
  psql -U appuser -d bioloop_sync

# Step 3: If successful, run to production
node src/bigbang_sync.js --target-db=app
```

### 2. Backup Before Production Sync

```bash
# Backup app database before big-bang sync
docker compose exec postgres pg_dump -U appuser app > backup_$(date +%Y%m%d_%H%M%S).sql
```

### 3. Dry Run Check

```bash
# Verify which database will be targeted without actually running
node -e "
  require('./src/utils/db_config').setDatabaseUrl('app');
  console.log('Will connect to:', process.env.DATABASE_URL);
  console.log('Database name:', process.env.DATABASE_URL.match(/\/([^?]+)/)[1]);
"
```

---

## Troubleshooting

### Error: "Cannot read ../api/.env"

**Cause:** File doesn't exist or permissions issue

**Solution:**
```bash
# Check if file exists
ls -la /opt/sca/api/.env

# If missing, use custom target with explicit DATABASE_URL
export DATABASE_URL="postgresql://appuser:password@postgres:5432/app?schema=public"
node src/bigbang_sync.js --target-db=custom
```

### Error: "Connection refused" when using --target-db=app

**Cause:** Cannot reach main postgres container

**Solution:**
```bash
# Test connectivity from sandbox container
ping postgres
nc -zv postgres 5432
psql -h postgres -U appuser -d app -c "SELECT 1;"
```

### Error: "Database app does not exist"

**Cause:** App database not initialized

**Solution:**
```bash
# Run Prisma migrations on app DB first
cd /opt/sca/api
npx prisma migrate deploy
```

### Error: "Invalid target database: xyz"

**Cause:** Typo in `--target-db` value

**Valid values:**
- `--target-db=sandbox`
- `--target-db=app`
- `--target-db=custom`

---

## Production Deployment Checklist

Before running sync to production:

- [ ] ✅ Tested successfully in sandbox with `--target-db=sandbox`
- [ ] ✅ Verified `../api/.env` contains correct production credentials
- [ ] ✅ Tested connectivity: `psql -h postgres -U appuser -d app -c "SELECT 1;"`
- [ ] ✅ Backed up production database: `pg_dump -U appuser app > backup.sql`
- [ ] ✅ Confirmed CMG MongoDB credentials are for production
- [ ] ✅ Notified team of sync operation
- [ ] ✅ Have rollback plan ready (restore from backup)
- [ ] ✅ Scheduled during low-traffic period
- [ ] ✅ Monitoring/logging enabled
- [ ] ✅ Confirmed target database: `node -e "require('./src/utils/db_config').setDatabaseUrl('app'); console.log(process.env.DATABASE_URL)"`

---

## Quick Command Reference

```bash
# Test in sandbox (default)
node src/bigbang_sync.js

# Explicitly target sandbox
node src/bigbang_sync.js --target-db=sandbox

# Sync to app's database (reads from ../api/.env)
node src/bigbang_sync.js --target-db=app

# Sync to custom database
export DATABASE_URL="postgresql://user:pass@host:5432/db"
node src/bigbang_sync.js --target-db=custom

# Start poller for app database
node src/poller_sync.js --target-db=app

# Start poller for sandbox (testing)
node src/poller_sync.js --target-db=sandbox

# Run with all safety options
node src/bigbang_sync.js --target-db=app --skip-sessions --clear-locks
```

---

## Understanding the Utility Function

The `src/utils/db_config.js` utility handles all the logic:

```javascript
// When you call:
node src/bigbang_sync.js --target-db=app

// The script does:
const { setDatabaseUrl } = require('./utils/db_config');
const databaseUrl = setDatabaseUrl('app'); // Reads ../api/.env
process.env.DATABASE_URL = databaseUrl;    // Sets for Prisma
const prisma = new PrismaClient();         // Uses DATABASE_URL
```

**Benefits:**
- No need to manually copy/paste DATABASE_URL
- Automatically stays in sync with app's configuration
- Reduces configuration errors
- Clear separation between sandbox and production

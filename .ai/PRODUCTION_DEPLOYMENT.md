# Production Deployment Guide

**Last Updated:** 2026-01-18

This document describes the production deployment architecture and differences from local development.

---

## 🏗️ Production Architecture

### Multi-Host Deployment

Production uses a **multi-host architecture** with services distributed across different servers:

#### Host 1: Service Host (UI, API)
- **Hostname:** `cmg-new-service1.sca.iu.edu`
- **Services:** UI, API, postgres, monitoring (Grafana, Prometheus)
- **Deployment Method:** Docker Compose
- **Compose File:** `docker-compose-prod.yml`
- **Repository Path:** `/opt/sca/cmg`

#### Host 2: Worker Hosts
- **Hostnames:** `colo`, `mmge2`, `mmge3`, etc.
- **Services:** Celery workers, watch scripts, conversion workers, metrics
- **Deployment Method:** **PM2** (NOT Docker)
- **Repository Path:** `/opt/sca/cmg-bioloop`
- **Python Environment:** Poetry

#### Host 3: Secure Download Host
- **Location:** Same as worker hosts (or similar hosts with `/N/...` access)
- **Services:** secure_download API, nginx
- **Deployment Method:** Docker Compose
- **Compose File:** `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml` (⚠️ **READ-ONLY** access)
- **Repository Path:** `/opt/sca/cmg-bioloop/secure_download` (mounted into container)

**Note:** Workers do NOT run on `cmg-new-service1.sca.iu.edu`

---

## 🐳 Docker Compose Configuration

### Main Services (docker-compose-prod.yml)

```yaml
services:
  ui:
    # Node.js 20, builds UI
  api:
    # Node.js 21, runs API with Prisma
    volumes:
      - ${FILESYSTEM_BASE_DIR_SCRATCH}:${FILESYSTEM_MOUNT_DIR_SCRATCH}
      - ${FILESYSTEM_BASE_DIR_PROJECT}:${FILESYSTEM_MOUNT_DIR_PROJECT}
  postgres:
    # PostgreSQL 14.5
  grafana, prometheus, postgres_exporter:
    # Monitoring stack
```

### Secure Download

**Location:** `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml` (⚠️ **READ-ONLY** access)

```yaml
services:
  api:
    # secure_download API - handles file uploads and downloads
    build:
      context: /opt/sca/cmg-bioloop/secure_download
    volumes:
      - /opt/sca/cmg-bioloop/secure_download/:/opt/sca/app
      - api_modules:/opt/sca/app/node_modules
      # ⚠️ MISSING: /N/scratch/cmguser/cmg-bioloop/uploads (needs write access)
    expose:
      - 3080
  
  nginx-cmg-bioloop:
    # Reverse proxy for secure_download
    volumes:
      - /N/scratch/cmguser/cmg-bioloop/:/N/scratch/cmguser/cmg-bioloop/:ro
      # ... nginx config volumes ...
```

**⚠️ CRITICAL ISSUE:** The `api` service (secure_download) needs write access to `/N/scratch/cmguser/cmg-bioloop/uploads` but currently doesn't have it mounted. This causes upload errors: `EACCES: permission denied, mkdir '/N'`

**Note:** The compose file in the repository (`secure_download/docker-compose-prod.yml`) is for reference only and is NOT the actual deployment file.

---

## 🔧 Workers Deployment (PM2)

### Location
Workers run on a **separate host** from the web services and do NOT use Docker.

### Setup Process

1. **Environment Setup**
```bash
# Add to ~/.modules
module load python/3.10.5
```

2. **Install Dependencies**
```bash
cd /opt/sca/cmg-bioloop/workers  # on worker hosts
poetry export --without-hashes --format=requirements.txt > requirements.txt
pip install -r requirements.txt
```

3. **Configure Environment**
```bash
# Create/update workers/.env
# Set APP_ENV=production
# Configure paths, API tokens, etc.
```

4. **Start Workers with PM2**
```bash
cd /opt/sca/cmg-bioloop/workers  # or /opt/sca/cmg/workers on service host
poetry shell  # Enter poetry virtual environment
pm2 start ecosystem.config.js
pm2 save  # Optional: save PM2 process list
```

### PM2 Configuration (ecosystem.config.js)

The workers are defined in `workers/ecosystem.config.js`:

```javascript
apps: [
  {
    name: "celery_worker",
    script: "python",
    args: "-m celery -A workers.celery_app worker --loglevel INFO -O fair --pidfile celery_worker.pid --hostname 'cmg-test-celery-w1@%h' --autoscale=8,2 --queues 'cmg-test.sca.iu.edu.q'",
    // ... logging and restart config
  },
  {
    name: "conversions_worker",
    script: "python",
    args: "-m celery -A workers.conversions_app worker --loglevel INFO -O fair --pidfile conversions_worker.pid --hostname 'cmg-test-celery-w1@%h' --autoscale=8,2 --queues 'conversion.cmg-test.sca.iu.edu.q'",
    // ... logging and restart config
  },
  {
    name: "watch",
    script: "python",
    args: "-u -m workers.scripts.watch",
    // ... logging and restart config
  },
  {
    name: "metrics",
    script: "python",
    args: "-u -m workers.scripts.metrics",
    cron_restart: "00 21 * * *",  // Runs daily at 9 PM
    autorestart: false,
    // ... logging and restart config
  }
]
```

### PM2 Commands

```bash
# Start all workers
pm2 start ecosystem.config.js

# Check status
pm2 status
pm2 list

# View logs
pm2 logs celery_worker
pm2 logs conversions_worker
pm2 logs watch

# Restart workers
pm2 restart celery_worker
pm2 restart all

# Stop workers
pm2 stop celery_worker
pm2 stop all

# Delete from PM2
pm2 delete celery_worker
pm2 delete all

# Save PM2 process list (survives reboots)
pm2 save

# Restore saved processes
pm2 resurrect
```

---

## 📂 Production Paths

### Web Services Host (UI/API/secure_download)

**Local paths (inside containers):**
- `/opt/sca/app` - Application code (mounted from host)
- `/opt/sca/data` - Temporary/staged data (volume)

**Mounted external paths:**
- `${FILESYSTEM_BASE_DIR_SCRATCH}` → `${FILESYSTEM_MOUNT_DIR_SCRATCH}`
  - Example: `/N/scratch/cmguser` → `/N/scratch/cmguser`
- `${FILESYSTEM_BASE_DIR_PROJECT}` → `${FILESYSTEM_MOUNT_DIR_PROJECT}`
  - Example: `/N/project/CMG-SCA` → `/N/project/CMG-SCA`
- `${SLATE_DATA_DIR}` → `${SLATE_DATA_DIR}` (read-only)
- `${SCRATCH_DATA_DIR}` → `${SCRATCH_DATA_DIR}` (read-write, for uploads)

### Workers Host

**Production paths (from `workers/workers/config/production.py`):**
```python
paths: {
    'root': '/N/scratch/cmguser',
    'RAW_DATA': {
        'stage': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data',
        'bundle': {
            'generate': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data',
            'stage': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/bundles/raw_data',
        },
        'qc': '/N/scratch/cmguser/cmg-bioloop/qc/raw_data'
    },
    'DATA_PRODUCT': {
        'stage': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
        'bundle': {
            'generate': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
            'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/data_products',
        },
    },
    'download_dir': '/N/scratch/cmguser/cmg-bioloop/production/downloads',
    'conversion': {
        # Conversion reports paths
    },
}
```

**Upload directory:**
- `/N/scratch/cmguser/cmg-bioloop/uploads` - Where uploaded datasets are stored

---

## 🔐 Environment Variables

### API Service (api/.env)

```bash
NODE_ENV=production
UPLOAD_DIR=/N/scratch/cmguser/cmg-bioloop/uploads
DATA_ROOT=/N/scratch/cmguser/cmg-bioloop/stage
# ... database, OAuth, etc.
```

### Secure Download (secure_download/.env)

```bash
NODE_ENV=production
JWKS_URI=https://oauth-dev.sca.iu.edu/oauth/jwks
# Note: upload path comes from API request body, not config
```

### Workers (workers/.env)

```bash
APP_ENV=production
APP_API_TOKEN=<service_account_token>
API_BASE_URL=https://cmg-test.sca.iu.edu/api  # NO trailing slash
# ... queue, mongo, etc.
```

---

## 🚀 Deployment Workflow

### Initial Deployment

1. **On Service Host (`cmg-new-service1.sca.iu.edu`):**
```bash
cd /opt/sca/cmg
# ⚠️ Git pull NOT allowed - user must do it
# Update docker-compose-prod.yml if needed
# Ensure environment variables are set

# ⚠️ Docker commands NOT allowed - provide commands for user:
# docker compose -f docker-compose-prod.yml down
# docker compose -f docker-compose-prod.yml up -d --build
```

2. **On Worker Hosts (`colo`, `mmge2`, etc.):**
```bash
cd /opt/sca/cmg-bioloop/workers
# ⚠️ Git pull NOT allowed - user must do it

# Update dependencies if needed
poetry install

# ⚠️ PM2 restart NOT allowed - provide commands for user:
# poetry shell
# pm2 restart all
```

**Note:** You cannot execute git pull, docker commands, or PM2 restart commands in production. Provide the commands for the user to run.

### Code Updates

**Web Services (API/UI):**
- Most changes use Hot Module Replacement (HMR) - no restart needed
- Config changes require restart: `docker compose -f docker-compose-prod.yml restart api`
- Prisma schema changes: `docker compose -f docker-compose-prod.yml exec api npx prisma generate`

**Workers:**
- Most changes use Hot Module Replacement - no restart needed
- Config changes require restart: `pm2 restart celery_worker`
- Dependency changes: `poetry install && pm2 restart all`

---

## 📊 Monitoring

### Logs

**Web Services (Service Host):**
```bash
# ⚠️ Agent cannot run docker commands - user must run:
# docker compose -f docker-compose-prod.yml logs -f api

# Agent CAN read application logs directly:
tail -f /opt/sca/cmg/api/logs/app.log  # if logs are mounted
```

**Workers (Worker Hosts):**
```bash
# Agent CAN view PM2 logs:
pm2 logs celery_worker
pm2 logs conversions_worker
pm2 logs watch
pm2 monit  # Real-time monitoring

# Agent CAN read log files directly (as configured in ecosystem.config.js):
tail -f ../logs/workers/celery_worker.log
tail -f ../logs/workers/conversions_worker.err
```

### Metrics

- **Grafana:** Accessible via nginx proxy
- **Prometheus:** Collects metrics from postgres_exporter and application
- **PM2 Monitoring:** `pm2 monit`

---

## ⚠️ Production Restrictions

See `.ai/PRODUCTION_ENVIRONMENT.md` and `.cursorrules` for detailed restrictions.

### ❌ NEVER Allowed (Unless User Explicitly Permits):

**File System:**
- **NEVER** write/edit/delete anything in `/N/...` paths (read-only/debugging operations OK)
- **ALWAYS** delete temporary files from `/tmp` after use

**Database:**
- **NEVER** reset the database on service host

**Docker:**
- **NEVER** run ANY docker commands (not even read-only: `docker ps`, `docker logs`)
- **NEVER** exec into containers or restart them

**Workers:**
- **NEVER** restart/kill/stop workers (`pm2 restart`, `pm2 stop`, `pm2 delete`)
- Can view: `pm2 logs`, `pm2 monit`, `pm2 status`
- Can use: `poetry shell`

**System Services:**
- **NEVER** restart services (docker, nginx, apache, etc.)
- **NEVER** use sudo (no sudo access)

**Git:**
- **NEVER** do write operations (`git pull`, `git push`, `git commit`, `git merge`)
- Can do read-only: `git status`, `git diff`, `git log`, `git reflog`

**nodemon:**
- **NEVER** use in production (except UI service)

### ✅ Always Allowed:

- Safe read-only operations for debugging
- Viewing logs, process status
- File inspection (`cat`, `grep`, `ls`, etc., except `.env` files)
- Git read-only operations
- `poetry shell` in workers directory

---

## 🔄 Service Restart Patterns

**⚠️ Agent cannot execute these commands in production. Provide them to user to run.**

### Web Services (Docker)

```bash
# On service host (cmg-new-service1.sca.iu.edu)
cd /opt/sca/cmg

# Restart specific service
docker compose -f docker-compose-prod.yml restart api

# Restart all services
docker compose -f docker-compose-prod.yml restart

# Full rebuild (for Dockerfile changes)
docker compose -f docker-compose-prod.yml up -d --build api
```

### Workers (PM2)

```bash
# On worker hosts (colo, mmge2, etc.)
cd /opt/sca/cmg-bioloop/workers
poetry shell

# Restart specific worker
pm2 restart celery_worker

# Restart all workers
pm2 restart all

# Reload (zero-downtime restart)
pm2 reload all
```

---

## 🐛 Troubleshooting

### Upload Errors

**Error:** `EACCES: permission denied, mkdir '/N'`

**Cause:** The `secure_download` api container (in `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml`) doesn't have write access to `/N/scratch/cmguser/cmg-bioloop/uploads`

**Current state:**
- Only nginx has `/N/scratch/cmguser/cmg-bioloop/:ro` (read-only)
- api service has no `/N/...` mounts at all

**Fix:** Someone with write access to `/opt/sca/docker/scripts/cmg-new-download/docker-compose-prod.yml` needs to add to the `api` service:
```yaml
api:
  volumes:
    - /opt/sca/cmg-bioloop/secure_download/:/opt/sca/app
    - api_modules:/opt/sca/app/node_modules
    - /N/scratch/cmguser/cmg-bioloop/uploads:/N/scratch/cmguser/cmg-bioloop/uploads  # ADD THIS
```

Then restart the secure_download containers.

### Worker Connection Issues

**Error:** Workers can't connect to API/Queue

**Check:**
1. API is accessible from workers host: `curl https://cmg-test.sca.iu.edu/api/health`
2. Queue is accessible: Check `QUEUE_URL` in `workers/.env`
3. Auth token is valid: Ask user to regenerate from inside api container on service host (`cmg-new-service1.sca.iu.edu`):
   ```bash
   docker compose -f docker-compose-prod.yml exec api node src/scripts/issue_token.js svc_tasks
   ```

### PM2 Process Not Starting

**Check:**
1. Python environment: `which python` (should be Poetry's Python)
2. Dependencies installed: `poetry install`
3. Environment variables: `cat workers/.env`
4. PM2 logs: `pm2 logs <process_name> --err`

---

## 📚 Related Documentation

- [Production Environment Warnings](.ai/PRODUCTION_ENVIRONMENT.md)
- [AI Protocol](.ai/AI_PROTOCOL.md)
- [Worker Overview](docs/worker/overview.md)
- [Docker Installation](docs/installation/install-docker.md)

---

**Last Updated:** 2026-01-18


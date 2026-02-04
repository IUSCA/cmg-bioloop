# Parallel Instances - Port Configuration

**Feature Scope:** Running multiple Bioloop instances simultaneously on the same machine for testing and development.

**Status:** Active

**Repository:** cmg-bioloop-3

**Related Documentation:**
- `docker-compose.yml` (port mappings)
- `.ai/PRODUCTION_DEPLOYMENT.md` (production architecture)
- `.ai/bioloop/architecture.md` (service components)

---

## Overview

This repository (cmg-bioloop-3) is configured to run alongside other Bioloop instances (`cmg-bioloop` and `cmg-bioloop-2`) on the same machine without port conflicts. All host-side ports have been changed from default values while internal Docker container ports remain standard.

### Purpose

- Enable testing of data sync processes between instances
- Allow development and testing without disrupting running instances
- Support poller testing and multi-instance workflows

---

## Port Configuration

### cmg-bioloop-3 (This Instance) - Non-Default Ports

| Service | Internal Port | Host Port | Default Port | Status |
|---------|---------------|-----------|--------------|--------|
| UI (HTTPS) | 443 | **8443** | 443 | ✅ Changed |
| API | 3030 | **3031** | 3030 | ✅ Changed |
| PostgreSQL | 5432 | **5434** | 5432 | ✅ Changed |
| MongoDB | 27017 | **27019** | 27017 | ✅ Changed |
| RabbitMQ Mgmt | 15672 | **15673** | 15672 | ✅ Changed |
| Secure Download | 3060 | **3061** | 3060 | ✅ Changed |
| Docs (VitePress) | 5174 | **5174** | 5174 | ✅ Changed |
| Jupyter | 8888 | **8889** | 8888 | ✅ Changed |
| Grafana | 3000 | **3001** | 3000 | ✅ Changed |

**Note:** Internal container ports remain unchanged. Only host-side port mappings were modified in `docker-compose.yml`.

### Other Instances on Same Machine

**cmg-bioloop-2:**
- UI: 443, API: 3030, Postgres: 5432, MongoDB: 27017, RabbitMQ: 15672, Secure Download: 3060

**cmg-bioloop:**
- (If running) Uses default ports

---

## Docker Configuration

### Project Name

```yaml
name: cmg-bioloop-3
```

**Location:** `docker-compose.yml` (line 4)

This ensures:
- Container names are prefixed with `cmg-bioloop-3-*`
- Docker network is `cmg-bioloop-3_default`
- No naming conflicts with other instances

### Container Name Pattern

All containers follow the pattern: `cmg-bioloop-3-{service}-1`

Examples:
- `cmg-bioloop-3-api-1`
- `cmg-bioloop-3-postgres-1`
- `cmg-bioloop-3-celery_worker-1`

---

## Files Modified

### Core Configuration

**docker-compose.yml:**
- Changed project name from `cmg-bioloop-2` to `cmg-bioloop-3`
- Updated all host-side port mappings (left side of colon)
- Internal ports unchanged (right side of colon)

**Environment Files:**
- `ui/.env.default`:
  - `VITE_CAS_RETURN`, `VITE_GOOGLE_RETURN`, `VITE_CILOGON_RETURN`, `VITE_MICROSOFT_RETURN`: Updated OAuth callbacks to use port 8443
  - `VITE_UPLOAD_API_BASE_PATH`: Changed to `https://localhost:8443`
- `api/.env.default`:
  - `DOWNLOAD_SERVER_BASE_URL`: Changed to `http://localhost:3061`
- `data_sync/.env.default`:
  - `SYNC_PG_PORT`: Updated to 5434 (for connecting to this instance's Postgres)

### Helper Scripts

Updated container name references from `cmg-bioloop-2-*` to `cmg-bioloop-3-*`:
- `clear_workflows.sh`
- `db/mongo/clear_mongo.sh`
- `workers/clear_celery_queue.sh`
- All files in `genomic_data_testing/` (13 shell scripts)

### NOT Modified (Intentional)

**Shared filesystem paths remain unchanged:**
- `/N/scratch/cmguser/cmg-bioloop/` paths in production configs
- `workers/workers/config/production.py` paths
- `data_sync/src/sync/constants.js` output directories
- These are intentionally shared across all instances

---

## Access URLs

### This Instance (cmg-bioloop-3)

- **UI:** https://localhost:8443
- **API:** http://localhost:3031
- **API Health:** http://localhost:3031/health
- **RabbitMQ Management:** http://localhost:15673
- **Secure Download:** http://localhost:3061
- **Docs:** http://localhost:5174

### Testing API

```bash
# Health check
curl http://localhost:3031/health

# Dataset list
curl http://localhost:3031/api/datasets | jq '.datasets | length'
```

---

## Verification Commands

### Check Container Status
```bash
docker compose ps
```

### Check Port Conflicts
```bash
lsof -iTCP -sTCP:LISTEN -P -n | grep -E "(8443|3031|5434|27019|15673|3061)"
```

### Verify All Instances
```bash
docker ps --filter "name=cmg-bioloop" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Check Networks
```bash
docker network ls | grep bioloop
```

Expected networks:
- `cmg-bioloop_default`
- `cmg-bioloop-2_default`
- `cmg-bioloop-3_default`

---

## Changelog

### 2026-01-30 - Initial Port Configuration

**Decision:** Configure cmg-bioloop-3 repository to use non-conflicting ports for parallel instance testing.

**Implementation:**
- Changed all host-side port mappings in `docker-compose.yml`
- Updated environment files with new port references
- Changed Docker project name from `cmg-bioloop-2` to `cmg-bioloop-3`
- Updated helper scripts with new container name pattern

**Rationale:**
- Enable testing of data sync processes between instances
- Allow poller testing without disrupting running instances
- Support development without stopping production-like test instance

**Port Selection Strategy:**
- Incremented ports logically from cmg-bioloop-2 values
- Avoided common service ports
- Maintained easy-to-remember pattern (e.g., 3030→3031, 5432→5434)
- Changed MongoDB from initial 27018 to 27019 due to conflict detection

**Files Changed:**
- `docker-compose.yml` (project name, all port mappings)
- `ui/.env.default` (OAuth callbacks, upload API base path)
- `api/.env.default` (download server URL)
- `data_sync/.env.default` (Postgres port)
- All helper scripts (container name references)
- 13 test scripts in `genomic_data_testing/`

**Verification:**
- ✅ All containers started successfully
- ✅ No port conflicts detected with other instances
- ✅ API health check passes
- ✅ UI accessible at https://localhost:8443
- ✅ Workers (celery, conversion, watch) running
- ✅ All services healthy (API, Rhythm, Signet, Secure Download)

**Conflicts Resolved:**
- Fixed git merge conflicts in `api/config/default.json`
- Fixed git merge conflicts in `api/config/custom-environment-variables.json`
- Changed MongoDB port from 27018→27019 (27018 was in use)

---

## Important Notes

### Internal vs Host Ports

**Internal ports (inside containers) are UNCHANGED:**
- API still listens on 3030 inside container
- Postgres still listens on 5432 inside container
- etc.

**Only host-side mappings changed:**
- Format: `HOST_PORT:CONTAINER_PORT`
- Example: `127.0.0.1:3031:3030` maps host 3031 to container 3030

### Service Communication

Services communicate using **internal Docker network** with standard ports:
- UI → API: Uses `http://api:3030` (internal)
- API → Postgres: Uses `postgres:5432` (internal)
- Workers → Queue: Uses `queue:5672` (internal)

External access uses the changed host ports.

### Data Sync Implications

When running data_sync processes:
- Must specify correct Postgres port (5434) via `SYNC_PG_PORT`
- API requests should use port 3031
- Test scripts already updated to use new API port

### Production vs Development

**Production:** Uses default ports, single instance per host
**Development (this setup):** Uses modified ports, multiple instances on same machine

---

## Troubleshooting

### Port Already in Use

If you see "address already in use" errors:
1. Check what's using the port: `lsof -iTCP:PORT -sTCP:LISTEN`
2. Verify other Bioloop instances: `docker ps --filter "name=bioloop"`
3. Ensure port assignments match this document

### Container Name Conflicts

If you see container name conflicts:
1. Verify project name in `docker-compose.yml` is `cmg-bioloop-3`
2. Check Docker networks: `docker network ls | grep bioloop`
3. Remove old containers: `docker compose down` then `docker compose up -d`

### Service Can't Connect

If services can't reach each other:
1. Check they're on same Docker network: `docker network inspect cmg-bioloop-3_default`
2. Use internal service names (not localhost) for inter-service communication
3. Verify environment variables use internal ports for Docker services

---

**Last Updated:** 2026-01-30

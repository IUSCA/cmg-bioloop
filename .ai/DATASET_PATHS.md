# Dataset Paths: Production vs Docker

## Overview

Dataset paths differ between **production** and **docker (local development)** environments. This document explains the path structure and how to work with them correctly.

---

## Environment Detection

The environment is determined by the `APP_ENV` environment variable:

- **Production:** `APP_ENV=production`
- **Docker/Local:** `APP_ENV=docker` (or unset)

---

## Path Differences by Component

### Workers (Python)

**Configuration Files:**
- Production: `workers/workers/config/production.py`
- Docker: `workers/workers/config/docker.py`
- Common: `workers/workers/config/common.py` (shared defaults)

**Production Paths** (`/N/...` - Network filesystem):
```python
config = {
    'paths': {
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
        'download_dir': '/N/scratch/cmguser/cmg-bioloop/download',
        'conversion': {
            'output': '/N/scratch/cmguser/cmg-bioloop/conversions/output'
        }
    }
}
```

**Docker Paths** (`/opt/sca/data` - Mounted volume):
```python
config = {
    'paths': {
        'RAW_DATA': {
            'stage': '/opt/sca/data/stage/raw_data',
            'bundle': {
                'generate': '/opt/sca/data/bundle/raw_data',
                'stage': '/opt/sca/data/bundle/raw_data',
            },
            'qc': '/opt/sca/data/qc/raw_data'
        },
        'DATA_PRODUCT': {
            'stage': '/opt/sca/data/stage/data_products',
            'bundle': {
                'generate': '/opt/sca/data/bundle/data_products',
                'stage': '/opt/sca/data/bundle/data_products',
            },
        },
        'download_dir': '/opt/sca/data/download',
        'conversion': {
            'output': '/opt/sca/data/conversion/output'
        }
    }
}
```

---

### API (Node.js)

**Configuration Files:**
- Production: `api/config/production.json`
- Docker: `api/config/docker.json`
- Default: `api/config/default.json`

**Production:**
```json
{
  "mode": "production",
  "conversion": {
    "output_directory": "/N/project/CMG-SCA/production/conversion",
    "reports_base_path": "/opt/sca/project/ingestion_source_dir/CMG-SCA/production/conversion"
  }
}
```

**Docker:**
```json
{
  "data_root": "/opt/sca/data/staged/data_products"
}
```

---

## Path Mounting in Docker

**From `docker-compose.yml`:**

```yaml
services:
  api:
    volumes:
      - landing_volume:/opt/sca/data  # Shared volume for all data

  celery_worker:
    volumes:
      - landing_volume:/opt/sca/data  # Same shared volume

  secure_download:
    volumes:
      - landing_volume:/opt/sca/data  # Same shared volume

volumes:
  landing_volume:
    external: false
```

**Key Points:**
- All services share the same `landing_volume` mounted at `/opt/sca/data`
- This allows datasets to be accessed across API, workers, and download services
- The volume persists across container restarts

---

## Production Network Filesystem Structure

In production, datasets live on network-mounted filesystems (`/N/...`):

```
/N/
├── scratch/cmguser/
│   └── CMG-SCA/cmg-bioloop/
│       └── stage/
│           ├── raw_data/
│           │   └── <uuid>/
│           │       └── <dataset_name>/
│           │           ├── <files>
│           │           └── RunInfo.xml (for sequencing runs)
│           └── data_products/
│               └── <uuid>/
│                   └── <product_name>/
│                       └── <files>
└── project/CMG-SCA/
    ├── cmg-bioloop/
    │   ├── stage/raw_data/      # Alternate staging location
    │   └── bundles/raw_data/     # Bundle storage
    └── production/
        └── conversion/           # Conversion outputs (API config)
```

---

## Docker Volume Structure

In docker, all paths are under `/opt/sca/data` (the mounted volume):

```
/opt/sca/data/
├── origin/
│   ├── raw_data/           # Where watch.py looks for new datasets
│   └── data_products/
├── stage/
│   ├── raw_data/
│   │   └── <uuid>/
│   │       └── <dataset_name>/
│   │           ├── <files>
│   │           └── RunInfo.xml (for sequencing runs)
│   └── data_products/
├── bundle/
│   ├── raw_data/           # Tar bundles of staged datasets
│   └── data_products/
├── download/               # Symlinks for secure download
├── conversion/
│   └── output/             # Conversion outputs
└── qc/
    └── raw_data/           # QC reports
```

---

## Important Path Behaviors

### 1. **Staging Path Structure**

When a dataset is staged, it follows this pattern:

```
<stage_base_path>/<uuid>/<dataset_name>/
```

**Production Example:**
```
/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data/
  └── a74a17d3c85d9782f771f3a9596ce8ea/
      └── iseq-DI/
          ├── RunInfo.xml
          ├── Data/
          ├── InterOp/
          └── ... (other runfolder files)
```

**Docker Example:**
```
/opt/sca/data/stage/raw_data/
  └── a74a17d3c85d9782f771f3a9596ce8ea/
      └── iseq-DI/
          ├── RunInfo.xml
          └── ... (same structure)
```

### 2. **Bundle Path Structure**

Bundles (tar archives) are stored separately:

```
<bundle_stage_path>/<dataset_name>.<dataset_type>.tar
```

**Example:**
- Production: `/N/project/CMG-SCA/cmg-bioloop/bundles/raw_data/iseq-DI.RAW_DATA.tar`
- Docker: `/opt/sca/data/bundle/raw_data/iseq-DI.RAW_DATA.tar`

### 3. **Conversion Output Structure**

Conversion outputs follow:

```
<output_directory>/<conversion_id>/<dataset_name>/
```

**Example:**
- Production: `/N/scratch/cmguser/cmg-bioloop/conversions/output/2736/iseq-DI/`
- Docker: `/opt/sca/data/conversion/output/2736/iseq-DI/`

---

## Path References in Code

### Workers (Python)

```python
from workers.config import config

# Access paths
stage_path = config['paths']['RAW_DATA']['stage']
bundle_path = config['paths']['RAW_DATA']['bundle']['stage']
output_path = config['paths']['conversion']['output']
```

### API (Node.js)

```javascript
const config = require('config');

// Access paths (less common in API - workers handle file operations)
const dataRoot = config.data_root;  // Docker only
const conversionOutput = config.conversion.output_directory;  // Production
```

---

## Critical Differences

### Production (`APP_ENV=production`)

- **Paths:** Real network filesystems (`/N/...`)
- **Access:** Direct filesystem access (no containers)
- **Workers:** Run via PM2 on worker hosts
- **Permissions:** Must respect `/N/...` filesystem ACLs
- **Write Restrictions:** NEVER write to `/N/...` in production (read-only for staging/bundles)

### Docker (`APP_ENV=docker`)

- **Paths:** Shared volume mounted at `/opt/sca/data`
- **Access:** Through Docker volume
- **Workers:** Run in containers
- **Permissions:** Container user permissions
- **Write Access:** Full read/write to `/opt/sca/data`

---

## Path Gotchas

### 1. **Don't Mix Environments**

```python
# ❌ WRONG - Hardcoding production paths in docker
stage_path = '/N/scratch/cmguser/...'  # Won't exist in docker

# ✅ CORRECT - Use config
from workers.config import config
stage_path = config['paths']['RAW_DATA']['stage']
```

### 2. **API File Serving**

The API container doesn't directly serve files from `/N/...` paths. File serving happens through:

1. **Secure Download Service** - Has access to network mounts
2. **File Exposure Routes** - Generate signed URLs

### 3. **Conversion Definition Paths**

Conversion definitions in the database may have hardcoded paths:

```sql
SELECT output_directory FROM conversion_definition WHERE name = 'bcl2fastq';
-- Production: /N/scratch/cmguser/cmg-bioloop/conversions/output
-- Docker: /opt/sca/data/conversion/output
```

These must match the environment's actual paths.

---

## Testing Across Environments

When testing code that uses paths:

1. **Use Config Always:** Never hardcode paths
2. **Check `APP_ENV`:** Conditional logic based on environment
3. **Test Both:** Verify code works in both prod and docker
4. **Mock Paths:** Use temp directories for unit tests

---

## References

- **Workers Config:** `workers/workers/config/production.py`, `workers/workers/config/docker.py`
- **API Config:** `api/config/production.json`, `api/config/docker.json`
- **Docker Compose:** `docker-compose.yml` (volume mounts)
- **Production Rules:** `.cursorrules`, `.ai/PRODUCTION_ENVIRONMENT.md`

---

**Last Updated:** 2026-01-19


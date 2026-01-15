# Pipeline Version Verification Report
**Date:** 2026-01-15  
**Environment:** Production Host (cmg-new-service1.sca.iu.edu)

## Executive Summary

✅ **All conversion pipelines match production installation**  
❌ **MultiQC version updated from v1.15 → v1.28**

---

## Detailed Comparison

### Conversion Pipelines

| Pipeline | Bioloop Configuration | Production Installation | Status |
|----------|----------------------|------------------------|--------|
| **bcl2fastq** | v2.20.0.422 | v2.20.0.422 | ✅ MATCH |
| **bcl-convert** | v4.3.6 | v4.3.6 | ✅ MATCH |
| **cellranger** | v8.0.1 | cellranger-8.0.1 | ✅ MATCH |
| **cellranger** | v6.1.2 | cellranger-6.1.2 | ✅ MATCH |
| **cellranger** | v4.0.0 | 4.0.0 | ✅ MATCH |
| **cellranger-arc** | v1.0.0 | cellranger-arc-1.0.0 | ✅ MATCH |
| **cellranger-arc** | v2.0.0 | cellranger-arc-2.0.0 | ✅ MATCH |
| **cellranger-atac** | v1.2.0 | 1.2.0 | ✅ MATCH |
| **spaceranger** | v3.0.1 | spaceranger-3.0.1 | ✅ MATCH |
| **spaceranger** | v1.3.1 | spaceranger-1.3.1 | ✅ MATCH |
| **spaceranger** | v1.1.0 | 1.1.0 | ✅ MATCH |

### QC Tools

| Tool | Bioloop Configuration | Production Installation | Status | Action Taken |
|------|----------------------|------------------------|--------|--------------|
| **FastQC** | v0.11.9 | (not specified) | ℹ️ INFO | No change needed |
| **MultiQC** | v1.15 | v1.28 | ❌ MISMATCH | **Updated to v1.28** |

---

## Configuration Locations

### Pipeline Executable Paths

All pipeline executables are configured in the following files:

1. **API Seed Data** (for database seeding):
   - `api/prisma/seed_data/cmd_line_programs.js`
   - `api/prisma/seed_data/conversion/cmd_line_programs.js`

2. **Data Sync Constants** (for CMG migration):
   - `data_sync/src/sync/constants.js`

3. **Configuration Files**:
   - `api/config/default.json` - Line 287-299
   - `ui/src/config.js` - Line 127-139
   - `workers/workers/config/common.py` - Line 240-245

### Pipeline Executable Path Pattern

All pipelines follow this path structure:
```
/opt/sca/data/conversion/{pipeline-name}/bin/{pipeline-executable}
```

Examples:
- bcl2fastq: `/opt/sca/data/conversion/bcl2fastq/bin/bcl2fastq`
- cellranger-v8.0.1: `/opt/sca/data/conversion/cellranger-v8.0.1/bin/cellranger-v8.0.1`
- spaceranger-v3.0.1: `/opt/sca/data/conversion/spaceranger-v3.0.1/bin/spaceranger-v3.0.1`

### QC Tool Configuration

**MultiQC** is installed via pip in the worker Docker image:
- **File**: `workers/Dockerfile`
- **Line**: 25
- **Updated**: `pip install multiqc==1.28` (was v1.15)

**FastQC** is installed from source:
- **File**: `workers/Dockerfile`
- **Lines**: 18-23
- **Version**: v0.11.9

---

## Changes Made

### 1. MultiQC Version Update

**File**: `workers/Dockerfile`

**Before**:
```dockerfile
pip install multiqc==1.15 && \
```

**After**:
```dockerfile
pip install multiqc==1.28 && \
```

**Reason**: Match production installation (v1.28)

---

## Verification Steps

### To verify the changes:

1. **Rebuild the worker Docker image**:
   ```bash
   docker compose build celery_worker conversion_worker watch
   ```

2. **Restart the worker containers**:
   ```bash
   docker compose up -d celery_worker conversion_worker watch
   ```

3. **Verify MultiQC version inside container**:
   ```bash
   docker compose exec celery_worker multiqc --version
   ```
   Expected output: `multiqc, version 1.28`

4. **Verify pipeline executables are accessible**:
   ```bash
   # Check bcl2fastq
   docker compose exec conversion_worker ls -la /opt/sca/data/conversion/bcl2fastq/bin/
   
   # Check cellranger versions
   docker compose exec celery_worker ls -la /opt/sca/data/conversion/cellranger-v*/bin/
   
   # Check spaceranger versions
   docker compose exec celery_worker ls -la /opt/sca/data/conversion/spaceranger-v*/bin/
   ```

---

## Production Deployment Notes

### On Production Host (cmg-new-service1.sca.iu.edu)

The production environment already has all the correct pipeline versions installed. The bioloop application is configured to use these exact versions.

**No changes needed on production** - the pipeline executables are already in place and match the bioloop configuration.

### Directory Structure on Production

```
/opt/sca/data/conversion/
├── bcl2fastq/
│   └── bin/
│       └── bcl2fastq (v2.20.0.422)
├── bcl-convert/
│   └── bin/
│       └── bcl-convert (v4.3.6)
├── cellranger-v8.0.1/
│   └── bin/
│       └── cellranger-v8.0.1
├── cellranger-v6.1.2/
│   └── bin/
│       └── cellranger-v6.1.2
├── cellranger-v4.0.0/
│   └── bin/
│       └── cellranger-v4.0.0
├── cellranger-arc/
│   └── bin/
│       └── cellranger-arc (v1.0.0)
├── cellranger-arc-v2/
│   └── bin/
│       └── cellranger-arc-v2 (v2.0.0)
├── cellranger-atac/
│   └── bin/
│       └── cellranger-atac (v1.2.0)
├── spaceranger-v3.0.1/
│   └── bin/
│       └── spaceranger-v3.0.1
├── spaceranger-v1.3.1/
│   └── bin/
│       └── spaceranger-v1.3.1
└── spaceranger-v1.1.0/
    └── bin/
        └── spaceranger-v1.1.0
```

---

## Testing Recommendations

### 1. Test MultiQC Report Generation

After updating to v1.28, test QC report generation:

```bash
# Run a test conversion with QC generation
# Verify multiqc_report.html is generated correctly
```

### 2. Test Each Pipeline

Verify each pipeline can be executed:

```bash
# Test bcl2fastq
docker compose exec conversion_worker /opt/sca/data/conversion/bcl2fastq/bin/bcl2fastq --version

# Test cellranger versions
docker compose exec celery_worker /opt/sca/data/conversion/cellranger-v8.0.1/bin/cellranger-v8.0.1 --version
docker compose exec celery_worker /opt/sca/data/conversion/cellranger-v6.1.2/bin/cellranger-v6.1.2 --version
docker compose exec celery_worker /opt/sca/data/conversion/cellranger-v4.0.0/bin/cellranger-v4.0.0 --version

# Test spaceranger versions
docker compose exec celery_worker /opt/sca/data/conversion/spaceranger-v3.0.1/bin/spaceranger-v3.0.1 --version
docker compose exec celery_worker /opt/sca/data/conversion/spaceranger-v1.3.1/bin/spaceranger-v1.3.1 --version
docker compose exec celery_worker /opt/sca/data/conversion/spaceranger-v1.1.0/bin/spaceranger-v1.1.0 --version
```

---

## Summary

✅ **All 11 conversion pipelines match production installation**  
✅ **MultiQC updated to match production (v1.28)**  
✅ **No changes needed to pipeline executable paths**  
✅ **Configuration files already reference correct versions**

The bioloop application is now fully aligned with the production environment's pipeline versions.

---

**Last Updated**: 2026-01-15  
**Status**: ✅ Complete


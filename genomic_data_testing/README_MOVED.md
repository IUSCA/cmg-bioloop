# Genomic Data Testing Scripts - MOVED

**⚠️ NOTICE: This directory has been migrated to the workers environment.**

## New Location

The genomic data testing scripts have been **converted to Python** and **moved to**:

```
/opt/sca/cmg-bioloop/workers/workers/scripts/genomic_data_testing/
```

## What Changed

### 1. **Language:** Bash → Python
All bash scripts (`.sh` files) have been converted to Python (`.py` files) to:
- Work within the poetry environment
- Use the workers API module for dataset existence checks
- Provide better error handling and logging
- Maintain consistency with other worker scripts

### 2. **Location:** Root → Workers
Scripts are now part of the workers module at:
```
workers/workers/scripts/genomic_data_testing/
```

### 3. **Execution:** Direct → Module
Scripts should now be run as Python modules:

**Old way (bash):**
```bash
./genomic_data_testing/register_sequencing_runs/runs/register_iseq-DI.sh
```

**New way (Python):**
```bash
cd /opt/sca/cmg-bioloop/workers
poetry shell
python -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di
```

### 4. **Features Added:**
- **Duplicate detection:** Automatically checks if dataset names exist via API
- **Numeric suffixes:** Appends `---1`, `---2`, etc. if name conflicts occur
- **Configuration-aware:** Uses origin paths from `workers/config/`
- **Better retry logic:** Enhanced download resumption and error handling
- **PM2 support:** Optional ecosystem.config.js for managed execution

## Migration Guide

### For Users

If you were running these scripts manually:

1. Navigate to workers directory:
   ```bash
   cd /opt/sca/cmg-bioloop/workers
   ```

2. Activate poetry shell:
   ```bash
   poetry shell
   ```

3. Run scripts as Python modules:
   ```bash
   # Sequencing runs
   python -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di
   
   # Data products
   python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigwig
   python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigbed
   python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_methylation
   python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_gsm429321_h3k27ac
   
   # Alternative directory
   python -m workers.scripts.genomic_data_testing.register_genome_browser_suitable_data_products.products.register_h3k27ac
   ```

### For Automation

If these scripts were part of automated workflows:

1. Update scripts to use poetry environment
2. Change working directory to `/opt/sca/cmg-bioloop/workers`
3. Update commands to use Python module syntax
4. Optionally use PM2 ecosystem.config.js for process management

## Bash Scripts

The original bash scripts remain in this directory for reference but should be considered **deprecated**. They will not receive updates and may be removed in the future.

## Documentation

Comprehensive documentation is available in the new location:

```
/opt/sca/cmg-bioloop/workers/workers/scripts/genomic_data_testing/README.md
```

## Questions?

See the README in the new location or contact the development team.

---

**Migration Date:** 2026-01-22  
**Old Location:** `/opt/sca/cmg-bioloop/genomic_data_testing/`  
**New Location:** `/opt/sca/cmg-bioloop/workers/workers/scripts/genomic_data_testing/`


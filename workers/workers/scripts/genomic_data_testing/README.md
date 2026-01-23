# Genomic Data Testing

This directory contains Python scripts for downloading and registering test datasets with the Bioloop system for development and testing purposes.

## Overview

These scripts have been converted from bash to Python and are designed to work within the workers' poetry environment. They automatically:

- Download test datasets from public sources
- Check if dataset names already exist (via API)
- Append numeric suffixes (---1, ---2, etc.) if names conflict
- Place datasets in appropriate origin paths from configuration
- Create documentation for conversion testing and usage

## Directory Structure

```
genomic_data_testing/
├── utils.py                                   # Common utility functions
├── ecosystem.config.js                        # PM2 configuration (optional)
├── register_sequencing_runs/                  # RAW_DATA sequencing runs
│   ├── runs/                                  
│   │   └── register_iseq_di.py               # iSeq dual-index demo run
│   └── conversion_testing/                    # Auto-generated conversion docs
│       └── iseq-DI---bcl2fastq.md            # Created by register_iseq_di.py
├── register_data_products_suitable_for_genome_browser/
│   ├── products/
│   │   ├── register_bigwig.py                # H3K4me3 ChIP-seq signal
│   │   ├── register_bigbed.py                # Peaks/features track
│   │   ├── register_methylation.py           # H1 methylation data
│   │   └── register_gsm429321_h3k27ac.py     # H3K27ac ChIP-seq signal
│   └── product_docs/                          # Auto-generated product docs
└── register_genome_browser_suitable_data_products/
    ├── products/
    │   └── register_h3k27ac.py               # H3K27ac ChIP-seq signal
    └── product_docs/                          # Auto-generated product docs
```

## Prerequisites

- Poetry environment must be active (`poetry shell`)
- Workers configuration must be set up (`workers/.env`)
- API must be accessible
- Network access to download sources

## Usage

### Running Scripts Manually

All scripts should be run as Python modules from the repository root:

```bash
# Activate poetry environment
cd /opt/sca/cmg-bioloop/workers
poetry shell

# Run sequencing run registration
python -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di

# Run data product registration
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigwig
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigbed
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_methylation
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_gsm429321_h3k27ac

# Alternative directory
python -m workers.scripts.genomic_data_testing.register_genome_browser_suitable_data_products.products.register_h3k27ac
```

### Running with PM2 (Optional)

If you want to manage these as PM2 processes (uncomment entries in ecosystem.config.js first):

```bash
cd /opt/sca/cmg-bioloop/workers/workers/scripts/genomic_data_testing
pm2 start ecosystem.config.js
```

## Features

### Automatic Duplicate Detection

Scripts check if a dataset name already exists via the `/datasets/:datasetType/:datasetName/exists` API endpoint. If it exists, a numeric suffix is automatically appended:

- First attempt: `iseq-DI`
- If exists: `iseq-DI---1`
- If exists: `iseq-DI---2`
- And so on...

### Environment-Aware Paths

Scripts automatically use the correct origin paths based on configuration:

**Production (APP_ENV=production):**
- RAW_DATA → `/N/scratch/cmguser/cmg-bioloop/origin/raw_data`
- DATA_PRODUCT → `/N/scratch/cmguser/cmg-bioloop/origin/data_products`

**Development (APP_ENV=docker or development):**
- RAW_DATA → `/opt/sca/data/origin/raw_data`
- DATA_PRODUCT → `/opt/sca/data/origin/data_products`

### Chunked Downloads with Resume

All downloads use chunked approach (10-25MB chunks) with:
- Automatic retry on failure (up to 50 retries per chunk)
- Resume capability for interrupted downloads
- Progress logging

### Automatic Extraction

Archives are automatically detected and extracted:
- `.tar.gz`, `.tgz` → tar + gzip
- `.tar.bz2`, `.tbz2` → tar + bzip2
- `.tar.xz`, `.txz` → tar + xz
- `.tar` → tar only
- `.gz` → gzip (non-tar)
- `.zip` → zip

Single-directory extractions are automatically flattened.

## Available Test Datasets

### Sequencing Runs (RAW_DATA)

**iseq-DI** - 10x Genomics iSeq dual-index demo run
- Source: 10x Genomics spatial expression demultiplexing test data
- Size: ~541 MB
- Purpose: bcl2fastq conversion testing
- Auto-creates: `conversion_testing/iseq-DI---bcl2fastq.md`

### Data Products (DATA_PRODUCT)

**bigWig_h3k4me3_hg19** - H3K4me3 ChIP-seq Signal Track
- Source: WashU Epigenome Browser
- Size: Several MB
- Genome: hg19
- Purpose: BigWig signal visualization testing

**bigBed_test** - BigBed Peaks/Features Track
- Source: WashU Epigenome Browser
- Size: ~805 KB
- Genome: hg19
- Purpose: BigBed feature visualization testing

**methylation_h1_hg19** - H1 MethylC-seq Track
- Source: WashU Public Data
- Size: Few MB (gzipped)
- Genome: hg19
- Purpose: Methylation data visualization testing

**bigWig_GSM429321_H3K27ac_hg19** / **bigWig_h3k27ac_hg19** - H3K27ac ChIP-seq Signal
- Source: WashU Epigenome Browser
- Size: Variable MB
- Genome: hg19
- Purpose: Enhancer/promoter visualization testing

## Monitoring Registration

After running a script, monitor the watch.py logs to confirm registration:

```bash
# Check watch.py logs
pm2 logs watch

# Or if watch is running in docker
docker-compose logs -f watch

# Check API for registered datasets
curl https://cmg-test.sca.iu.edu/api/datasets | jq '.datasets[] | select(.name | contains("iseq-DI"))'
```

## Documentation

Each script auto-generates detailed documentation:

- **Sequencing runs:** `register_sequencing_runs/conversion_testing/*.md`
  - Includes bcl2fastq commands, SampleSheets, validation steps
  
- **Data products:** `register_*_suitable_for_genome_browser/product_docs/*.md`
  - Includes testing ranges, browser compatibility, verification steps

## Troubleshooting

### Dataset Already Exists

If you see "Dataset already exists" messages, the script will automatically append a numeric suffix. Check the final output for the actual dataset name used.

### Download Failures

Scripts automatically retry failed chunks up to 50 times. If download still fails:
- Check network connectivity
- Verify source URL is accessible
- Check available disk space in `/tmp`

### Permission Errors

Ensure you have write permissions to the origin directories specified in configuration.

### Import Errors

Make sure you're running from the workers directory with poetry shell active:

```bash
cd /opt/sca/cmg-bioloop/workers
poetry shell
python -m workers.scripts.genomic_data_testing...
```

## Notes

- Scripts use atomic file operations to prevent watch.py from detecting partial downloads
- Downloads occur in `/tmp` first, then moved to final destination
- All downloads include comprehensive retry logic for reliability
- Documentation is auto-generated each time a script runs
- Scripts can be run multiple times safely (duplicate detection prevents conflicts)

## Data Sources

- **10x Genomics:** https://www.10xgenomics.com/support
- **WashU Epigenome Browser:** https://vizhub.wustl.edu/
- **WashU Public Data:** https://vizhub.wustl.edu/public/

All datasets are publicly available for testing and demonstration purposes.


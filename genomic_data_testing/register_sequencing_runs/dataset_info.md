# Sequencing Run Test Dataset Information

This document provides detailed information about the sequencing run test datasets available for testing the Bioloop data processing pipeline.

## Directory Structure

```
register_sequencing_runs/
├── runs/                                    # Individual run registration scripts
│   ├── register_iseq-DI.sh                 # bcl2fastq testing
│   ├── register_bcl-convert-demo.sh        # bcl-convert testing
│   ├── register_cellranger-tiny-bcl.sh     # cellranger mkfastq testing
│   ├── register_cellranger-atac-tiny-bcl.sh # cellranger-atac mkfastq testing
│   ├── register_cellranger-arc-tiny-bcl.sh # cellranger-arc mkfastq testing
│   └── register_spaceranger-tiny-bcl.sh    # spaceranger mkfastq testing
├── conversion_testing/                      # Auto-generated conversion documentation
│   ├── iseq-DI---bcl2fastq.md
│   ├── bcl-convert-demo---bcl-convert.md
│   ├── cellranger-tiny-bcl---cellranger.md
│   ├── cellranger-atac-tiny-bcl---cellranger-atac.md
│   ├── cellranger-arc-tiny-bcl---cellranger-arc.md
│   └── spaceranger-tiny-bcl---spaceranger.md
└── dataset_info.md                          # This file
```

---

## Available Datasets

### 1. iSeq-DI - 10x Genomics Spatial Expression Demultiplexing Data

**Target Pipeline:** bcl2fastq  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~541 MB  
**Source:** https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz

#### Description
This dataset contains sequencing run output from a 10x Genomics spatial expression experiment. It's designed for testing demultiplexing and data processing workflows.

- **Platform:** iSeq (Illumina)
- **Indexing:** Dual-index
- **Application:** bcl2fastq conversion testing

---

### 2. BCL Convert Demo - Illumina BCL Convert Testing

**Target Pipeline:** bcl-convert  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~541 MB  
**Source:** https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz (same as iSeq-DI)

#### Description
Uses the same iSeq-DI dataset but for testing Illumina's BCL Convert tool, which is the successor to bcl2fastq.

- **Platform:** iSeq (Illumina)
- **Indexing:** Dual-index
- **Application:** bcl-convert conversion testing

---

### 3. Cell Ranger Tiny-BCL - Single-Cell Gene Expression

**Target Pipelines:** cellranger, cellranger-v8.0.1, cellranger-v6.1.2, cellranger-v4.0.0  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~37 MB  
**Source:** https://cf.10xgenomics.com/supp/cell-exp/cellranger-tiny-bcl-1.2.0.tar.gz

#### Description
A minimal BCL run folder designed for testing Cell Ranger's mkfastq pipeline. This is the official demo dataset from 10x Genomics.

- **Platform:** Illumina
- **Indexing:** Dual-index (SI-TT-D9)
- **Application:** cellranger mkfastq testing
- **Limitation:** ONLY for mkfastq, NOT for cellranger count

#### Sample Sheet
- Simple CSV: https://cf.10xgenomics.com/supp/cell-exp/cellranger-tiny-bcl-simple-1.2.0.csv
- IEM Format: https://cf.10xgenomics.com/supp/cell-exp/cellranger-tiny-bcl-samplesheet-1.2.0.csv

---

### 4. Cell Ranger ATAC Tiny-BCL - Single-Cell Chromatin Accessibility

**Target Pipeline:** cellranger-atac  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~50 MB (estimated)  
**Source:** https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-1.0.0.tar.gz

#### Description
A minimal BCL run folder designed for testing Cell Ranger ATAC's mkfastq pipeline. Single-cell ATAC-seq data for chromatin accessibility analysis.

- **Platform:** Illumina
- **Indexing:** Single-index (SI-NA-C1)
- **Application:** cellranger-atac mkfastq testing
- **Limitation:** ONLY for mkfastq, NOT for cellranger-atac count
- **Special Note:** ATAC-seq FASTQs have different structure (R1, R2, R3, I1) - cell barcode is in R2

#### Sample Sheet
- Simple CSV: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-simple-1.0.0.csv
- IEM Format: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-samplesheet-1.0.0.csv

---

### 5. Cell Ranger ARC Tiny-BCL - Multiome ATAC + Gene Expression

**Target Pipelines:** cellranger-arc, cellranger-arc-v2  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~60 MB (estimated)  
**Source:** https://cf.10xgenomics.com/supp/cell-arc/cellranger-arc-tiny-bcl-1.0.0.tar.gz

#### Description
A minimal BCL run folder designed for testing Cell Ranger ARC's mkfastq pipeline. Multiome data combines ATAC and Gene Expression from the same cells.

- **Platform:** Illumina
- **Indexing:** Mixed (GEX: dual-index SI-TT-*, ATAC: single-index SI-NA-*)
- **Application:** cellranger-arc mkfastq testing
- **Limitation:** ONLY for mkfastq, NOT for cellranger-arc count
- **Deprecation Notice:** cellranger-arc mkfastq is deprecated; use BCL Convert instead

---

### 6. Space Ranger Tiny-BCL - Visium Spatial Transcriptomics

**Target Pipelines:** spaceranger, spaceranger-v3.0.1, spaceranger-v1.3.1, spaceranger-v1.1.0  
**File Type:** Compressed archive (tar.gz)  
**Size:** ~40 MB (estimated)  
**Source:** https://cf.10xgenomics.com/supp/spatial-exp/spaceranger-tiny-bcl-1.0.0.tar.gz

#### Description
A minimal BCL run folder designed for testing Space Ranger's mkfastq pipeline. Visium spatial transcriptomics data.

- **Platform:** Illumina
- **Indexing:** Dual-index
- **Application:** spaceranger mkfastq testing
- **Limitation:** ONLY for mkfastq, NOT for spaceranger count

#### Sample Sheet
- Simple CSV: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-simple-1.0.0.csv
- IEM Format: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-samplesheet-1.0.0.csv

---

## Pipeline to Dataset Mapping

| CMD_LINE_PROGRAM | Dataset to Use | Script |
|-----------------|----------------|--------|
| bcl2fastq | iSeq-DI | `register_iseq-DI.sh` |
| bcl-convert | BCL Convert Demo | `register_bcl-convert-demo.sh` |
| cellranger | Cell Ranger Tiny-BCL | `register_cellranger-tiny-bcl.sh` |
| cellranger-v8.0.1 | Cell Ranger Tiny-BCL | `register_cellranger-tiny-bcl.sh` |
| cellranger-v6.1.2 | Cell Ranger Tiny-BCL | `register_cellranger-tiny-bcl.sh` |
| cellranger-v4.0.0 | Cell Ranger Tiny-BCL | `register_cellranger-tiny-bcl.sh` |
| cellranger-atac | Cell Ranger ATAC Tiny-BCL | `register_cellranger-atac-tiny-bcl.sh` |
| cellranger-arc | Cell Ranger ARC Tiny-BCL | `register_cellranger-arc-tiny-bcl.sh` |
| cellranger-arc-v2 | Cell Ranger ARC Tiny-BCL | `register_cellranger-arc-tiny-bcl.sh` |
| spaceranger | Space Ranger Tiny-BCL | `register_spaceranger-tiny-bcl.sh` |
| spaceranger-v3.0.1 | Space Ranger Tiny-BCL | `register_spaceranger-tiny-bcl.sh` |
| spaceranger-v1.3.1 | Space Ranger Tiny-BCL | `register_spaceranger-tiny-bcl.sh` |
| spaceranger-v1.1.0 | Space Ranger Tiny-BCL | `register_spaceranger-tiny-bcl.sh` |

---

## Using the Registration Scripts

### Quick Start

```bash
cd runs

# Download iSeq-DI for bcl2fastq testing
./register_iseq-DI.sh

# Download BCL Convert demo
./register_bcl-convert-demo.sh

# Download Cell Ranger tiny-bcl
./register_cellranger-tiny-bcl.sh

# Download Cell Ranger ATAC tiny-bcl
./register_cellranger-atac-tiny-bcl.sh

# Download Cell Ranger ARC tiny-bcl
./register_cellranger-arc-tiny-bcl.sh

# Download Space Ranger tiny-bcl
./register_spaceranger-tiny-bcl.sh
```

### Custom Destination

```bash
./register_cellranger-tiny-bcl.sh -d /custom/path/to/raw_data
```

---

## Download Configuration

All scripts are configured with robust downloading:

- **Chunk Size:** 25 MB per chunk (for large files)
- **Retries:** Up to 10-50 attempts with delays
- **Resume Support:** Automatically resumes from last successful chunk
- **User Agent:** Required for 10x Genomics server compatibility
- **Atomic Operations:** Prevents partial file detection by watch.py

---

## What Happens During Registration

1. **Download:** File is downloaded to `/tmp` with retry logic
2. **Extraction:** Archive is extracted
3. **Directory Creation:** A directory is created in the destination
4. **Atomic Move:** Contents are moved into the directory
5. **Watch Detection:** The watch.py script detects the new directory as RAW_DATA
6. **Workflow Trigger:** The integrated workflow is automatically triggered

---

## Verification After Registration

After running a script, verify the dataset was registered:

```bash
# Check the directory was created (production)
ls -la /N/scratch/cmguser/cmg-bioloop/origin/raw_data/<dataset-name>

# Check the directory was created (development)
docker-compose exec celery_worker ls -la /opt/sca/data/origin/raw_data/<dataset-name>

# Check watch.py logs for registration events
docker-compose logs watch | grep <dataset-name>

# Check API for dataset entry
curl http://localhost:3000/api/datasets | jq '.datasets[] | select(.name=="<dataset-name>")'
```

---

## Important Notes

1. **Demo Datasets Only:** All tiny-bcl datasets are for mkfastq pipeline testing ONLY. They cannot be used for downstream analysis (count, etc.)

2. **Sample Sheets:** Each pipeline requires specific sample sheet formats. See the conversion documentation for details.

3. **Environment Adaptation:** Scripts automatically detect APP_ENV and use appropriate paths:
   - Production: `/N/scratch/cmguser/cmg-bioloop/origin/raw_data`
   - Development: `/opt/sca/data/origin/raw_data`

4. **Deprecation:** The `cellranger-arc mkfastq` pipeline is deprecated. Consider using BCL Convert instead.

---

## Data Sources

All datasets are publicly available test data provided by:

- **10x Genomics:** https://www.10xgenomics.com/support/software/
- **Illumina:** https://support.illumina.com/sequencing/sequencing_software/bcl-convert.html


# Sequencing Run Registration Scripts

This directory contains individual registration scripts for specific sequencing runs used in pipeline testing.

## Available Runs

### bcl2fastq Testing

#### iSeq-DI (Dual-Index Demo Run)

**Script:** `register_iseq-DI.sh`  
**Source:** 10x Genomics  
**Size:** ~541 MB  
**Purpose:** bcl2fastq conversion testing

```bash
./register_iseq-DI.sh
```

This script:
- Downloads the iSeq-DI dual-index demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/iseq-DI---bcl2fastq.md`

---

### bcl-convert Testing

#### BCL Convert Demo Run

**Script:** `register_bcl-convert-demo.sh`  
**Source:** 10x Genomics (same iSeq-DI data)  
**Size:** ~541 MB  
**Purpose:** bcl-convert conversion testing (Illumina's successor to bcl2fastq)

```bash
./register_bcl-convert-demo.sh
```

This script:
- Downloads the iSeq-DI dual-index demo run
- Registers it as RAW_DATA for bcl-convert testing
- Creates conversion documentation at `../conversion_testing/bcl-convert-demo---bcl-convert.md`

---

### Cell Ranger Testing (Single-Cell Gene Expression)

#### Cell Ranger Tiny-BCL

**Script:** `register_cellranger-tiny-bcl.sh`  
**Source:** 10x Genomics  
**Size:** ~37 MB  
**Purpose:** cellranger mkfastq conversion testing

```bash
./register_cellranger-tiny-bcl.sh
```

**Target Pipelines:**
- `cellranger`
- `cellranger-v8.0.1`
- `cellranger-v6.1.2`
- `cellranger-v4.0.0`

This script:
- Downloads the Cell Ranger tiny-bcl demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/cellranger-tiny-bcl---cellranger.md`

---

### Cell Ranger ATAC Testing (Single-Cell Chromatin Accessibility)

#### Cell Ranger ATAC Tiny-BCL

**Script:** `register_cellranger-atac-tiny-bcl.sh`  
**Source:** 10x Genomics  
**Size:** ~50 MB (estimated)  
**Purpose:** cellranger-atac mkfastq conversion testing

```bash
./register_cellranger-atac-tiny-bcl.sh
```

**Target Pipelines:**
- `cellranger-atac`

This script:
- Downloads the Cell Ranger ATAC tiny-bcl demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/cellranger-atac-tiny-bcl---cellranger-atac.md`

---

### Cell Ranger ARC Testing (Multiome: ATAC + Gene Expression)

#### Cell Ranger ARC Tiny-BCL

**Script:** `register_cellranger-arc-tiny-bcl.sh`  
**Source:** 10x Genomics  
**Size:** ~60 MB (estimated)  
**Purpose:** cellranger-arc mkfastq conversion testing

```bash
./register_cellranger-arc-tiny-bcl.sh
```

**Target Pipelines:**
- `cellranger-arc`
- `cellranger-arc-v2`

This script:
- Downloads the Cell Ranger ARC tiny-bcl demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/cellranger-arc-tiny-bcl---cellranger-arc.md`

**Note:** The `cellranger-arc mkfastq` pipeline is deprecated. Consider using bcl-convert instead.

---

### Space Ranger Testing (Visium Spatial Transcriptomics)

#### Space Ranger Tiny-BCL

**Script:** `register_spaceranger-tiny-bcl.sh`  
**Source:** 10x Genomics  
**Size:** ~40 MB (estimated)  
**Purpose:** spaceranger mkfastq conversion testing

```bash
./register_spaceranger-tiny-bcl.sh
```

**Target Pipelines:**
- `spaceranger`
- `spaceranger-v3.0.1`
- `spaceranger-v1.3.1`
- `spaceranger-v1.1.0`

This script:
- Downloads the Space Ranger tiny-bcl demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/spaceranger-tiny-bcl---spaceranger.md`

---

## Pipeline Coverage Summary

| Pipeline | Script | Dataset |
|----------|--------|---------|
| bcl2fastq | `register_iseq-DI.sh` | iSeq-DI |
| bcl-convert | `register_bcl-convert-demo.sh` | iSeq-DI |
| cellranger (all versions) | `register_cellranger-tiny-bcl.sh` | cellranger-tiny-bcl |
| cellranger-atac | `register_cellranger-atac-tiny-bcl.sh` | cellranger-atac-tiny-bcl |
| cellranger-arc (all versions) | `register_cellranger-arc-tiny-bcl.sh` | cellranger-arc-tiny-bcl |
| spaceranger (all versions) | `register_spaceranger-tiny-bcl.sh` | spaceranger-tiny-bcl |

---

## How These Scripts Work

Each run-specific script:

1. **Downloads** the sequencing run using chunked downloading (prevents connection resets)
2. **Registers** it with the Bioloop system by placing it in `/opt/sca/data/origin/raw_data`
3. **Documents** the recommended conversion parameters in `../conversion_testing/`

## Auto-Generated Documentation

When you run a registration script, it creates a markdown file in the `conversion_testing/` directory with:

- Dataset overview and characteristics
- Recommended conversion tool command
- Flag justifications
- Required configuration files (e.g., SampleSheet.csv)
- Expected outputs
- Validation checklist
- Troubleshooting tips
- Bioloop conversion submission instructions

## Adding New Runs

To add a new sequencing run:

1. Create a new script: `register_[run-name].sh`
2. Follow the pattern from existing scripts
3. Update the `RUN_NAME` and `PIPELINE` variables
4. Create the conversion documentation in the `create_conversion_doc()` function
5. Make the script executable: `chmod +x register_[run-name].sh`

## Usage

All scripts support these options:

```bash
# Download to default location
./register_[run-name].sh

# Download to custom location
./register_[run-name].sh -d /custom/path

# View help
./register_[run-name].sh -h
```

## Notes

- Scripts execute commands inside the celery_worker container via docker-compose (dev) or directly on host (production)
- Downloads use retry logic for reliability
- Files are moved atomically to prevent watch.py from detecting partial downloads
- Conversion documentation is generated before downloading starts
- All tiny-bcl datasets are for mkfastq testing ONLY (cannot be used for downstream count pipelines)

## Important: Testing Limitations

The tiny-bcl demo datasets are **ONLY** suitable for testing the `mkfastq` step of each pipeline. They **CANNOT** be used for downstream analysis steps like:

- `cellranger count`
- `cellranger-atac count`
- `cellranger-arc count`
- `spaceranger count`

This is by design - the demo datasets are minimal and lack the data needed for full analysis pipelines.


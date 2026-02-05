#!/bin/bash
# =============================================================================
# Cell Ranger ARC Tiny-BCL Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads the Cell Ranger ARC tiny-bcl demo run from 10x Genomics and
#   registers it as RAW_DATA for cellranger-arc mkfastq conversion testing.
#
# Usage:
#   ./register_cellranger-arc-tiny-bcl.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to auto-detected location based on APP_ENV
#   ./register_cellranger-arc-tiny-bcl.sh
#
#   # Download to custom location (overrides auto-detection)
#   ./register_cellranger-arc-tiny-bcl.sh -d /custom/path/to/raw_data
#
# Dataset Information:
#   - Dataset: cellranger-arc-tiny-bcl-1.0.0.tar.gz
#   - Source: 10x Genomics Cell Ranger ARC (Multiome) demo data
#   - Size: ~60 MB (estimated)
#   - Type: Illumina BCL run folder for Multiome ATAC + Gene Expression
#   - Purpose: cellranger-arc mkfastq conversion testing
#
# Target Pipelines:
#   - cellranger-arc (Multiome versions: v1.0.0, v2.0.0)
#   - cellranger-arc mkfastq subcommand
#
# Note:
#   - This script adapts to APP_ENV in workers/.env:
#     * Production (APP_ENV=production): Runs directly on host, downloads to /N/scratch/...
#     * Development: Runs in celery_worker container, downloads to /opt/sca/data/...
#   - The tiny-bcl dataset is ONLY for testing mkfastq, NOT for downstream count pipelines
#   - Creates conversion testing documentation automatically
#   - cellranger-arc mkfastq is deprecated; Illumina BCL Convert is recommended
#
# =============================================================================

set -e

# Get the script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONVERSION_DOC_DIR="$(dirname "$SCRIPT_DIR")/conversion_testing"

# Determine destination based on APP_ENV in workers/.env
WORKERS_ENV="$REPO_ROOT/workers/.env"
if [ -f "$WORKERS_ENV" ]; then
    APP_ENV=$(grep '^APP_ENV=' "$WORKERS_ENV" | cut -d '=' -f2 | tr -d '"' | tr -d "'")
fi

# Set destination based on environment
if [ "$APP_ENV" = "production" ]; then
    # Production: use path from workers/config/production.py
    DESTINATION="/N/scratch/cmguser/cmg-bioloop/origin/raw_data"
else
    # Non-production: use default path
    DESTINATION="/opt/sca/data/origin/raw_data"
fi

SERVICE_NAME="celery_worker"

# Dataset definition
FILENAME="cellranger-arc-tiny-bcl-1.0.0.tar.gz"
URL="https://cf.10xgenomics.com/supp/cell-arc/cellranger-arc-tiny-bcl-1.0.0.tar.gz"
DIR_NAME="cellranger-arc-tiny-bcl"
RUN_NAME="cellranger-arc-tiny-bcl"
PIPELINE="cellranger-arc"

# Parse command line arguments
show_help() {
    head -n 42 "$0" | tail -n +2 | sed 's/^# \?//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--destination)
            DESTINATION="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Error: Unknown option: $1"
            echo ""
            show_help
            ;;
    esac
done

# Function to create conversion testing documentation
create_conversion_doc() {
    local doc_file="$CONVERSION_DOC_DIR/${RUN_NAME}---${PIPELINE}.md"
    
    cat > "$doc_file" << 'EOF'
# Cell Ranger ARC Tiny-BCL Run - cellranger-arc mkfastq Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/cell-arc/cellranger-arc-tiny-bcl-1.0.0.tar.gz  
**File:** `cellranger-arc-tiny-bcl-1.0.0.tar.gz`  
**Size:** ~60 MB (estimated)

### What This Run Is

- A real Illumina BCL run folder designed for Cell Ranger ARC (Multiome) testing
- Single Cell Multiome ATAC + Gene Expression data
- Produced by 10x Genomics for demonstration purposes
- NOT a biologically meaningful experiment
- NOT intended for downstream count/analysis pipelines

### Purpose for Pipeline Testing

This run is ideal for validating the Cell Ranger ARC mkfastq pipeline:

```
BCL -> cellranger-arc mkfastq -> FASTQ files (ATAC + GEX)
```

**Important:** This dataset is ONLY for testing mkfastq. It cannot be used with `cellranger-arc count`.

**Deprecation Notice:** The `cellranger-arc mkfastq` pipeline is deprecated and will be removed in future releases. 10x Genomics recommends using Illumina's BCL Convert instead.

---

## Target Pipelines

This dataset can be used with these cellranger-arc versions:

| Pipeline Name | Executable Path |
|--------------|-----------------|
| cellranger-arc | /N/project/CMG-SCA/bin/cellranger-arc-1.0.0/cellranger-arc |
| cellranger-arc-v2 | /N/project/CMG-SCA/bin/cellranger-arc-2.0.0/cellranger-arc |

---

## Multiome Data Structure

Multiome experiments generate two types of libraries from the same cells:
1. **GEX (Gene Expression)** - Single-cell RNA-seq
2. **ATAC (Chromatin Accessibility)** - Single-cell ATAC-seq

Each library type requires its own sample sheet entry and produces separate FASTQ files.

---

## Recommended cellranger-arc mkfastq Configuration

### Simple CSV Sample Sheet Format

For Multiome data, the sample sheet lists both GEX and ATAC samples:

```csv
Lane,Sample,Index
1,Sample1_GEX,SI-TT-A1
1,Sample1_ATAC,SI-NA-A1
```

Note: GEX uses dual-index (SI-TT-*), ATAC uses single-index (SI-NA-*)

---

## Correct cellranger-arc mkfastq Command

### Basic Command

```bash
cellranger-arc mkfastq \
  --id=multiome-tiny-bcl-output \
  --run=/path/to/cellranger-arc-tiny-bcl \
  --csv=samplesheet.csv
```

### For GEX Libraries Only

```bash
cellranger-arc mkfastq \
  --id=multiome-gex-output \
  --run=/path/to/cellranger-arc-tiny-bcl \
  --csv=gex-samplesheet.csv \
  --filter-dual-index
```

### For ATAC Libraries Only

```bash
cellranger-arc mkfastq \
  --id=multiome-atac-output \
  --run=/path/to/cellranger-arc-tiny-bcl \
  --csv=atac-samplesheet.csv \
  --filter-single-index
```

---

## Bioloop Conversion Submission

When submitting a Conversion in Bioloop against this dataset:

### Pipeline Selection
- Select: `cellranger-arc` or `cellranger-arc-v2`

### Required Arguments
The conversion should use the `mkfastq` subcommand:

```
mkfastq --run <input_dir> --csv <samplesheet>
```

### Sample Sheet
You will need to provide a sample sheet with both GEX and ATAC entries.

---

## Expected Outputs

### Directory Structure After Successful Conversion

```
multiome-tiny-bcl-output/
├── outs/
│   └── fastq_path/
│       ├── Reports/
│       ├── Stats/
│       ├── Sample1_GEX/
│       │   ├── Sample1_GEX_S1_L001_I1_001.fastq.gz
│       │   ├── Sample1_GEX_S1_L001_I2_001.fastq.gz
│       │   ├── Sample1_GEX_S1_L001_R1_001.fastq.gz
│       │   └── Sample1_GEX_S1_L001_R2_001.fastq.gz
│       ├── Sample1_ATAC/
│       │   ├── Sample1_ATAC_S2_L001_I1_001.fastq.gz
│       │   ├── Sample1_ATAC_S2_L001_R1_001.fastq.gz
│       │   ├── Sample1_ATAC_S2_L001_R2_001.fastq.gz
│       │   └── Sample1_ATAC_S2_L001_R3_001.fastq.gz
│       └── Undetermined_*.fastq.gz
└── _* (other Cell Ranger ARC output files)
```

### Output Characteristics

- **GEX FASTQs:** R1, R2, I1, I2 (dual-indexed)
- **ATAC FASTQs:** R1, R2, R3, I1 (single-indexed, barcode in R2)
- NOT suitable for downstream `cellranger-arc count`
- For mkfastq pipeline validation only

---

## Validation Checklist

After running cellranger-arc mkfastq, verify:

- [ ] Exit code is 0
- [ ] `outs/fastq_path/` directory exists
- [ ] Both GEX and ATAC sample directories exist (if both in sample sheet)
- [ ] GEX sample has 4 files: R1, R2, I1, I2
- [ ] ATAC sample has 4 files: R1, R2, R3, I1
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty)

---

## Troubleshooting

### Common Issues

**Issue:** "Mixed single-index and dual-index samples"  
**Solution:** Use `--filter-dual-index` or `--filter-single-index` to process one type at a time

**Issue:** "Unrecognized sample index"  
**Solution:** Use correct index format - SI-TT-* for GEX, SI-NA-* for ATAC

**Issue:** Only partial samples generated  
**Solution:** Check sample sheet format and index names

---

## Additional Notes

- Multiome combines GEX and ATAC from the same cells
- GEX libraries use dual-indexing (SI-TT-* format)
- ATAC libraries use single-indexing (SI-NA-* format)
- Demo runs are minimal and fast to process
- The goal is pipeline validation, not scientific discovery
- Consider using BCL Convert instead (mkfastq is deprecated)

---

## Alternative: Using BCL Convert

Since `cellranger-arc mkfastq` is deprecated, you can also use Illumina's BCL Convert:

```bash
bcl-convert \
  --bcl-input-directory /path/to/cellranger-arc-tiny-bcl \
  --output-directory bcl_convert_output \
  --sample-sheet samplesheet.csv
```

This produces compatible FASTQs for downstream Cell Ranger ARC analysis.

EOF

    echo "Created conversion testing documentation: $doc_file"
}

# Function to run commands (in container or directly on host)
run_command() {
    if [ "$APP_ENV" = "production" ]; then
        # Production: run directly on host
        bash -c "$1"
    else
        # Non-production: run inside container
        docker-compose exec -T "$SERVICE_NAME" bash -c "$1"
    fi
}

# Check if service is running (only for non-production)
if [ "$APP_ENV" != "production" ]; then
    if ! docker-compose ps "$SERVICE_NAME" 2>/dev/null | grep -q "Up"; then
        echo "Error: Service '$SERVICE_NAME' is not running"
        echo "Please start the service with: docker-compose up -d"
        exit 1
    fi
fi

echo "================================"
echo "Cell Ranger ARC Tiny-BCL Run Registration"
echo "================================"
echo "Environment: ${APP_ENV:-development}"
if [ "$APP_ENV" = "production" ]; then
    echo "Execution: Direct (host)"
else
    echo "Execution: Container ($SERVICE_NAME)"
fi
echo "Destination: $DESTINATION"
echo "Dataset: $FILENAME"
echo ""

# Create conversion documentation
mkdir -p "$CONVERSION_DOC_DIR"
create_conversion_doc

echo "[1/1] Processing: $FILENAME"
echo "  URL: $URL"
echo "  Directory: $DIR_NAME"

# Run download and organization (in container or on host)
run_command "
    set -e
    
    # Create temporary directory for download
    TMP_DIR=\$(mktemp -d)
    cd \$TMP_DIR
    
    echo '  Downloading...'
    
    if curl -L --fail --retry 10 --retry-delay 2 \
        -H 'User-Agent: Mozilla/5.0' \
        -o '$FILENAME' \
        '$URL'; then
        echo '  Download complete'
    else
        echo '  Download failed (URL may not exist - check 10x Genomics website)'
        echo '  Alternative: use cellranger-arc testrun --id=tiny for a built-in test'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    # Verify file was downloaded
    if [ ! -f '$FILENAME' ]; then
        echo '  File not found after download'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    FILE_SIZE=\$(stat -f%z '$FILENAME' 2>/dev/null || stat -c%s '$FILENAME' 2>/dev/null || echo 0)
    echo \"  File size: \$FILE_SIZE bytes\"
    
    # Extract the tar.gz file
    echo '  Extracting archive...'
    tar -xzf '$FILENAME'
    rm '$FILENAME'
    echo '  Extraction complete'
    
    # Find the extracted directory name
    EXTRACTED_DIR=\$(ls -d */ 2>/dev/null | head -n1 | tr -d '/')
    if [ -z \"\$EXTRACTED_DIR\" ]; then
        echo '  No directory found after extraction'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    # Find next available directory name (with numeric suffix if needed)
    BASE_DIR_NAME='$DIR_NAME'
    FINAL_DIR_NAME=\"\$BASE_DIR_NAME\"
    COUNTER=2
    
    while [ -d '$DESTINATION'/\"\$FINAL_DIR_NAME\" ]; do
        echo \"  Directory '$DESTINATION/\$FINAL_DIR_NAME' already exists\"
        FINAL_DIR_NAME=\"\$BASE_DIR_NAME-\$COUNTER\"
        COUNTER=\$((COUNTER + 1))
    done
    
    if [ \"\$FINAL_DIR_NAME\" != \"\$BASE_DIR_NAME\" ]; then
        echo \"  Using directory name: \$FINAL_DIR_NAME\"
    fi
    
    # Create the dataset directory in destination
    DATASET_DIR='$DESTINATION'/\"\$FINAL_DIR_NAME\"
    echo \"  Creating directory: \$DATASET_DIR\"
    mkdir -p \$DATASET_DIR
    
    # Move extracted contents to dataset directory
    echo \"  Moving contents to: \$DATASET_DIR\"
    mv \"\$EXTRACTED_DIR\"/* \$DATASET_DIR/ 2>/dev/null || true
    mv \"\$EXTRACTED_DIR\"/.* \$DATASET_DIR/ 2>/dev/null || true
    rmdir \"\$EXTRACTED_DIR\" 2>/dev/null || true
    
    # Cleanup temp directory
    cd /
    rm -rf \$TMP_DIR
    
    echo '  Complete'
"

if [ $? -eq 0 ]; then
    echo "  Successfully created dataset: $DIR_NAME"
else
    echo "  Failed to create dataset: $DIR_NAME"
fi

echo ""
echo "================================"
echo "Registration Complete!"
echo "================================"
echo ""
echo "Downloaded dataset is in directory at:"
echo "  $DESTINATION/$DIR_NAME"
echo ""
echo "Conversion testing documentation created at:"
echo "  $CONVERSION_DOC_DIR/${RUN_NAME}---${PIPELINE}.md"
echo ""
echo "The watch.py script should detect this new directory and register it as RAW_DATA."
echo ""
if [ "$APP_ENV" = "production" ]; then
    echo "To verify dataset was created:"
    echo "  ls -la $DESTINATION/$DIR_NAME"
    echo ""
    echo "To check dataset files:"
    echo "  find $DESTINATION/$DIR_NAME -type f | head -20"
else
    echo "To verify dataset was created:"
    echo "  docker-compose exec $SERVICE_NAME ls -la $DESTINATION/$DIR_NAME"
    echo ""
    echo "To check dataset files:"
    echo "  docker-compose exec $SERVICE_NAME find $DESTINATION/$DIR_NAME -type f | head -20"
fi


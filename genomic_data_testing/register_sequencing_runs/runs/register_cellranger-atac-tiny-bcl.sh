#!/bin/bash
# =============================================================================
# Cell Ranger ATAC Tiny-BCL Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads the Cell Ranger ATAC tiny-bcl demo run from 10x Genomics and
#   registers it as RAW_DATA for cellranger-atac mkfastq conversion testing.
#
# Usage:
#   ./register_cellranger-atac-tiny-bcl.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to auto-detected location based on APP_ENV
#   ./register_cellranger-atac-tiny-bcl.sh
#
#   # Download to custom location (overrides auto-detection)
#   ./register_cellranger-atac-tiny-bcl.sh -d /custom/path/to/raw_data
#
# Dataset Information:
#   - Dataset: cellranger-atac-tiny-bcl-1.0.0.tar.gz
#   - Source: 10x Genomics Cell Ranger ATAC demo data
#   - Size: ~50 MB (estimated)
#   - Type: Illumina BCL run folder for single-cell ATAC-seq
#   - Purpose: cellranger-atac mkfastq conversion testing
#
# Target Pipelines:
#   - cellranger-atac (single-cell ATAC-seq)
#   - cellranger-atac mkfastq subcommand
#
# Note:
#   - This script adapts to APP_ENV in workers/.env:
#     * Production (APP_ENV=production): Runs directly on host, downloads to /N/scratch/...
#     * Development: Runs in celery_worker container, downloads to /opt/sca/data/...
#   - The tiny-bcl dataset is ONLY for testing mkfastq, NOT for downstream count pipelines
#   - Creates conversion testing documentation automatically
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
FILENAME="cellranger-atac-tiny-bcl-1.0.0.tar.gz"
URL="https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-1.0.0.tar.gz"
DIR_NAME="cellranger-atac-tiny-bcl"
RUN_NAME="cellranger-atac-tiny-bcl"
PIPELINE="cellranger-atac"

# Parse command line arguments
show_help() {
    head -n 40 "$0" | tail -n +2 | sed 's/^# \?//'
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
# Cell Ranger ATAC Tiny-BCL Run - cellranger-atac mkfastq Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-1.0.0.tar.gz  
**File:** `cellranger-atac-tiny-bcl-1.0.0.tar.gz`  
**Size:** ~50 MB (estimated)

### What This Run Is

- A real Illumina BCL run folder designed for Cell Ranger ATAC testing
- Single-indexed sample (SI-NA-C1 index)
- Produced by 10x Genomics for demonstration purposes
- Single-cell ATAC-seq (chromatin accessibility) data
- NOT a biologically meaningful experiment
- NOT intended for downstream count/analysis pipelines

### Purpose for Pipeline Testing

This run is ideal for validating the Cell Ranger ATAC mkfastq pipeline:

```
BCL -> cellranger-atac mkfastq -> FASTQ files (R1, R2, R3, I1)
```

**Important:** This dataset is ONLY for testing mkfastq. It cannot be used with `cellranger-atac count`.

---

## Target Pipelines

This dataset should be used with:

| Pipeline Name | Executable Path |
|--------------|-----------------|
| cellranger-atac | /N/project/CMG-SCA/bin/cellranger-atac-1.2.0/cellranger-atac |

---

## Recommended cellranger-atac mkfastq Configuration

### Simple CSV Sample Sheet

Download from: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-simple-1.0.0.csv

Content:
```csv
Lane,Sample,Index
1,test_sample,SI-NA-C1
```

### IEM Sample Sheet

Download from: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-samplesheet-1.0.0.csv

---

## Correct cellranger-atac mkfastq Command

### Using Simple CSV

```bash
cellranger-atac mkfastq \
  --id=atac-tiny-bcl-output \
  --run=/path/to/cellranger-atac-tiny-bcl \
  --csv=cellranger-atac-tiny-bcl-simple-1.0.0.csv
```

### Using IEM Sample Sheet

```bash
cellranger-atac mkfastq \
  --id=atac-tiny-bcl-output \
  --run=/path/to/cellranger-atac-tiny-bcl \
  --samplesheet=cellranger-atac-tiny-bcl-samplesheet-1.0.0.csv
```

---

## Bioloop Conversion Submission

When submitting a Conversion in Bioloop against this dataset:

### Pipeline Selection
- Select: `cellranger-atac`

### Required Arguments
The conversion should use the `mkfastq` subcommand:

```
mkfastq --run <input_dir> --csv <samplesheet>
```

### Sample Sheet
You will need to provide the simple CSV sample sheet content:

```csv
Lane,Sample,Index
1,test_sample,SI-NA-C1
```

### Important Note on ATAC-seq FASTQs

Cell Ranger ATAC produces 4 FASTQ files per sample:
- **R1**: Read 1 (genomic)
- **R2**: Cell barcode (i5 index read)
- **R3**: Read 2 (genomic)
- **I1**: Sample index (i7)

This is different from standard RNA-seq FASTQs!

---

## Expected Outputs

### Directory Structure After Successful Conversion

```
atac-tiny-bcl-output/
├── outs/
│   └── fastq_path/
│       ├── Reports/
│       ├── Stats/
│       ├── tiny-bcl/
│       │   └── test_sample/
│       │       ├── test_sample_S1_L001_I1_001.fastq.gz
│       │       ├── test_sample_S1_L001_R1_001.fastq.gz
│       │       ├── test_sample_S1_L001_R2_001.fastq.gz
│       │       └── test_sample_S1_L001_R3_001.fastq.gz
│       └── Undetermined_*.fastq.gz
└── _* (other Cell Ranger ATAC output files)
```

### Output Characteristics

These FASTQs are:

- Valid FASTQ format for ATAC-seq
- Contain cell barcode in R2 read
- NOT suitable for downstream `cellranger-atac count`
- For mkfastq pipeline validation only

---

## Validation Checklist

After running cellranger-atac mkfastq, verify:

- [ ] Exit code is 0
- [ ] `outs/fastq_path/` directory exists
- [ ] Sample directory contains 4 FASTQ files (R1, R2, R3, I1)
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty)
- [ ] Reports and Stats directories exist

---

## Troubleshooting

### Common Issues

**Issue:** "Unrecognized sample index"  
**Solution:** Ensure using SI-NA-C1 (single-index) format in sample sheet

**Issue:** "bcl2fastq not found"  
**Solution:** Ensure bcl2fastq is installed and in PATH (cellranger-atac wraps bcl2fastq)

**Issue:** Only 3 FASTQ files instead of 4  
**Solution:** This is expected if using `--delete-undetermined`; check for R1, R2, R3, I1

---

## Additional Notes

- This is single-indexed ATAC-seq data
- The sample index SI-NA-C1 is from the 10x ATAC Single Index Kit
- Cell barcode is sequenced as part of i5 index read (R2 in output)
- Demo runs are minimal and fast to process
- The goal is pipeline validation, not scientific discovery

---

## Sample Sheet Files

### Simple CSV (Recommended)

Download: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-simple-1.0.0.csv

### IEM Format

Download: https://cf.10xgenomics.com/supp/cell-atac/cellranger-atac-tiny-bcl-samplesheet-1.0.0.csv

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
echo "Cell Ranger ATAC Tiny-BCL Run Registration"
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
        echo '  Download failed'
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


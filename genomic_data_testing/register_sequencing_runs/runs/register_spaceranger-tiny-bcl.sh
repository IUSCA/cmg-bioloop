#!/bin/bash
# =============================================================================
# Space Ranger Tiny-BCL Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads the Space Ranger tiny-bcl demo run from 10x Genomics and
#   registers it as RAW_DATA for spaceranger mkfastq conversion testing.
#
# Usage:
#   ./register_spaceranger-tiny-bcl.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to auto-detected location based on APP_ENV
#   ./register_spaceranger-tiny-bcl.sh
#
#   # Download to custom location (overrides auto-detection)
#   ./register_spaceranger-tiny-bcl.sh -d /custom/path/to/raw_data
#
# Dataset Information:
#   - Dataset: spaceranger-tiny-bcl-1.0.0.tar.gz
#   - Source: 10x Genomics Space Ranger demo data
#   - Size: ~40 MB (estimated)
#   - Type: Illumina BCL run folder for Visium spatial transcriptomics
#   - Purpose: spaceranger mkfastq conversion testing
#
# Target Pipelines:
#   - spaceranger (all versions: v1.1.0, v1.3.1, v3.0.1, etc.)
#   - spaceranger mkfastq subcommand
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
FILENAME="spaceranger-tiny-bcl-1.0.0.tar.gz"
URL="https://cf.10xgenomics.com/supp/spatial-exp/spaceranger-tiny-bcl-1.0.0.tar.gz"
DIR_NAME="spaceranger-tiny-bcl"
RUN_NAME="spaceranger-tiny-bcl"
PIPELINE="spaceranger"

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
# Space Ranger Tiny-BCL Run - spaceranger mkfastq Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/spatial-exp/spaceranger-tiny-bcl-1.0.0.tar.gz  
**File:** `spaceranger-tiny-bcl-1.0.0.tar.gz`  
**Size:** ~40 MB (estimated)

### What This Run Is

- A real Illumina BCL run folder designed for Space Ranger testing
- Visium spatial transcriptomics data
- Produced by 10x Genomics for demonstration purposes
- NOT a biologically meaningful experiment
- NOT intended for downstream count/analysis pipelines

### Purpose for Pipeline Testing

This run is ideal for validating the Space Ranger mkfastq pipeline:

```
BCL -> spaceranger mkfastq -> FASTQ files
```

**Important:** This dataset is ONLY for testing mkfastq. It cannot be used with `spaceranger count`.

---

## Target Pipelines

This dataset can be used with any of these spaceranger versions:

| Pipeline Name | Executable Path |
|--------------|-----------------|
| spaceranger | /N/project/CMG-SCA/bin/spaceranger/spaceranger |
| spaceranger-v3.0.1 | /N/project/CMG-SCA/bin/spaceranger-3.0.1/spaceranger |
| spaceranger-v1.3.1 | /N/project/CMG-SCA/bin/spaceranger-1.3.1/spaceranger |
| spaceranger-v1.1.0 | /N/project/CMG-SCA/bin/spaceranger-1.1.0/spaceranger |

---

## Recommended spaceranger mkfastq Configuration

### Simple CSV Sample Sheet

Download from: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-simple-1.0.0.csv

Content:
```csv
Lane,Sample,Index
1,test_sample,SI-TT-A1
```

### IEM Sample Sheet

Download from: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-samplesheet-1.0.0.csv

---

## Correct spaceranger mkfastq Command

### Using Simple CSV

```bash
spaceranger mkfastq \
  --id=spatial-tiny-bcl-output \
  --run=/path/to/spaceranger-tiny-bcl \
  --csv=spaceranger-tiny-bcl-simple-1.0.0.csv
```

### Using IEM Sample Sheet

```bash
spaceranger mkfastq \
  --id=spatial-tiny-bcl-output \
  --run=/path/to/spaceranger-tiny-bcl \
  --samplesheet=spaceranger-tiny-bcl-samplesheet-1.0.0.csv
```

---

## Bioloop Conversion Submission

When submitting a Conversion in Bioloop against this dataset:

### Pipeline Selection
- Select: `spaceranger`, `spaceranger-v3.0.1`, `spaceranger-v1.3.1`, or `spaceranger-v1.1.0`

### Required Arguments
The conversion should use the `mkfastq` subcommand:

```
mkfastq --run <input_dir> --csv <samplesheet>
```

### Sample Sheet
You will need to provide the simple CSV sample sheet content:

```csv
Lane,Sample,Index
1,test_sample,SI-TT-A1
```

---

## Expected Outputs

### Directory Structure After Successful Conversion

```
spatial-tiny-bcl-output/
├── outs/
│   └── fastq_path/
│       ├── Reports/
│       ├── Stats/
│       ├── tiny-bcl/
│       │   └── test_sample/
│       │       ├── test_sample_S1_L001_I1_001.fastq.gz
│       │       ├── test_sample_S1_L001_I2_001.fastq.gz
│       │       ├── test_sample_S1_L001_R1_001.fastq.gz
│       │       └── test_sample_S1_L001_R2_001.fastq.gz
│       └── Undetermined_*.fastq.gz
└── _* (other Space Ranger output files)
```

### Output Characteristics

These FASTQs are:

- Valid FASTQ format for Visium spatial data
- Dual-indexed samples
- NOT suitable for downstream `spaceranger count`
- For mkfastq pipeline validation only

---

## Validation Checklist

After running spaceranger mkfastq, verify:

- [ ] Exit code is 0
- [ ] `outs/fastq_path/` directory exists
- [ ] Sample directory contains FASTQ files
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty)
- [ ] Reports and Stats directories exist

---

## Troubleshooting

### Common Issues

**Issue:** "Unrecognized sample index"  
**Solution:** Ensure using correct SI-TT-* format in sample sheet

**Issue:** "bcl2fastq not found"  
**Solution:** Ensure bcl2fastq is installed and in PATH (spaceranger wraps bcl2fastq)

**Issue:** Pipeline hangs  
**Solution:** Check available memory; mkfastq needs adequate RAM

---

## Additional Notes

- Visium data uses dual-indexing for spatial barcoding
- Demo runs are minimal and fast to process
- The goal is pipeline validation, not scientific discovery
- Focus on successful FASTQ generation, not data quality
- Visium-specific downstream steps (count, etc.) require additional files like tissue images

---

## Sample Sheet Files

### Simple CSV (Recommended)

Download: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-simple-1.0.0.csv

### IEM Format

Download: https://cdn.10xgenomics.com/raw/upload/v1682709348/software-support/Spatial-GEX/SR-v2.1/SR-mkfastq/spaceranger-tiny-bcl-samplesheet-1.0.0.csv

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
echo "Space Ranger Tiny-BCL Run Registration"
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


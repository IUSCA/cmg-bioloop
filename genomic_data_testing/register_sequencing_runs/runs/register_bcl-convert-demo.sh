#!/bin/bash
# =============================================================================
# BCL Convert Demo Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads the iSeq-DI demo run from 10x Genomics and registers it as
#   RAW_DATA for bcl-convert conversion testing.
#
# Usage:
#   ./register_bcl-convert-demo.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to auto-detected location based on APP_ENV
#   ./register_bcl-convert-demo.sh
#
#   # Download to custom location (overrides auto-detection)
#   ./register_bcl-convert-demo.sh -d /custom/path/to/raw_data
#
# Dataset Information:
#   - Dataset: iseq-DI.tar.gz (same as bcl2fastq script)
#   - Source: 10x Genomics spatial expression demultiplexing test data
#   - Size: ~541 MB
#   - Type: Illumina iSeq dual-index run folder
#   - Purpose: bcl-convert conversion testing
#
# Target Pipelines:
#   - bcl-convert (Illumina's newer BCL to FASTQ converter)
#
# Note:
#   - bcl-convert is Illumina's successor to bcl2fastq
#   - Same dataset works for both bcl2fastq and bcl-convert
#   - Sample sheet format differs between bcl2fastq and bcl-convert
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
FILENAME="iseq-DI.tar.gz"
URL="https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz"
DIR_NAME="bcl-convert-demo"
RUN_NAME="bcl-convert-demo"
PIPELINE="bcl-convert"

# Parse command line arguments
show_help() {
    head -n 38 "$0" | tail -n +2 | sed 's/^# \?//'
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
# BCL Convert Demo Run - bcl-convert Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz  
**File:** `iseq-DI.tar.gz`  
**Size:** ~541 MB

### What This Run Is

- A real Illumina iSeq dual-index run folder
- Produced for demonstrating demultiplexing
- NOT a biologically meaningful experiment
- NOT intended to produce interpretable genome signal

### Purpose for Pipeline Testing

This run is ideal for validating the BCL Convert pipeline:

```
BCL -> bcl-convert -> FASTQ files
```

**Important:** The resulting FASTQs are structurally valid but biologically meaningless (acceptable for testing).

---

## Target Pipeline

| Pipeline Name | Executable Path |
|--------------|-----------------|
| bcl-convert | /usr/bin/bcl-convert |

**Note:** bcl-convert is Illumina's successor to bcl2fastq and is the recommended tool for new sequencing platforms.

---

## BCL Convert vs bcl2fastq

| Feature | bcl2fastq | bcl-convert |
|---------|-----------|-------------|
| Status | Legacy | Current |
| Sample Sheet | IEM format | v2 format |
| Performance | Good | Better (faster) |
| New Platforms | Limited | Full support |
| NovaSeq X | No | Yes |

---

## Recommended bcl-convert Configuration

### BCL Convert v2 Sample Sheet Format

BCL Convert uses a different sample sheet format than bcl2fastq:

```csv
[Header]
FileFormatVersion,2

[BCLConvert_Settings]
SoftwareVersion,4.4.6
CreateFastqForIndexReads,0

[BCLConvert_Data]
Sample_ID,Index,Index2
Sample1,AAAAAA,CCCCCC
Sample2,CCCCCC,AAAAAA
```

**Key Differences from bcl2fastq:**
- Uses `[BCLConvert_Settings]` instead of `[Settings]`
- Uses `[BCLConvert_Data]` instead of `[Data]`
- Different adapter trimming settings
- No lane column needed (processes all lanes by default)

---

## Correct bcl-convert Command

### Basic Invocation

```bash
bcl-convert \
  --bcl-input-directory iseq-DI \
  --output-directory fastq_out \
  --sample-sheet SampleSheet_v2.csv
```

### With Additional Options

```bash
bcl-convert \
  --bcl-input-directory iseq-DI \
  --output-directory fastq_out \
  --sample-sheet SampleSheet_v2.csv \
  --no-lane-splitting true \
  --bcl-num-conversion-threads 4 \
  --bcl-num-compression-threads 4 \
  --bcl-num-decompression-threads 4
```

---

## Bioloop Conversion Submission

When submitting a Conversion in Bioloop against this dataset:

### Pipeline Selection
- Select: `bcl-convert`

### Required Arguments

```
--bcl-input-directory <input_dir> --output-directory <output_dir> --sample-sheet <samplesheet>
```

### Sample Sheet
You will need to provide a BCL Convert v2 format sample sheet.

---

## Flag Reference

### Recommended Flags

| Flag | Description |
|------|-------------|
| `--bcl-input-directory` | Path to BCL run folder (required) |
| `--output-directory` | Output path for FASTQs (required) |
| `--sample-sheet` | Path to sample sheet (required) |
| `--no-lane-splitting true` | Merge all lanes (iSeq has single lane) |

### Performance Tuning Flags

| Flag | Description |
|------|-------------|
| `--bcl-num-conversion-threads` | Threads for BCL to FASTQ conversion |
| `--bcl-num-compression-threads` | Threads for FASTQ compression |
| `--bcl-num-decompression-threads` | Threads for BCL decompression |
| `--bcl-num-parallel-tiles` | Tiles to process in parallel |

### Optional Flags

| Flag | Description |
|------|-------------|
| `--force` | Overwrite existing output |
| `--first-tile-only` | Process only first tile (for testing) |
| `--bcl-only-matched-reads` | Skip undetermined reads |

---

## Expected Outputs

### Directory Structure After Successful Conversion

```
fastq_out/
├── Sample1/
│   ├── Sample1_S1_R1_001.fastq.gz
│   └── Sample1_S1_R2_001.fastq.gz
├── Sample2/
│   ├── Sample2_S2_R1_001.fastq.gz
│   └── Sample2_S2_R2_001.fastq.gz
├── Reports/
│   └── Demultiplex_Stats.csv
├── Logs/
│   └── Warnings.txt
└── Undetermined_S0_R1_001.fastq.gz (if not using --bcl-only-matched-reads)
```

### Output Characteristics

These FASTQs are:

- Valid for alignment (proper format and structure)
- Can produce BAM -> BigWig (complete pipeline compatibility)
- Not biologically interpretable (important but acceptable for testing)

---

## Validation Checklist

After running bcl-convert, verify:

- [ ] Exit code is 0
- [ ] Both Sample1 and Sample2 directories exist
- [ ] FASTQ files exist for each sample
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty, not suspiciously small)
- [ ] Reports/Demultiplex_Stats.csv exists
- [ ] Logs directory contains no errors

---

## Troubleshooting

### Common Issues

**Issue:** "Unsupported sample sheet version"  
**Solution:** Use BCL Convert v2 sample sheet format (see above)

**Issue:** "Input directory is not a valid BCL directory"  
**Solution:** Ensure the run folder contains RunInfo.xml and Data/Intensities/

**Issue:** "Missing BCL files"  
**Solution:** Some demo data may be incomplete; use `--ignore-missing-bcls`

**Issue:** Low demultiplexing rate  
**Solution:** Check index sequences in sample sheet match actual indices

---

## Sample Sheet Templates

### BCL Convert v2 Format (Recommended)

```csv
[Header]
FileFormatVersion,2

[BCLConvert_Settings]
SoftwareVersion,4.4.6
CreateFastqForIndexReads,0

[BCLConvert_Data]
Sample_ID,Index,Index2
Sample1,AAAAAA,CCCCCC
Sample2,CCCCCC,AAAAAA
```

### Legacy IEM Format (bcl2fastq compatible)

Some versions of bcl-convert also accept the legacy format:

```csv
[Header]
IEMFileVersion,4
Experiment Name,iSeq-DI

[Reads]
151
151

[Data]
Sample_ID,index,index2
Sample1,AAAAAA,CCCCCC
Sample2,CCCCCC,AAAAAA
```

---

## Additional Notes

- bcl-convert is faster than bcl2fastq on modern systems
- Required for NovaSeq X and newer platforms
- Supports both single-index and dual-index runs
- Demo runs may have incomplete tile data - this is expected
- The goal is pipeline validation, not scientific discovery
- Focus on successful file generation, not data quality

---

## Related Documentation

- [Illumina BCL Convert Documentation](https://support.illumina.com/sequencing/sequencing_software/bcl-convert.html)
- [BCL Convert User Guide](https://support.illumina.com/content/dam/illumina-support/documents/documentation/software_documentation/bcl_convert/bcl-convert-v4-software-guide.pdf)

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
echo "BCL Convert Demo Run Registration"
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
# Downloads to /tmp, extracts if compressed, then moves contents to destination
run_command "
    set -e
    
    # Create temporary directory for download
    TMP_DIR=\$(mktemp -d)
    cd \$TMP_DIR
    
    # Download the file using chunked download to avoid connection resets
    echo '  Downloading in chunks...'
    
    # File size in bytes
    FILE_SIZE=566891317
    CHUNK_SIZE=\$((25*1024*1024))   # 25 MiB chunks
    
    # Initialize empty file if it doesn't exist
    touch '$FILENAME'
    
    # Get current size (resume capability)
    CUR_SIZE=\$(stat -f%z '$FILENAME' 2>/dev/null || stat -c%s '$FILENAME' 2>/dev/null || echo 0)
    echo \"  Have \$CUR_SIZE / \$FILE_SIZE bytes\"
    
    start=\$CUR_SIZE
    
    while [ \"\$start\" -lt \"\$FILE_SIZE\" ]; do
        end=\$((start + CHUNK_SIZE - 1))
        if [ \"\$end\" -ge \"\$FILE_SIZE\" ]; then end=\$((FILE_SIZE - 1)); fi
        
        echo \"  Fetching bytes \$start-\$end\"
        
        if curl -L --fail --retry 50 --retry-delay 2 \
            -H \"User-Agent: Mozilla/5.0\" \
            -H \"Referer: https://www.10xgenomics.com/\" \
            -H \"Range: bytes=\$start-\$end\" \
            \"$URL\" >> '$FILENAME'; then
            start=\$((end + 1))
        else
            echo '  Download chunk failed'
            rm -rf \$TMP_DIR
            exit 1
        fi
    done
    
    echo '  Download complete'
    
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
    
    # If extraction resulted in a single directory, flatten it
    ITEM_COUNT=\$(ls -A | wc -l)
    if [ \"\$ITEM_COUNT\" -eq 1 ]; then
        SINGLE_ITEM=\$(ls -A)
        if [ -d \"\$SINGLE_ITEM\" ]; then
            echo \"  Flattening single extracted directory: \$SINGLE_ITEM\"
            mv \"\$SINGLE_ITEM\"/* .
            mv \"\$SINGLE_ITEM\"/.* . 2>/dev/null || true
            rmdir \"\$SINGLE_ITEM\"
        fi
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
    
    # Move all contents (extracted or original) to dataset directory
    echo \"  Moving contents to: \$DATASET_DIR\"
    for item in *; do
        if [ -e \"\$item\" ]; then
            mv \"\$item\" \$DATASET_DIR/
        fi
    done
    
    # Move hidden files too
    for item in .*; do
        if [ -e \"\$item\" ] && [ \"\$item\" != \".\" ] && [ \"\$item\" != \"..\" ]; then
            mv \"\$item\" \$DATASET_DIR/
        fi
    done
    
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
    echo "To check dataset file:"
    echo "  find $DESTINATION/$DIR_NAME -type f"
else
    echo "To verify dataset was created:"
    echo "  docker-compose exec $SERVICE_NAME ls -la $DESTINATION/$DIR_NAME"
    echo ""
    echo "To check dataset file:"
    echo "  docker-compose exec $SERVICE_NAME find $DESTINATION/$DIR_NAME -type f"
fi


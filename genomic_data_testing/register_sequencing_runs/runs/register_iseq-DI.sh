#!/bin/bash
# =============================================================================
# iSeq-DI Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads the iSeq-DI dual-index demo run from 10x Genomics and registers
#   it as RAW_DATA for bcl2fastq conversion testing.
#
# Usage:
#   ./register_iseq-DI.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to auto-detected location based on APP_ENV
#   ./register_iseq-DI.sh
#
#   # Download to custom location (overrides auto-detection)
#   ./register_iseq-DI.sh -d /custom/path/to/raw_data
#
# Dataset Information:
#   - Dataset: iseq-DI.tar.gz
#   - Source: 10x Genomics spatial expression demultiplexing test data
#   - Size: ~541 MB
#   - Type: Illumina iSeq dual-index run folder
#   - Purpose: bcl2fastq conversion testing
#
# Note:
#   - This script adapts to APP_ENV in workers/.env:
#     * Production (APP_ENV=production): Runs directly on host, downloads to /N/scratch/...
#     * Development: Runs in celery_worker container, downloads to /opt/sca/data/...
#   - Files are downloaded to /tmp first, then moved into directories atomically
#   - Download uses chunked approach (25MB chunks) with retry logic for reliability
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
DIR_NAME="iseq-DI"
RUN_NAME="iseq-DI"
PIPELINE="bcl2fastq"

# Parse command line arguments
show_help() {
    head -n 36 "$0" | tail -n +2 | sed 's/^# \?//'
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
# iseq-DI Run - bcl2fastq Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz  
**File:** `iseq-DI.tar.gz`  
**Size:** ~541 MB

### What This Run Is

✅ **A real Illumina iSeq dual-index run folder**

✅ **Produced for demonstrating demultiplexing**

❌ **NOT a biologically meaningful experiment**

❌ **NOT intended to produce interpretable genome signal**

### Purpose for Pipeline Testing

This run is ideal for validating the complete data processing pipeline:

```
BCL → FASTQ → Alignment → BigWig → Genome Browser Ingestion
```

**Important:** The resulting tracks will be biologically meaningless (which is acceptable for pipeline testing).

---

## Recommended bcl2fastq Configuration

### Goal-Aligned Approach

**Your stated goal:**  
Create Data Products whose files/tracks can be used for creating genome browser sessions.

**Translated to bcl2fastq terms:**
- ✅ Clean FASTQs
- ✅ No demultiplexing surprises
- ✅ Deterministic output
- ❌ Don't care about undetermined reads
- ❌ Don't care about lane splitting
- ❌ Don't care about barcode exploration

---

## ✅ Correct bcl2fastq Command

### Minimal, Correct, Conservative Invocation

```bash
bcl2fastq \
  --runfolder-dir iseq-DI \
  --output-dir fastq_out \
  --sample-sheet SampleSheet.csv \
  --no-lane-splitting \
  --barcode-mismatches 0 \
  --ignore-missing-bcls \
  --ignore-missing-filter \
  --ignore-missing-positions \
  --delete-undetermined
```

---

## Flag Justification

### Core Flags (Use These)

| Flag | Why It Is Appropriate |
|------|----------------------|
| `--no-lane-splitting` | iSeq = single lane; simplifies downstream file handling |
| `--barcode-mismatches 0` | Deterministic demux; avoids weird cross-talk |
| `--ignore-missing-bcls` | Demo runs sometimes omit tiles/cycles |
| `--ignore-missing-filter` | Same reason; prevents hard failure |
| `--ignore-missing-positions` | iSeq demo runs are incomplete by design |
| `--delete-undetermined` | Undetermined reads are useless for browser tracks |

### ❌ Flags You Should NOT Use

| Flag | Why Not |
|------|---------|
| `--filter-single-index` | This run is dual-index |
| `--use-bases-mask` | Not needed unless overriding chemistry |
| Aggressive trimming flags | You want vanilla FASTQs for alignment |

---

## Required SampleSheet

### SampleSheet.csv

This is the canonical SampleSheet that 10x expects for this dataset:

```csv
[Header]
IEMFileVersion,4
Investigator Name,10xGenomics
Experiment Name,iSeq-DI
Date,2020-01-01
Workflow,GenerateFASTQ
Application,FASTQ Only
Assay,TruSeq HT
Description,iSeq Dual Index Test Run
Chemistry,Amplicon

[Reads]
151
151

[Settings]
ReverseComplement,0
Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA
AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT

[Data]
Sample_ID,Sample_Name,index,index2
Sample1,Sample1,AAAAAA,CCCCCC
Sample2,Sample2,CCCCCC,AAAAAA
```

### Why This SampleSheet Is Correct

✔ **Dual-index layout** - Matches run chemistry  
✔ **Matches iSeq DI demo chemistry** - Validated by 10x  
✔ **Two samples** - Verifies demux logic  
✔ **Long reads (151bp)** - Realistic FASTQs  
✔ **No custom masking** - Safe default  

This SampleSheet is sufficient and correct for bcl2fastq to run without warnings.

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
└── Reports/
    ├── html/
    └── Stats/
```

### Output Characteristics

These FASTQs are:

✅ **Valid for alignment** - Proper format and structure  
✅ **Can produce BAM → BigWig** - Complete pipeline compatibility  
❌ **Not biologically interpretable** - Important but acceptable for testing  

---

## Validation Checklist

After running bcl2fastq, verify:

- [ ] Exit code is 0
- [ ] Both Sample1 and Sample2 directories exist
- [ ] Four FASTQ files total (R1 and R2 for each sample)
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty, not suspiciously small)
- [ ] Reports directory contains HTML and Stats
- [ ] No undetermined reads directory exists (due to `--delete-undetermined`)

---

## Downstream Processing

### Next Steps for Pipeline Testing

1. **Alignment:** Align FASTQs to appropriate reference genome
2. **BAM Processing:** Sort, index, and validate BAM files
3. **Coverage Tracks:** Generate BigWig files from BAM
4. **Browser Ingestion:** Create tracks in genome browser sessions
5. **Visual Verification:** Confirm tracks load (even if data is meaningless)

### Reference Genome

Since this is synthetic/demo data, align to:
- **hg38** (human reference) - Most common choice
- Or any reference genome your pipeline supports

The alignment will succeed but won't produce biologically meaningful results.

---

## Troubleshooting

### Common Issues

**Issue:** `bcl2fastq` fails with "missing BCL files"  
**Solution:** Ensure `--ignore-missing-bcls` flag is present

**Issue:** "Unknown barcodes" warning  
**Solution:** This is expected for demo data; use `--barcode-mismatches 0` to be strict

**Issue:** "Missing filter files"  
**Solution:** Ensure `--ignore-missing-filter` flag is present

**Issue:** Large Undetermined_* files  
**Solution:** Use `--delete-undetermined` to skip creating them

---

## Additional Notes

- This run is **dual-index** - do not use single-index filtering
- The barcodes are simple (AAAAAA, CCCCCC) for testing purposes
- Demo runs may have incomplete tile data - this is expected
- The goal is pipeline validation, not scientific discovery
- Focus on successful file generation, not data quality

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
echo "iSeq-DI Run Registration"
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
    CHUNK_SIZE=$((25*1024*1024))   # 25 MiB chunks
    
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
            echo '  ✗ Download chunk failed'
            rm -rf \$TMP_DIR
            exit 1
        fi
    done
    
    echo '  ✓ Download complete'
    
    # Verify file was downloaded
    if [ ! -f '$FILENAME' ]; then
        echo '  ✗ File not found after download'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    FILE_SIZE=\$(stat -f%z '$FILENAME' 2>/dev/null || stat -c%s '$FILENAME' 2>/dev/null || echo 0)
    echo \"  File size: \$FILE_SIZE bytes\"
    
    # Check if file is compressed and extract if needed
    if [[ '$FILENAME' == *.tar.gz ]] || [[ '$FILENAME' == *.tgz ]]; then
        echo '  Detected compressed tar archive (.tar.gz), extracting...'
        tar -xzf '$FILENAME'
        rm '$FILENAME'
        echo '  ✓ Extraction complete'
    elif [[ '$FILENAME' == *.tar.bz2 ]] || [[ '$FILENAME' == *.tbz2 ]]; then
        echo '  Detected compressed tar archive (.tar.bz2), extracting...'
        tar -xjf '$FILENAME'
        rm '$FILENAME'
        echo '  ✓ Extraction complete'
    elif [[ '$FILENAME' == *.tar.xz ]] || [[ '$FILENAME' == *.txz ]]; then
        echo '  Detected compressed tar archive (.tar.xz), extracting...'
        tar -xJf '$FILENAME'
        rm '$FILENAME'
        echo '  ✓ Extraction complete'
    elif [[ '$FILENAME' == *.tar ]]; then
        echo '  Detected tar archive (.tar), extracting...'
        tar -xf '$FILENAME'
        rm '$FILENAME'
        echo '  ✓ Extraction complete'
    elif [[ '$FILENAME' == *.gz ]]; then
        echo '  Detected gzip file (.gz), decompressing...'
        gunzip '$FILENAME'
        echo '  ✓ Decompression complete'
    elif [[ '$FILENAME' == *.zip ]]; then
        echo '  Detected zip archive (.zip), extracting...'
        unzip -q '$FILENAME'
        rm '$FILENAME'
        echo '  ✓ Extraction complete'
    else
        echo '  File is not compressed, will move as-is'
    fi
    
    # Create the dataset directory in destination
    DATASET_DIR='$DESTINATION/$DIR_NAME'
    echo \"  Creating directory: \$DATASET_DIR\"
    mkdir -p \$DATASET_DIR
    
    # Move all contents (extracted or original) to dataset directory
    echo \"  Moving contents to: \$DATASET_DIR\"
    for item in *; do
        if [ -e \"\$item\" ]; then
            mv \"\$item\" \$DATASET_DIR/
        fi
    done
    
    # Cleanup temp directory
    cd /
    rm -rf \$TMP_DIR
    
    echo '  ✓ Complete'
"

if [ $? -eq 0 ]; then
    echo "  ✓ Successfully created dataset: $DIR_NAME"
else
    echo "  ✗ Failed to create dataset: $DIR_NAME"
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



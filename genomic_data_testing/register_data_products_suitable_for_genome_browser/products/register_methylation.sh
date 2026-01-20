#!/bin/bash
# =============================================================================
# Methylation Track Registration Script
# =============================================================================
#
# Purpose:
#   Downloads H1 cell line MethylC-seq data from WashU Public Data and
#   registers it as DATA_PRODUCT for genome browser testing.
#
# Usage:
#   ./register_methylation.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -h, --help              Show this help message
#
# Note:
#   - This script creates detailed documentation in ../product_docs/methylation_h1_hg19.md
#
# =============================================================================

set -e

# Get the script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PRODUCT_DOC_DIR="$(dirname "$SCRIPT_DIR")/product_docs"

# Determine destination based on APP_ENV in workers/.env
WORKERS_ENV="$REPO_ROOT/workers/.env"
if [ -f "$WORKERS_ENV" ]; then
    APP_ENV=$(grep '^APP_ENV=' "$WORKERS_ENV" | cut -d '=' -f2 | tr -d '"' | tr -d "'")
fi

# Set destination based on environment
if [ "$APP_ENV" = "production" ]; then
    # Production: use path from workers/config/production.py
    DESTINATION="/N/scratch/cmguser/cmg-bioloop/origin/data_products"
else
    # Non-production: use default path
    DESTINATION="/opt/sca/data/origin/data_products"
fi

SERVICE_NAME="celery_worker"

# Dataset definition
FILENAME="h1.liftedtohg19.gz"
URL="https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz"
DIR_NAME="methylation_h1_hg19"

# Parse command line arguments
show_help() {
    head -n 16 "$0" | tail -n +2 | sed 's/^# \?//'
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

# Function to create product documentation
create_product_doc() {
    local doc_file="$PRODUCT_DOC_DIR/${DIR_NAME}.md"
    
    cat > "$doc_file" << 'EOF'
# methylation_h1_hg19 - MethylC-seq Track

## Dataset Overview

**File:** `h1.liftedtohg19.gz`  
**Source:** https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz  
**Size:** ~few MB  
**Format:** Methylation bedGraph (gzipped)  
**Genome:** hg19  
**Cell Line:** H1 (human embryonic stem cells)

### What This Dataset Is

✅ **Real methylation data from MethylC-seq experiments**

✅ **H1 cell line data lifted over to hg19 coordinates**

✅ **Suitable for testing methylation visualization**

### Purpose

This dataset is used to test:
- Methylation bedGraph file format support
- Compressed file handling (.gz)
- Methylation signal visualization
- Epigenetic data display in genome browsers

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify methylation data:

### Test Region 1
```
chr11:1950000-2120000
```
- **Chromosome:** 11
- **Region Size:** ~170 KB
- **Expected:** Methylation signal patterns visible

### Test Region 2
```
chr19:58430000-58600000
```
- **Chromosome:** 19
- **Region Size:** ~170 KB
- **Expected:** Variable methylation levels

### Test Region 3
```
chr6:32500000-33000000
```
- **Chromosome:** 6
- **Region Size:** ~500 KB
- **Expected:** Broader methylation landscape

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in:
```
/opt/sca/data/origin/data_products/methylation_h1_hg19/
```

### 2. File Format

The file is a gzipped bedGraph with methylation levels:
- Compressed with gzip
- BedGraph format: chrom, start, end, value
- Values represent methylation levels

### 3. Creating a Genome Browser Session

Once registered:
1. Navigate to `/sessions/new`
2. Select the methylation track
3. Set genome to `hg19`
4. Create the session
5. View methylation patterns in the browser

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="methylation_h1_hg19")'
```

### 2. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr11:1950000-2120000`
3. Verify methylation signal is visible
4. Compare patterns across different regions

---

## Data Source

**WashU Public Data - MethylC-seq**  
https://vizhub.wustl.edu/public/

Publicly available methylation data from H1 embryonic stem cells.

EOF

    echo "Created product documentation: $doc_file"
}

# Create documentation before downloading
echo "Creating product documentation..."
mkdir -p "$PRODUCT_DOC_DIR"
create_product_doc

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

echo ""
echo "================================"
echo "Methylation Track Registration"
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

echo "[1/1] Processing: $FILENAME"
echo "  URL: $URL"
echo "  Directory: $DIR_NAME"

# Run download and organization (in container or on host)
run_command "
    set -e
    
    TMP_DIR=\$(mktemp -d)
    cd \$TMP_DIR
    
    echo '  Downloading in chunks...'
    
    FILE_SIZE=\$(curl -sIL '$URL' | grep -i content-length | tail -1 | awk '{print \$2}' | tr -d '\\r')
    
    if [ -z \"\$FILE_SIZE\" ] || [ \"\$FILE_SIZE\" -eq 0 ]; then
        echo '  ✗ Could not determine file size'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    echo \"  Total size: \$FILE_SIZE bytes\"
    
    CHUNK_SIZE=$((10*1024*1024))
    touch '$FILENAME'
    
    CUR_SIZE=\$(stat -f%z '$FILENAME' 2>/dev/null || stat -c%s '$FILENAME' 2>/dev/null || echo 0)
    echo \"  Have \$CUR_SIZE / \$FILE_SIZE bytes\"
    
    start=\$CUR_SIZE
    
    while [ \"\$start\" -lt \"\$FILE_SIZE\" ]; do
        end=\$((start + CHUNK_SIZE - 1))
        if [ \"\$end\" -ge \"\$FILE_SIZE\" ]; then end=\$((FILE_SIZE - 1)); fi
        
        echo \"  Fetching bytes \$start-\$end\"
        
        if curl -L --fail --retry 50 --retry-delay 2 \
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
echo "Registration complete!"
echo "================================"
echo ""
echo "Downloaded dataset is in directory at:"
echo "  $DESTINATION/$DIR_NAME"
echo ""
echo "Product documentation created at:"
echo "  $PRODUCT_DOC_DIR/${DIR_NAME}.md"
echo ""
echo "Testing ranges (hg19):"
echo "  - chr11:1950000-2120000"
echo "  - chr19:58430000-58600000"
echo "  - chr6:32500000-33000000"


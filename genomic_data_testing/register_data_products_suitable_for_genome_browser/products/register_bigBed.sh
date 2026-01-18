#!/bin/bash
# =============================================================================
# BigBed Test Track Registration Script
# =============================================================================
#
# Purpose:
#   Downloads a bigBed peaks/features file from WashU Epigenome Browser and
#   registers it as DATA_PRODUCT for genome browser testing.
#
# Usage:
#   ./register_bigBed.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to default location
#   ./register_bigBed.sh
#
#   # Download to custom location
#   ./register_bigBed.sh -d /opt/sca/data/origin/data_products
#
# Dataset Information:
#   - Dataset: bigBed_test.bigBed
#   - Source: WashU Epigenome Browser Hub Sample
#   - Size: ~805 KB
#   - Type: BigBed peaks/features file
#   - Genome: hg19
#   - Purpose: Testing BigBed track loading and display
#
# Note:
#   - File is named with .bigBed extension for automatic track creation
#   - This script creates detailed documentation in ../product_docs/bigBed_test.md
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
ORIGINAL_FILENAME="bigBed1"
FILENAME="bigBed_test.bigBed"  # Renamed with proper extension
URL="https://vizhub.wustl.edu/hubSample/hg19/bigBed1"
DIR_NAME="bigBed_test"

# Parse command line arguments
show_help() {
    head -n 29 "$0" | tail -n +2 | sed 's/^# \?//'
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
# bigBed_test - Peaks/Features Track

## Dataset Overview

**File:** `bigBed_test.bigBed`  
**Source:** https://vizhub.wustl.edu/hubSample/hg19/bigBed1  
**Size:** ~805 KB  
**Format:** BigBed  
**Genome:** hg19

### What This Dataset Is

✅ **A real genomic peaks/features file in BigBed format**

✅ **Suitable for genome browser visualization testing**

✅ **Contains chromosome coordinates with associated features**

### Purpose

This dataset is used to test:
- BigBed file format support in genome browsers
- Track loading and rendering
- Feature visualization at specific genomic coordinates
- Automatic track creation from files with proper extensions

---

## File Naming Convention

**Original filename:** `bigBed1` (no extension)  
**Renamed to:** `bigBed_test.bigBed` (with extension)

### Why the Rename?

The automatic track creation system relies on file extensions to determine file type:

- ❌ `bigBed1` → Format: `null` → Role: `null` → **NO track created**
- ✅ `bigBed_test.bigBed` → Format: `BIGBED` → Role: `PRIMARY` → **Track auto-created**

**Supported extensions for auto-track creation:**
- `.bigBed` or `.bb`
- `.bigWig` or `.bw`
- `.bam`, `.cram`
- `.vcf.gz`
- `.bed.gz`
- `fragments.tsv.gz`

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify the data appears correctly:

### Test Region 1
```
chr12:6643000-6648500
```
- **Chromosome:** 12
- **Region Size:** ~5.5 KB
- **Expected:** Feature peaks should be visible

### Test Region 2
```
chr7:5566000-5571000
```
- **Chromosome:** 7
- **Region Size:** ~5 KB
- **Expected:** Feature annotations visible

### Test Region 3
```
chr8:128700000-128900000
```
- **Chromosome:** 8
- **Region Size:** ~200 KB
- **Expected:** Broader view of feature distribution

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in:
```
/opt/sca/data/origin/data_products/bigBed_test/
```

The watch.py script will:
1. Detect the new directory
2. Register it as a DATA_PRODUCT dataset
3. Extract file metadata
4. Create dataset_file entries

### 2. Track Auto-Creation

Because the file has the `.bigBed` extension:

1. **Format Detection:** `normalizeFormatFromPath('bigBed_test.bigBed')` → `'BIGBED'`
2. **Role Assignment:** `getRoleFromFormat('BIGBED')` → `'PRIMARY'`
3. **Track Creation:** Automatic track created with:
   - `name`: `bigBed_test.bigBed`
   - `dataset_file_id`: [auto-assigned]

### 3. Creating a Genome Browser Session

Once the track is created, you can:

1. Navigate to `/sessions/new`
2. Select the `bigBed_test` track from the autocomplete
3. Set genome to `hg19`
4. Create the session
5. View in IGV or WashU browser

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "annotation",
  "format": "bigbed",
  "name": "bigBed_test.bigBed",
  "url": "/api/sessions/{id}/files/expose/staged/data_products/{hash}/bigBed_test.bigBed",
  "color": "#2669a3",
  "height": 100
}
```

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigbed",
  "name": "bigBed_test.bigBed",
  "url": "/api/sessions/{id}/files/expose/staged/data_products/{hash}/bigBed_test.bigBed",
  "showOnHubLoad": true,
  "options": {
    "color": "#2669a3",
    "height": 100
  }
}
```

---

## Verification Steps

### 1. Check Dataset Registration

```bash
# Via API
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigBed_test")'

# Expected fields:
# - type: "DATA_PRODUCT"
# - is_staged: true (after staging)
# - metadata.stage_alias: "[hash]"
```

### 2. Check Track Creation

```bash
# Via API
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("bigBed_test"))'

# Expected:
# - name: "bigBed_test.bigBed"
# - dataset_file_id: [number]
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr12:6643000-6648500`
3. Verify features are visible
4. Try zooming in/out
5. Test in both IGV and WashU browsers

---

## Troubleshooting

### Issue: No track was created

**Cause:** File doesn't have proper extension or wasn't processed correctly

**Solution:**
```bash
# Check file metadata
docker exec cmg-bioloop-2-api-1 node -e "
const prisma = require('./src/db');
(async () => {
  const files = await prisma.dataset_file.findMany({
    where: { dataset: { name: 'bigBed_test' } }
  });
  console.log(JSON.stringify(files, null, 2));
  await prisma.\$disconnect();
})()
"

# Look for metadata.format and metadata.role
# Should be: format='BIGBED', role='PRIMARY'
```

### Issue: Track exists but not visible in autocomplete

**Cause:** Track may not be associated with proper genomic attributes

**Solution:**
- Verify the dataset has genomic_details set
- Check that the track query includes proper filtering

### Issue: Browser can't load the file

**Cause:** File path resolution or authentication issues

**Solution:**
1. Check `dataset.metadata.stage_alias` is set
2. Verify file is in staged location
3. Check session cookie is set before loading
4. Verify file hasn't been compressed

---

## Manual Download (For Reference)

```bash
# Direct download
curl -L -o bigBed_test.bigBed https://vizhub.wustl.edu/hubSample/hg19/bigBed1

# Verify file type (should show binary data)
file bigBed_test.bigBed

# Check file size
ls -lh bigBed_test.bigBed
```

---

## Data Source

**WashU Epigenome Browser Hub Samples**  
https://vizhub.wustl.edu/

This is publicly available test data provided by the WashU Epigenome Browser team for testing and demonstration purposes.

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
echo "BigBed Test Track Registration"
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
    
    # Create temporary directory for download
    TMP_DIR=\$(mktemp -d)
    cd \$TMP_DIR
    
    # Download the file using chunked download to avoid connection resets
    echo '  Downloading in chunks...'
    
    # Get file size from server
    FILE_SIZE=\$(curl -sIL '$URL' | grep -i content-length | tail -1 | awk '{print \$2}' | tr -d '\\r')
    
    if [ -z \"\$FILE_SIZE\" ] || [ \"\$FILE_SIZE\" -eq 0 ]; then
        echo '  ✗ Could not determine file size'
        rm -rf \$TMP_DIR
        exit 1
    fi
    
    echo \"  Total size: \$FILE_SIZE bytes\"
    
    CHUNK_SIZE=$((10*1024*1024))   # 10 MiB chunks
    
    # Initialize empty file with the new name
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
    
    # Create the dataset directory in destination and move file atomically
    DATASET_DIR='$DESTINATION/$DIR_NAME'
    echo \"  Creating directory: \$DATASET_DIR\"
    mkdir -p \$DATASET_DIR
    
    # Move the file into the dataset directory immediately (atomic operation)
    echo \"  Moving file to: \$DATASET_DIR/$FILENAME\"
    mv '$FILENAME' \$DATASET_DIR/
    
    # Cleanup temp directory
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
echo "The watch.py script should detect this new directory and register it as DATA_PRODUCT."
echo "A track will be automatically created because the file has the .bigBed extension."
echo ""
echo "Testing ranges (hg19):"
echo "  - chr12:6643000-6648500"
echo "  - chr7:5566000-5571000"
echo "  - chr8:128700000-128900000"


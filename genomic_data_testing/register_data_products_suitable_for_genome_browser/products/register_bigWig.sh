#!/bin/bash
# =============================================================================
# BigWig Signal Track Registration Script
# =============================================================================
#
# Purpose:
#   Downloads H3K4me3 ChIP-seq signal track from WashU Epigenome Browser and
#   registers it as DATA_PRODUCT for genome browser testing.
#
# Usage:
#   ./register_bigWig.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -h, --help              Show this help message
#
# Note:
#   - This script creates detailed documentation in ../product_docs/bigWig_h3k4me3_hg19.md
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
FILENAME="GSM429321.bigWig"
URL="https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig"
DIR_NAME="bigWig_h3k4me3_hg19"

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
# bigWig_h3k4me3_hg19 - H3K4me3 ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321.bigWig`  
**Source:** https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig  
**Size:** ~several MB  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K4me3)

### What This Dataset Is

✅ **Real H3K4me3 ChIP-seq signal data**

✅ **Histone modification associated with active transcription**

✅ **Suitable for testing continuous signal visualization**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Signal intensity rendering

---

## H3K4me3 Background

**H3K4me3** (Histone H3 Lysine 4 Trimethylation):
- **Function:** Marks active gene promoters
- **Location:** Typically found at transcription start sites (TSS)
- **Interpretation:** High signal = active transcription region
- **Application:** Used to identify active genes and regulatory elements

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify signal tracks:

### Test Region 1
```
chr12:6643000-6648500
```
- **Chromosome:** 12
- **Region Size:** ~5.5 KB
- **Expected:** H3K4me3 signal peaks at promoter regions

### Test Region 2
```
chr7:5566000-5571000
```
- **Chromosome:** 7
- **Region Size:** ~5 KB
- **Expected:** Signal intensity patterns visible

### Test Region 3
```
chr8:128748000-128756000
```
- **Chromosome:** 8
- **Region Size:** ~8 KB
- **Expected:** Continuous signal display

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in:
```
/opt/sca/data/origin/data_products/bigWig_h3k4me3_hg19/
```

### 2. Track Auto-Creation

Because the file has the `.bigWig` extension:
- **Format Detection:** `normalizeFormatFromPath('GSM429321.bigWig')` → `'BIGWIG'`
- **Role Assignment:** `getRoleFromFormat('BIGWIG')` → `'PRIMARY'`
- **Track Creation:** Automatic track created

### 3. Creating a Genome Browser Session

Once the track is created:
1. Navigate to `/sessions/new`
2. Select the `GSM429321.bigWig` track
3. Set genome to `hg19`
4. Create the session
5. View signal patterns in IGV or WashU

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "wig",
  "format": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "color": "#2669a3",
  "height": 100
}
```

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
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
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigWig_h3k4me3_hg19")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("GSM429321"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr12:6643000-6648500`
3. Verify signal intensity is visible
4. Look for peaks at promoter regions
5. Test zoom functionality

---

## Interpreting the Data

When viewing this track:
- **High signal** → Active promoter/transcription start site
- **Low signal** → Inactive or repressed region
- **Sharp peaks** → Well-defined TSS
- **Broad signals** → Extended regulatory regions

Compare with gene annotations to verify H3K4me3 enrichment at promoters.

---

## Data Source

**WashU Epigenome Browser Hub Samples**  
https://vizhub.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization.

EOF

    echo "Created product documentation: $doc_file"
}

# Create documentation before downloading
echo "Creating product documentation..."
create_product_doc

# Function to run commands inside the celery_worker container
run_in_container() {
    docker-compose exec -T "$SERVICE_NAME" bash -c "$1"
}

# Check if service is running
if ! docker-compose ps "$SERVICE_NAME" 2>/dev/null | grep -q "Up"; then
    echo "Error: Service '$SERVICE_NAME' is not running"
    echo "Please start the service with: docker-compose up -d"
    exit 1
fi

echo ""
echo "================================"
echo "BigWig Signal Track Registration"
echo "================================"
echo "Service: $SERVICE_NAME"
echo "Destination: $DESTINATION"
echo "Dataset: $FILENAME"
echo ""

echo "[1/1] Processing: $FILENAME"
echo "  URL: $URL"
echo "  Directory: $DIR_NAME"

# Run download and organization inside the container
run_in_container "
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
        
        if curl -L --fail --retry 50 --retry-delay 2 --retry-all-errors \
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
    
    DATASET_DIR='$DESTINATION/$DIR_NAME'
    echo \"  Creating directory: \$DATASET_DIR\"
    mkdir -p \$DATASET_DIR
    
    echo \"  Moving file to: \$DATASET_DIR/$FILENAME\"
    mv '$FILENAME' \$DATASET_DIR/
    
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
echo "  - chr12:6643000-6648500"
echo "  - chr7:5566000-5571000"
echo "  - chr8:128748000-128756000"


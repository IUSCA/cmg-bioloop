#!/bin/bash
# =============================================================================
# H3K27ac BigWig Signal Track Registration Script
# =============================================================================
#
# Purpose:
#   Downloads H3K27ac ChIP-seq signal track from WashU Epigenome Browser and
#   registers it as DATA_PRODUCT for genome browser testing.
#
# Usage:
#   ./register_h3k27ac.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (auto-detected from APP_ENV)
#   -h, --help              Show this help message
#
# Note:
#   - This script adapts to APP_ENV in workers/.env:
#     * Production (APP_ENV=production): downloads to /N/scratch/...
#     * Development: downloads to /opt/sca/data/...
#   - This script creates detailed documentation in ../product_docs/
#
# =============================================================================

set -e

# Get the script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
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
FILENAME="GSM429321_H3K27ac.bigWig"
URL="https://egg.wustl.edu/d/hg19/GSM429321_H3K27ac.bigWig"
DIR_NAME="bigWig_h3k27ac_hg19"

# Parse command line arguments
show_help() {
    head -n 18 "$0" | tail -n +2 | sed 's/^# \?//'
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
# bigWig_h3k27ac_hg19 - H3K27ac ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321_H3K27ac.bigWig`  
**Source:** https://egg.wustl.edu/d/hg19/GSM429321_H3K27ac.bigWig  
**Size:** Variable (MB range)  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K27ac)

### What This Dataset Is

✅ **Real H3K27ac ChIP-seq signal data**

✅ **Histone modification marking active enhancers and promoters**

✅ **Suitable for testing continuous signal visualization**

✅ **Complementary to H3K4me3 for regulatory element mapping**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Enhancer and promoter identification
- Multi-track epigenomic analysis (when combined with H3K4me3)

---

## H3K27ac Background

**H3K27ac** (Histone H3 Lysine 27 Acetylation):

### Function & Significance
- **Primary Role:** Marks active enhancers and promoters
- **Cellular Process:** Associated with transcriptional activation
- **Genomic Distribution:** Found at both proximal and distal regulatory elements
- **Biological Importance:** Key marker for identifying active regulatory regions

### Interpretation Guidelines
- **High signal at promoters:** Active gene transcription
- **High signal distal to genes:** Active enhancers
- **Broad peaks:** Extended regulatory domains
- **Sharp peaks:** Well-defined regulatory elements

### Comparison with H3K4me3
| Feature | H3K27ac | H3K4me3 |
|---------|---------|---------|
| Primary Location | Enhancers + Promoters | Promoters only |
| Peak Width | Broader | Narrower/Sharp |
| Regulatory Role | Activation mark | TSS marker |
| Genomic Coverage | Wider distribution | TSS-focused |

**Combined Analysis:** Using both H3K27ac and H3K4me3 together allows distinction between:
- **Active promoters:** High H3K27ac + High H3K4me3
- **Active enhancers:** High H3K27ac + Low/No H3K4me3
- **Poised promoters:** Low H3K27ac + High H3K4me3

---

## Testing Recommendations

### Suggested Genomic Regions

While this file may have data across the genome, good testing regions typically include:

#### Known Enhancer-Rich Regions
```
chr8:128700000-128900000
```
- **Region Type:** Super-enhancer region
- **Expected Pattern:** Broad H3K27ac domains
- **Cell Type Specific:** May vary by sample

#### Active Gene Promoters
```
chr12:6640000-6650000
```
- **Region Type:** Gene promoter regions
- **Expected Pattern:** Sharp H3K27ac peaks at TSS
- **Compare with:** H3K4me3 signal (should overlap)

#### Random Sampling for Validation
```
chr7:5500000-5600000
```
- **Region Type:** General genomic region
- **Expected Pattern:** Mix of peaks and background
- **Use For:** Overall data quality assessment

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in:
```
/opt/sca/data/origin/data_products/bigWig_h3k27ac_hg19/
```

### 2. Track Auto-Creation

Because the file has the `.bigWig` extension:
- **Format Detection:** `normalizeFormatFromPath('GSM429321_H3K27ac.bigWig')` → `'BIGWIG'`
- **Role Assignment:** `getRoleFromFormat('BIGWIG')` → `'PRIMARY'`
- **Track Creation:** Automatic track created

### 3. Creating a Genome Browser Session

Once the track is created:
1. Navigate to `/sessions/new`
2. Select the `GSM429321_H3K27ac.bigWig` track
3. Optionally add the H3K4me3 track for comparison
4. Set genome to `hg19`
5. Create the session
6. View signal patterns in IGV or WashU

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "wig",
  "format": "bigwig",
  "name": "GSM429321_H3K27ac.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "color": "#FF8C00",
  "height": 100
}
```

**Recommended Color:** Orange/Dark Orange (#FF8C00) to distinguish from H3K4me3

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigwig",
  "name": "GSM429321_H3K27ac.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "showOnHubLoad": true,
  "options": {
    "color": "#FF8C00",
    "height": 100
  }
}
```

---

## Multi-Track Analysis

### Combining H3K27ac and H3K4me3

When viewing both marks together:

1. **Load both tracks** in the same session
2. **Use same vertical scale** for fair comparison
3. **Look for patterns:**
   - Overlapping peaks → Active promoters
   - H3K27ac only → Active enhancers
   - H3K4me3 only → Poised/bivalent promoters

### Visual Analysis Workflow

```
Step 1: Load H3K27ac track
Step 2: Load H3K4me3 track  
Step 3: Navigate to promoter region
Step 4: Observe peak overlap (both marks present)
Step 5: Navigate to distal region
Step 6: Observe H3K27ac peaks without H3K4me3 (enhancers)
Step 7: Cross-reference with gene annotations
```

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigWig_h3k27ac_hg19")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("H3K27ac"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to suggested genomic regions
3. Verify signal intensity is visible
4. Look for both sharp and broad peaks
5. Test zoom functionality
6. Compare with H3K4me3 track if available

### 4. Visual Quality Checks

- ✅ Signal-to-noise ratio is reasonable
- ✅ Peaks are clearly visible above background
- ✅ No extreme outliers or artifacts
- ✅ Consistent signal distribution across regions
- ✅ Smooth signal rendering at different zoom levels

---

## Interpreting the Data

### Signal Patterns

| Signal Pattern | Biological Interpretation |
|----------------|---------------------------|
| High sharp peaks at TSS | Active gene promoters |
| Broad peaks distal to genes | Active enhancers |
| Multiple peaks clustered | Super-enhancer regions |
| Low/no signal | Inactive or repressed regions |

### Common Observations

- **Promoters:** Sharp peaks coinciding with H3K4me3
- **Enhancers:** Broader peaks without H3K4me3
- **Gene Bodies:** Generally low signal (not expected here)
- **Intergenic:** Variable signal depending on regulatory elements

---

## Data Source

**WashU Epigenome Browser Data**  
https://egg.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization and regulatory element mapping.

---

## Additional Resources

### Understanding H3K27ac
- ENCODE ChIP-seq guidelines
- Roadmap Epigenomics project documentation
- H3K27ac vs H3K4me3 comparison studies

### Enhancer Identification
- Use H3K27ac peaks distal to TSS
- Cross-reference with DNase hypersensitivity data
- Validate with gene expression correlation

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
echo "H3K27ac BigWig Track Registration"
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
echo "Suggested testing regions (hg19):"
echo "  - chr8:128700000-128900000 (super-enhancer region)"
echo "  - chr12:6640000-6650000 (promoter regions)"
echo "  - chr7:5500000-5600000 (general validation)"
echo ""
echo "Tip: Load with H3K4me3 track for comparative analysis"



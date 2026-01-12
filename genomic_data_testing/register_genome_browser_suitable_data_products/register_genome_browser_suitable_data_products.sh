#!/bin/bash
# =============================================================================
# Genome Browser Suitable Data Products Registration Script
# =============================================================================
#
# Purpose:
#   Downloads genome browser compatible test datasets and places them in
#   individual directories for ingestion by the watch.py script as DATA_PRODUCT.
#
# Usage:
#   ./register_genome_browser_suitable_data_products.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -n, --number NUM         Number of datasets to download (1-3, default: all)
#   -h, --help              Show this help message
#
# Examples:
#   # Download all datasets to default location
#   ./register_genome_browser_suitable_data_products.sh
#
#   # Download only the first dataset (smallest file)
#   ./register_genome_browser_suitable_data_products.sh -n 1
#
#   # Download to custom location
#   ./register_genome_browser_suitable_data_products.sh -d /opt/sca/data/origin/data_products
#
#   # Download 2 datasets to custom location
#   ./register_genome_browser_suitable_data_products.sh -d /custom/path -n 2
#
# Dataset Information:
#   Datasets are ordered by size (smallest to largest):
#   1. bigBed1 (~few KB) - Peaks/Features file
#   2. h1.liftedtohg19.gz (~few MB) - Methylation MethylC-seq
#   3. GSM429321.bigWig (~larger) - H3K4me3 Signal track
#
# Note:
#   - This script executes commands inside the celery_worker service via docker-compose
#   - Files are downloaded to /tmp first, then moved into directories atomically
#   - Each file is placed in its own directory for watch.py to detect as a DATA_PRODUCT dataset
#   - Download uses chunked approach (10MB chunks) with retry logic for reliability
#   - Chunked downloading prevents connection reset issues
#   - See data_products_info.md for testing ranges and detailed information
#
# =============================================================================

set -e

# Default configuration
DESTINATION="/opt/sca/data/origin/data_products"
NUM_DATASETS=3  # Default: download all
SERVICE_NAME="celery_worker"

# Dataset definitions (ordered by size: smallest to largest)
# Format: "filename:url"
declare -a DATASETS=(
    "bigBed1:https://vizhub.wustl.edu/hubSample/hg19/bigBed1"
    "h1.liftedtohg19.gz:https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz"
    "GSM429321.bigWig:https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig"
)

# Parse command line arguments
show_help() {
    head -n 43 "$0" | tail -n +2 | sed 's/^# \?//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--destination)
            DESTINATION="$2"
            shift 2
            ;;
        -n|--number)
            NUM_DATASETS="$2"
            if ! [[ "$NUM_DATASETS" =~ ^[1-3]$ ]]; then
                echo "Error: Number must be between 1 and 3"
                exit 1
            fi
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

echo "================================"
echo "Genome Browser Data Products Registration"
echo "================================"
echo "Service: $SERVICE_NAME"
echo "Destination: $DESTINATION"
echo "Datasets to download: $NUM_DATASETS of ${#DATASETS[@]}"
echo ""

# Download and organize datasets
for i in $(seq 0 $((NUM_DATASETS - 1))); do
    dataset_info="${DATASETS[$i]}"
    filename=$(echo "$dataset_info" | cut -d: -f1)
    url=$(echo "$dataset_info" | cut -d: -f2-)
    
    # Create a directory name based on the filename (remove extension for clean directory name)
    # For files with multiple extensions (e.g., .liftedtohg19.gz), use the base name
    dir_name=$(basename "$filename" | sed -E 's/\.(gz|bigWig|bigBed)$//' | sed -E 's/\.[^.]*$//')
    
    # If dir_name is still the same as filename, use filename without any extension
    if [ "$dir_name" = "$filename" ]; then
        dir_name=$(basename "$filename" | cut -d. -f1)
    fi
    
    echo "[$((i + 1))/$NUM_DATASETS] Processing: $filename"
    echo "  URL: $url"
    echo "  Directory: $dir_name"
    
    # Run download and organization inside the container
    # Downloads to /tmp, then creates directory and moves file atomically
    run_in_container "
        set -e
        
        # Create temporary directory for download
        TMP_DIR=\$(mktemp -d)
        cd \$TMP_DIR
        
        # Download the file using chunked download to avoid connection resets
        echo '  Downloading in chunks...'
        
        # Get file size from server
        FILE_SIZE=\$(curl -sIL '$url' | grep -i content-length | tail -1 | awk '{print \$2}' | tr -d '\\r')
        
        if [ -z \"\$FILE_SIZE\" ] || [ \"\$FILE_SIZE\" -eq 0 ]; then
            echo '  ✗ Could not determine file size'
            rm -rf \$TMP_DIR
            exit 1
        fi
        
        echo \"  Total size: \$FILE_SIZE bytes\"
        
        CHUNK_SIZE=$((10*1024*1024))   # 10 MiB chunks
        
        # Initialize empty file
        touch '$filename'
        
        # Get current size (resume capability)
        CUR_SIZE=\$(stat -f%z '$filename' 2>/dev/null || stat -c%s '$filename' 2>/dev/null || echo 0)
        echo \"  Have \$CUR_SIZE / \$FILE_SIZE bytes\"
        
        start=\$CUR_SIZE
        
        while [ \"\$start\" -lt \"\$FILE_SIZE\" ]; do
            end=\$((start + CHUNK_SIZE - 1))
            if [ \"\$end\" -ge \"\$FILE_SIZE\" ]; then end=\$((FILE_SIZE - 1)); fi
            
            echo \"  Fetching bytes \$start-\$end\"
            
            if curl -L --fail --retry 50 --retry-delay 2 --retry-all-errors \
                -H \"Range: bytes=\$start-\$end\" \
                \"$url\" >> '$filename'; then
                start=\$((end + 1))
            else
                echo '  ✗ Download chunk failed'
                rm -rf \$TMP_DIR
                exit 1
            fi
        done
        
        echo '  ✓ Download complete'
        
        # Verify file was downloaded
        if [ ! -f '$filename' ]; then
            echo '  ✗ File not found after download'
            rm -rf \$TMP_DIR
            exit 1
        fi
        
        FILE_SIZE=\$(stat -f%z '$filename' 2>/dev/null || stat -c%s '$filename' 2>/dev/null || echo 0)
        echo \"  File size: \$FILE_SIZE bytes\"
        
        # Create the dataset directory in destination and move file atomically
        DATASET_DIR='$DESTINATION/$dir_name'
        echo \"  Creating directory: \$DATASET_DIR\"
        mkdir -p \$DATASET_DIR
        
        # Move the file into the dataset directory immediately (atomic operation)
        echo \"  Moving file to: \$DATASET_DIR/$filename\"
        mv '$filename' \$DATASET_DIR/
        
        # Cleanup temp directory
        rm -rf \$TMP_DIR
        
        echo '  ✓ Complete'
    "
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully created dataset: $dir_name"
    else
        echo "  ✗ Failed to create dataset: $dir_name"
    fi
    
    echo ""
done

echo "================================"
echo "Registration complete!"
echo "================================"
echo ""
echo "Downloaded datasets are in individual directories at:"
echo "  $DESTINATION"
echo ""
echo "The watch.py script should detect these new directories and register them as DATA_PRODUCT."
echo ""
echo "To verify datasets were created:"
echo "  docker-compose exec $SERVICE_NAME ls -la $DESTINATION"
echo ""
echo "To check dataset file contents:"
echo "  docker-compose exec $SERVICE_NAME find $DESTINATION -type f"



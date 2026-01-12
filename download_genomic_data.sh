#!/bin/bash
# =============================================================================
# Genome Browser Data Downloader
# =============================================================================
#
# Purpose:
#   Downloads genome browser compatible test datasets and places them in
#   individual directories for ingestion by the watch.py script.
#
# Usage:
#   ./download_genomic_data.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -n, --number NUM         Number of datasets to download (1-4, default: all)
#   -h, --help              Show this help message
#
# Examples:
#   # Download all datasets to default location
#   ./download_genomic_data.sh
#
#   # Download only the first 2 datasets (smallest files)
#   ./download_genomic_data.sh -n 2
#
#   # Download to custom location
#   ./download_genomic_data.sh -d /opt/sca/data/origin/data_products
#
#   # Download 3 datasets to custom location
#   ./download_genomic_data.sh -d /custom/path -n 3
#
# Dataset Information:
#   Datasets are ordered by size (smallest to largest):
#   1. bigBed1 (~few KB) - Feature file
#   2. E017_15_coreMarks_dense.gz (~few MB) - ChromHMM Chromatin States
#   3. h1.liftedtohg19.gz (~few MB) - Methylation MethylC-seq
#   4. GSM429321.bigWig (~larger) - H3K4me3 Signal
#
# Note:
#   - This script executes commands inside the celery_worker service via docker-compose
#   - Files are downloaded to /tmp first, then moved into directories atomically
#   - Each file is placed in its own directory for watch.py to detect as a dataset
#
# =============================================================================

set -e

# Default configuration
DESTINATION="/opt/sca/data/origin/data_products"
NUM_DATASETS=4  # Default: download all
SERVICE_NAME="celery_worker"

# Dataset definitions (ordered by size: smallest to largest)
# Format: "filename:url"
declare -a DATASETS=(
    "bigBed1:https://vizhub.wustl.edu/hubSample/hg19/bigBed1"
    "E017_15_coreMarks_dense.gz:https://egg.wustl.edu/d/hg19/E017_15_coreMarks_dense.gz"
    "h1.liftedtohg19.gz:https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz"
    "GSM429321.bigWig:https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig"
)

# Parse command line arguments
show_help() {
    head -n 45 "$0" | tail -n +2 | sed 's/^# \?//'
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
            if ! [[ "$NUM_DATASETS" =~ ^[1-4]$ ]]; then
                echo "Error: Number must be between 1 and 4"
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
echo "Genome Browser Data Downloader"
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
        
        # Download the file
        echo '  Downloading...'
        if curl -L -f -o '$filename' '$url' 2>&1; then
            echo '  ✓ Download complete'
        else
            echo '  ✗ Download failed'
            rm -rf \$TMP_DIR
            exit 1
        fi
        
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
echo "Download complete!"
echo "================================"
echo ""
echo "Downloaded datasets are in individual directories at:"
echo "  $DESTINATION"
echo ""
echo "The watch.py script should detect these new directories and begin registration."
echo ""
echo "To verify datasets were created:"
echo "  docker-compose exec $SERVICE_NAME ls -la $DESTINATION"
echo ""
echo "To check dataset file contents:"
echo "  docker-compose exec $SERVICE_NAME find $DESTINATION -type f"


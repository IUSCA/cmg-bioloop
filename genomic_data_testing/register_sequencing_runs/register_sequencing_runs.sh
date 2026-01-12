#!/bin/bash
# =============================================================================
# Sequencing Run Registration Script
# =============================================================================
#
# Purpose:
#   Downloads 10x Genomics sequencing run test data and places it in a
#   directory for ingestion by the watch.py script as RAW_DATA.
#
# Usage:
#   ./register_sequencing_runs.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/raw_data)
#   -h, --help              Show this help message
#
# Examples:
#   # Download to default location
#   ./register_sequencing_runs.sh
#
#   # Download to custom location
#   ./register_sequencing_runs.sh -d /opt/sca/data/origin/raw_data
#
# Dataset Information:
#   - Dataset: iseq-DI.tar.gz
#   - Source: 10x Genomics spatial expression demultiplexing test data
#   - Size: Variable (download includes resume capability)
#   - Type: Sequencing run archive
#
# Note:
#   - This script executes commands inside the celery_worker service via docker-compose
#   - Files are downloaded to /tmp first, then moved into directories atomically
#   - The file is placed in its own directory for watch.py to detect as a RAW_DATA dataset
#   - Download uses chunked approach (25MB chunks) with retry logic for reliability
#   - Chunked downloading prevents connection reset issues on large files
#
# =============================================================================

set -e

# Default configuration
DESTINATION="/opt/sca/data/origin/raw_data"
SERVICE_NAME="celery_worker"

# Dataset definition
FILENAME="iseq-DI.tar.gz"
URL="https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz"
DIR_NAME="iseq-DI"

# Parse command line arguments
show_help() {
    head -n 35 "$0" | tail -n +2 | sed 's/^# \?//'
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
echo "Sequencing Run Registration"
echo "================================"
echo "Service: $SERVICE_NAME"
echo "Destination: $DESTINATION"
echo "Dataset: $FILENAME"
echo ""

echo "[1/1] Processing: $FILENAME"
echo "  URL: $URL"
echo "  Directory: $DIR_NAME"

# Run download and organization inside the container
# Downloads to /tmp, then creates directory and moves file atomically
run_in_container "
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
        
        if curl -L --fail --retry 50 --retry-delay 2 --retry-all-errors \
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
echo "The watch.py script should detect this new directory and register it as RAW_DATA."
echo ""
echo "To verify dataset was created:"
echo "  docker-compose exec $SERVICE_NAME ls -la $DESTINATION/$DIR_NAME"
echo ""
echo "To check dataset file:"
echo "  docker-compose exec $SERVICE_NAME find $DESTINATION/$DIR_NAME -type f"


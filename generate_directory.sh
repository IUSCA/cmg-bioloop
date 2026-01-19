#!/bin/bash

################################################################################
# Script: create_dummy_dir.sh
# Purpose: Create a directory with dummy data of specified size
#
# Usage:
#   ./create_dummy_dir.sh [size_in_mb] [directory_name]
#
# Arguments:
#   size_in_mb      - Size in megabytes (default: 10)
#   directory_name  - Name of directory (default: dummy-[size]mb-[timestamp])
#
# Examples:
#   # Create 10MB directory with auto-generated name (dummy-10mb-20260117_143052)
#   ./create_dummy_dir.sh
#
#   # Create 50MB directory with auto-generated name (dummy-50mb-20260117_143100)
#   ./create_dummy_dir.sh 50
#
#   # Create 25MB directory with custom name
#   ./create_dummy_dir.sh 25 my-test-data
#
#   # Create 100MB directory in /tmp
#   cd /tmp && ./create_dummy_dir.sh 100
#
#   # Create 5MB directory with descriptive name
#   ./create_dummy_dir.sh 5 test-upload-$(date +%Y%m%d)
#
# Output:
#   - Creates directory with specified name
#   - Fills with binary dummy files (~1MB each)
#   - Shows progress dots during creation
#   - Displays summary with actual size and file count
#
# Notes:
#   - Uses /dev/urandom for dummy data generation
#   - Creates multiple 1MB files to reach target size
#   - Actual size may vary slightly due to filesystem overhead
#   - Directory created in current working directory
#
# Cleanup:
#   rm -rf [directory_name]
#
################################################################################

set -e

# Default values
DEFAULT_SIZE_MB=10
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Parse arguments
SIZE_MB=${1:-$DEFAULT_SIZE_MB}
DIR_NAME=${2:-"dummy-${SIZE_MB}mb-${TIMESTAMP}"}

# Validate size is a number
if ! [[ "$SIZE_MB" =~ ^[0-9]+$ ]]; then
    echo "Error: Size must be a positive integer (MB)"
    echo ""
    echo "Usage: $0 [size_in_mb] [directory_name]"
    echo ""
    echo "Examples:"
    echo "  $0                    # Create 10MB with default name"
    echo "  $0 50                 # Create 50MB with default name"
    echo "  $0 25 my-test-data    # Create 25MB with custom name"
    exit 1
fi

# Create directory
echo "Creating directory: $DIR_NAME"
mkdir -p "$DIR_NAME"

# Calculate how many files to create
# We'll create files of ~1MB each to reach the target size
FILE_SIZE_MB=1
NUM_FILES=$SIZE_MB
REMAINDER_KB=0

if [ "$SIZE_MB" -lt 1 ]; then
    # If less than 1MB, create a single smaller file
    NUM_FILES=1
    REMAINDER_KB=$((SIZE_MB * 1024))
fi

echo "Generating ${SIZE_MB}MB of dummy data in $NUM_FILES file(s)..."

# Create 1MB files
for i in $(seq 1 $NUM_FILES); do
    dd if=/dev/urandom of="$DIR_NAME/dummy_file_$i.bin" bs=1M count=1 status=none 2>/dev/null
    echo -n "."
done

echo ""
echo "✓ Directory created successfully!"
echo ""
echo "Directory: $DIR_NAME"
echo "Size: $(du -sh "$DIR_NAME" | cut -f1)"
echo "Files: $(ls -1 "$DIR_NAME" | wc -l | tr -d ' ')"
echo ""
echo "To remove: rm -rf $DIR_NAME"


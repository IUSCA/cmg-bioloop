#!/bin/bash
# =============================================================================
# Genome Browser Data Products Registration Script
# =============================================================================
#
# Purpose:
#   Master script that calls individual data product registration scripts
#   to download and register genome browser test datasets.
#
# Usage:
#   ./register_data_products.sh [OPTIONS]
#
# Options:
#   -d, --destination DIR    Destination directory (default: /opt/sca/data/origin/data_products)
#   -n, --number NUM         Number of datasets to download (1-3, default: all)
#   -h, --help              Show this help message
#
# Examples:
#   # Download all datasets to default location
#   ./register_data_products.sh
#
#   # Download only the first dataset (bigBed - smallest file)
#   ./register_data_products.sh -n 1
#
#   # Download to custom location
#   ./register_data_products.sh -d /opt/sca/data/origin/data_products
#
#   # Download 2 datasets to custom location
#   ./register_data_products.sh -d /custom/path -n 2
#
# Dataset Information:
#   Datasets are ordered by size (smallest to largest):
#   1. bigBed_test (~805 KB) - Peaks/Features file
#   2. methylation_h1_hg19 (~few MB) - MethylC-seq
#   3. bigWig_h3k4me3_hg19 (~larger) - H3K4me3 Signal track
#
# Note:
#   - This script calls individual registration scripts in the products/ directory
#   - Each script creates its own documentation in product_docs/[productName].md
#   - Files with proper extensions (.bigBed, .bigWig) will have tracks auto-created
#   - See data_products_info.md for overview information
#
# =============================================================================

set -e

# Get script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PRODUCTS_DIR="$SCRIPT_DIR/products"

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

NUM_DATASETS=3  # Default: download all

# Dataset scripts (ordered by size: smallest to largest)
declare -a DATASET_SCRIPTS=(
    "register_bigBed.sh"
    "register_methylation.sh"
    "register_bigWig.sh"
)

declare -a DATASET_NAMES=(
    "bigBed_test"
    "Methylation (H1)"
    "BigWig (H3K4me3)"
)

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

echo "========================================"
echo "Genome Browser Data Products Registration"
echo "========================================"
echo "Destination: $DESTINATION"
echo "Datasets to download: $NUM_DATASETS of ${#DATASET_SCRIPTS[@]}"
echo ""

# Download and register datasets
for i in $(seq 0 $((NUM_DATASETS - 1))); do
    script="${DATASET_SCRIPTS[$i]}"
    dataset_name="${DATASET_NAMES[$i]}"
    script_path="$PRODUCTS_DIR/$script"
    
    echo "[$((i + 1))/$NUM_DATASETS] Registering: $dataset_name"
    echo "  Script: $script"
    echo ""
    
    # Check if script exists
    if [ ! -f "$script_path" ]; then
        echo "  ✗ Script not found: $script_path"
        continue
    fi
    
    # Make script executable if not already
    chmod +x "$script_path"
    
    # Run the registration script
    if "$script_path" -d "$DESTINATION"; then
        echo "  ✓ Successfully registered: $dataset_name"
    else
        echo "  ✗ Failed to register: $dataset_name"
    fi
    
    echo ""
done

echo "========================================"
echo "Registration complete!"
echo "========================================"
echo ""
echo "Downloaded datasets are in individual directories at:"
echo "  $DESTINATION"
echo ""
echo "Product documentation created at:"
echo "  $SCRIPT_DIR/product_docs/"
echo ""
echo "The watch.py script should detect these new directories and register them as DATA_PRODUCT."
echo "Files with proper extensions will have tracks auto-created."
echo ""
echo "For more information:"
echo "  - See $SCRIPT_DIR/data_products_info.md"
echo "  - See $PRODUCTS_DIR/README.md"
echo "  - See individual product docs in $SCRIPT_DIR/product_docs/"


#!/bin/bash
set -e

# =============================================================================
# Conversion Pipelines Installation Script (Production - RedHat)
# =============================================================================
#
# Purpose:
#   Downloads and installs genomic conversion pipelines for production RedHat
#   environment where tools are accessed by pm2-managed Python processes
#
# Usage:
#   sudo ./install_conversion_pipelines_production.sh [--install-dir /path/to/install]
#
# Requirements:
#   - RedHat/CentOS/Rocky Linux
#   - sudo access
#   - wget or curl
#   - Internet access for downloads
#
# Default Installation:
#   /usr/local/bin/ - for executables
#   /opt/10x-genomics/ - for 10x Genomics tools (cellranger, spaceranger)
#   /opt/illumina/ - for Illumina tools (bcl2fastq, bcl-convert)
#
# =============================================================================

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default installation directories
INSTALL_BASE_DIR="/opt/sca/data/conversion"
TMP_DIR="/tmp/bioloop_pipeline_install"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --install-dir)
            INSTALL_BASE_DIR="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--install-dir /path/to/install]"
            echo ""
            echo "Options:"
            echo "  --install-dir   Base directory for installation (default: $INSTALL_BASE_DIR)"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}==================================================================="
echo "Bioloop Conversion Pipelines Installation Script"
echo "===================================================================${NC}"
echo ""
echo "Installation Base Directory: $INSTALL_BASE_DIR"
echo "Temporary Directory: $TMP_DIR"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}ERROR: This script must be run with sudo${NC}"
    exit 1
fi

# Create temporary directory
mkdir -p "$TMP_DIR"
cd "$TMP_DIR"

# Create base directories
mkdir -p "$INSTALL_BASE_DIR"

# Function to create installation directory
create_install_dir() {
    local dir=$1
    mkdir -p "$dir/bin"
    mkdir -p "$dir/logs"
    echo -e "${GREEN}Created directory structure: $dir${NC}"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to download file
download_file() {
    local url=$1
    local output=$2
    
    echo -e "${BLUE}Downloading: $url${NC}"
    
    if command_exists wget; then
        wget -q --show-progress "$url" -O "$output"
    elif command_exists curl; then
        curl -L "$url" -o "$output"
    else
        echo -e "${RED}ERROR: Neither wget nor curl found. Please install one of them.${NC}"
        exit 1
    fi
}

echo -e "${YELLOW}-------------------------------------------------------------------"
echo "IMPORTANT NOTES FOR PRODUCTION INSTALLATION"
echo "-------------------------------------------------------------------${NC}"
echo ""
echo "This script will guide you through installing conversion pipelines."
echo "Some tools require manual download due to licensing:"
echo ""
echo "  1. bcl2fastq (v2.20.0.422) - Requires Illumina account"
echo "     Download from: https://support.illumina.com/downloads/bcl2fastq-conversion-software-v2-20.html"
echo ""
echo "  2. bcl-convert (v4.3.6) - Requires Illumina account"
echo "     Download from: https://support.illumina.com/sequencing/sequencing_software/bcl-convert.html"
echo ""
echo "  3. 10x Genomics Tools - Requires registration"
echo "     - CellRanger (v8.0.1, v6.1.2, v4.0.0)"
echo "     - CellRanger-ARC (v1.0.0, v2.0.0)"
echo "     - CellRanger-ATAC (v1.2.0)"
echo "     - SpaceRanger (v3.0.1, v1.3.1, v1.1.0)"
echo "     Download from: https://www.10xgenomics.com/support/software"
echo ""
echo -e "${YELLOW}Press Enter to continue or Ctrl+C to abort...${NC}"
read

# =============================================================================
# BCL2FASTQ Installation
# =============================================================================
echo ""
echo -e "${BLUE}==================================================================="
echo "1. bcl2fastq v2.20.0.422"
echo "===================================================================${NC}"

BCL2FASTQ_DIR="$INSTALL_BASE_DIR/bcl2fastq"
create_install_dir "$BCL2FASTQ_DIR"

echo ""
echo "Please manually download bcl2fastq v2.20.0.422 RPM from Illumina:"
echo "  https://support.illumina.com/downloads/bcl2fastq-conversion-software-v2-20.html"
echo ""
echo "Select: bcl2fastq2-v2.20.0.422-Linux-x86_64.rpm"
echo ""
echo "After downloading, provide the path to the RPM file:"
read -p "RPM file path (or 'skip' to skip): " BCL2FASTQ_RPM

if [ "$BCL2FASTQ_RPM" != "skip" ] && [ -f "$BCL2FASTQ_RPM" ]; then
    echo "Installing bcl2fastq from RPM..."
    rpm -ivh "$BCL2FASTQ_RPM" || echo "RPM installation may have failed or already installed"
    
    # Copy binary to our installation directory
    if [ -f "/usr/local/bin/bcl2fastq" ]; then
        cp /usr/local/bin/bcl2fastq "$BCL2FASTQ_DIR/bin/"
        chmod +x "$BCL2FASTQ_DIR/bin/bcl2fastq"
        echo -e "${GREEN}✓ bcl2fastq installed successfully${NC}"
        "$BCL2FASTQ_DIR/bin/bcl2fastq" --version || true
    else
        echo -e "${YELLOW}⚠ bcl2fastq binary not found in /usr/local/bin${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Skipping bcl2fastq installation${NC}"
fi

# =============================================================================
# BCL-CONVERT Installation
# =============================================================================
echo ""
echo -e "${BLUE}==================================================================="
echo "2. bcl-convert v4.3.6"
echo "===================================================================${NC}"

BCLCONVERT_DIR="$INSTALL_BASE_DIR/bcl-convert"
create_install_dir "$BCLCONVERT_DIR"

echo ""
echo "Please manually download BCL Convert v4.3.6 RPM from Illumina:"
echo "  https://support.illumina.com/sequencing/sequencing_software/bcl-convert.html"
echo ""
echo "Select: bcl-convert-4.3.6-2.el7.x86_64.rpm (or similar for your OS)"
echo ""
echo "After downloading, provide the path to the RPM file:"
read -p "RPM file path (or 'skip' to skip): " BCLCONVERT_RPM

if [ "$BCLCONVERT_RPM" != "skip" ] && [ -f "$BCLCONVERT_RPM" ]; then
    echo "Installing bcl-convert from RPM..."
    rpm -ivh "$BCLCONVERT_RPM" || echo "RPM installation may have failed or already installed"
    
    # Copy binary to our installation directory
    if [ -f "/usr/local/bin/bcl-convert" ]; then
        cp /usr/local/bin/bcl-convert "$BCLCONVERT_DIR/bin/"
        chmod +x "$BCLCONVERT_DIR/bin/bcl-convert"
        echo -e "${GREEN}✓ bcl-convert installed successfully${NC}"
        "$BCLCONVERT_DIR/bin/bcl-convert" --version || true
    else
        echo -e "${YELLOW}⚠ bcl-convert binary not found in /usr/local/bin${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Skipping bcl-convert installation${NC}"
fi

# =============================================================================
# 10x Genomics Tools Installation
# =============================================================================

# Function to install 10x Genomics tool
install_10x_tool() {
    local tool_name=$1
    local version=$2
    local tarball_name=$3
    local install_subdir=$4
    
    echo ""
    echo -e "${BLUE}==================================================================="
    echo "$tool_name $version"
    echo "===================================================================${NC}"
    
    local TOOL_DIR="$INSTALL_BASE_DIR/$install_subdir"
    create_install_dir "$TOOL_DIR"
    
    echo ""
    echo "Please manually download $tool_name $version from 10x Genomics:"
    echo "  https://www.10xgenomics.com/support/software"
    echo ""
    echo "Expected file name: $tarball_name"
    echo ""
    echo "After downloading, provide the path to the tar.gz file:"
    read -p "Tar.gz file path (or 'skip' to skip): " TOOL_TARBALL
    
    if [ "$TOOL_TARBALL" != "skip" ] && [ -f "$TOOL_TARBALL" ]; then
        echo "Extracting $tool_name..."
        tar -xzf "$TOOL_TARBALL" -C "$TMP_DIR"
        
        # Find the extracted directory (may vary)
        local extracted_dir=$(find "$TMP_DIR" -maxdepth 1 -type d -name "${tool_name}*${version}*" | head -n 1)
        
        if [ -z "$extracted_dir" ]; then
            # Try without version in directory name
            extracted_dir=$(find "$TMP_DIR" -maxdepth 1 -type d -name "${tool_name}*" | head -n 1)
        fi
        
        if [ -n "$extracted_dir" ]; then
            echo "Installing from: $extracted_dir"
            cp -r "$extracted_dir"/* "$TOOL_DIR/"
            
            # Make main executable executable
            if [ -f "$TOOL_DIR/bin/$tool_name" ]; then
                chmod +x "$TOOL_DIR/bin/$tool_name"
                echo -e "${GREEN}✓ $tool_name $version installed successfully${NC}"
                "$TOOL_DIR/bin/$tool_name" --version || echo "Version check may not be supported"
            elif [ -f "$TOOL_DIR/$tool_name" ]; then
                chmod +x "$TOOL_DIR/$tool_name"
                ln -sf "$TOOL_DIR/$tool_name" "$TOOL_DIR/bin/$tool_name"
                echo -e "${GREEN}✓ $tool_name $version installed successfully${NC}"
            else
                echo -e "${YELLOW}⚠ $tool_name executable not found in expected location${NC}"
            fi
            
            # Cleanup extracted directory
            rm -rf "$extracted_dir"
        else
            echo -e "${RED}✗ Could not find extracted directory${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ Skipping $tool_name $version installation${NC}"
    fi
}

# CellRanger versions
install_10x_tool "cellranger" "8.0.1" "cellranger-8.0.1.tar.gz" "cellranger-v8.0.1"
install_10x_tool "cellranger" "6.1.2" "cellranger-6.1.2.tar.gz" "cellranger-v6.1.2"
install_10x_tool "cellranger" "4.0.0" "cellranger-4.0.0.tar.gz" "cellranger-v4.0.0"

# CellRanger-ARC versions
install_10x_tool "cellranger-arc" "1.0.0" "cellranger-arc-1.0.0.tar.gz" "cellranger-arc"
install_10x_tool "cellranger-arc" "2.0.0" "cellranger-arc-2.0.0.tar.gz" "cellranger-arc-v2"

# CellRanger-ATAC
install_10x_tool "cellranger-atac" "1.2.0" "cellranger-atac-1.2.0.tar.gz" "cellranger-atac"

# SpaceRanger versions
install_10x_tool "spaceranger" "3.0.1" "spaceranger-3.0.1.tar.gz" "spaceranger-v3.0.1"
install_10x_tool "spaceranger" "1.3.1" "spaceranger-1.3.1.tar.gz" "spaceranger-v1.3.1"
install_10x_tool "spaceranger" "1.1.0" "spaceranger-1.1.0.tar.gz" "spaceranger-v1.1.0"

# =============================================================================
# Cleanup
# =============================================================================
echo ""
echo -e "${BLUE}==================================================================="
echo "Cleanup"
echo "===================================================================${NC}"

cd /
rm -rf "$TMP_DIR"
echo -e "${GREEN}✓ Temporary files cleaned up${NC}"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${GREEN}==================================================================="
echo "Installation Summary"
echo "===================================================================${NC}"
echo ""
echo "Installation directory: $INSTALL_BASE_DIR"
echo ""
echo "Installed pipelines:"
echo ""

for pipeline_dir in "$INSTALL_BASE_DIR"/*; do
    if [ -d "$pipeline_dir/bin" ]; then
        pipeline_name=$(basename "$pipeline_dir")
        if [ -n "$(ls -A $pipeline_dir/bin 2>/dev/null)" ]; then
            echo -e "${GREEN}✓${NC} $pipeline_name"
            ls "$pipeline_dir/bin"
        else
            echo -e "${YELLOW}⚠${NC} $pipeline_name (directory exists but no binaries found)"
        fi
    fi
done

echo ""
echo -e "${BLUE}==================================================================="
echo "Next Steps"
echo "===================================================================${NC}"
echo ""
echo "1. Ensure pm2 processes can access the binaries:"
echo "   - Check that $INSTALL_BASE_DIR is accessible"
echo "   - Verify file permissions"
echo ""
echo "2. Update PATH environment variable if needed:"
echo "   export PATH=\"\$PATH:$INSTALL_BASE_DIR/*/bin\""
echo ""
echo "3. Verify installations:"
echo "   $INSTALL_BASE_DIR/bcl2fastq/bin/bcl2fastq --version"
echo "   $INSTALL_BASE_DIR/cellranger-v8.0.1/bin/cellranger --version"
echo ""
echo "4. Configure bioloop workers (ecosystem.config.js) if needed"
echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""


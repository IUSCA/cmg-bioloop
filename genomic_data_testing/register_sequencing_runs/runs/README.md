# Sequencing Run Registration Scripts

This directory contains individual registration scripts for specific sequencing runs used in pipeline testing.

## Available Runs

### iSeq-DI (Dual-Index Demo Run)

**Script:** `register_iseq-DI.sh`  
**Source:** 10x Genomics  
**Size:** ~541 MB  
**Purpose:** bcl2fastq conversion testing

```bash
./register_iseq-DI.sh
```

This script:
- Downloads the iSeq-DI dual-index demo run
- Registers it as RAW_DATA
- Creates conversion documentation at `../conversion_testing/iseq-DI---bcl2fastq.md`

## How These Scripts Work

Each run-specific script:

1. **Downloads** the sequencing run using chunked downloading (prevents connection resets)
2. **Registers** it with the Bioloop system by placing it in `/opt/sca/data/origin/raw_data`
3. **Documents** the recommended conversion parameters in `../conversion_testing/`

## Auto-Generated Documentation

When you run a registration script, it creates a markdown file in the `conversion_testing/` directory with:

- Dataset overview and characteristics
- Recommended conversion tool command (e.g., bcl2fastq)
- Flag justifications
- Required configuration files (e.g., SampleSheet.csv)
- Expected outputs
- Validation checklist
- Troubleshooting tips

## Adding New Runs

To add a new sequencing run:

1. Create a new script: `register_[run-name].sh`
2. Follow the pattern from `register_iseq-DI.sh`
3. Update the `RUN_NAME` and `PIPELINE` variables
4. Create the conversion documentation in the `create_conversion_doc()` function
5. Make the script executable: `chmod +x register_[run-name].sh`

## Usage

All scripts support these options:

```bash
# Download to default location
./register_[run-name].sh

# Download to custom location
./register_[run-name].sh -d /custom/path

# View help
./register_[run-name].sh -h
```

## Notes

- Scripts execute commands inside the celery_worker container via docker-compose
- Downloads use 25MB chunks with retry logic for reliability
- Files are moved atomically to prevent watch.py from detecting partial downloads
- Conversion documentation is generated before downloading starts


# File Info Population Script

This script populates file information for archived datasets that are missing file metadata in the Bioloop system.

## Overview

The script addresses the issue where datasets were archived but their file information was not populated in the database during the original `Integrated` workflow execution. It creates a new workflow called `file_info_population` that:

1. Downloads archived datasets from SDA to a local directory
2. Runs the file info population workflow with steps: inspect, archive, stage, validate, delete_source
3. Processes datasets in configurable batches
4. Tracks progress and supports resumption after interruption
5. Monitors disk space to prevent exceeding configured limits

## New Workflow: `file_info_population`

The new workflow includes these steps:
- **inspect**: Generates file metadata for the dataset
- **archive**: Creates tar bundle (with optional SDA upload via config)
- **stage**: Extracts and stages the dataset
- **validate**: Validates file checksums
- **delete_source**: Cleans up the downloaded source files

## Configuration

Add to `workers/config/common.py`:

```python
'file_info_population': {
    'batch_size': 10,                    # Datasets per batch
    'max_download_size_tb': 10,          # Max download directory size in TB
    'download_dir': '/opt/sca/data/file_info_downloads',
    'state_file': '/opt/sca/data/file_info_population_state.json',
    'skip_sda_upload': True,             # Skip SDA upload in archive step
    'poll_interval_seconds': 300,        # Check interval for batch completion
    'max_retries_per_dataset': 3         # Max retries per dataset
}
```

## Usage

### Basic Usage
```bash
# Run with default configuration
python -m workers.scripts.populate_file_info

# Dry run to see what would be processed
python -m workers.scripts.populate_file_info --dry-run
```

### Advanced Usage
```bash
# Custom batch size and storage limit
python -m workers.scripts.populate_file_info --batch-size 5 --max-size-tb 5

# Resume from last incomplete batch
python -m workers.scripts.populate_file_info --resume

# Force restart from beginning
python -m workers.scripts.populate_file_info --force-restart

# Custom download directory
python -m workers.scripts.populate_file_info --download-dir /custom/path

# Debug logging
python -m workers.scripts.populate_file_info --log-level DEBUG
```

## Command Line Options

- `--batch-size`: Number of datasets to process per batch (default: from config)
- `--max-size-tb`: Maximum download directory size in TB (default: from config)
- `--download-dir`: Directory for downloading datasets (default: from config)
- `--dry-run`: Simulate the process without making changes
- `--resume`: Resume from the last incomplete batch
- `--force-restart`: Start from the beginning, ignoring previous state
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## How It Works

### Dataset Selection
The script identifies datasets that need file info population by:
1. Finding all archived datasets (those with `archive_path`)
2. Checking if they have file metadata (empty or missing `files` array)
3. Excluding datasets already processed or currently being processed

### Batch Processing
1. Downloads datasets from SDA in batches
2. Extracts tar files to local directory
3. Updates dataset `origin_path` to point to extracted location
4. Starts `file_info_population` workflow for each dataset
5. Waits for batch completion before processing next batch
6. Cleans up downloaded files after successful processing

### State Management
- Progress is saved to a JSON state file
- Supports resumption after interruption
- Tracks processed, failed, and currently processing datasets
- Maintains batch information for recovery

### Space Management
- Monitors download directory size
- Prevents downloading if it would exceed configured limit
- Cleans up files after successful processing
- Skips datasets that are too large

### Error Handling
- Retries failed downloads with configurable limits
- Validates checksums after download
- Handles workflow failures gracefully
- Logs detailed error information
- Continues processing other datasets if one fails

## Monitoring

The script logs detailed information about:
- Datasets being processed
- Download progress and completion
- Workflow status and completion
- Errors and failures
- Final summary statistics

Log files are created in `/tmp/` with timestamp for each run.

## Recovery

If the script is interrupted:
1. Use `--resume` to continue from the last incomplete batch
2. The state file tracks all progress automatically
3. Already processed datasets are skipped
4. Failed datasets can be retried by removing them from the state file

## Example Run

```bash
$ python -m workers.scripts.populate_file_info --batch-size 5 --dry-run

2024-01-15 10:00:00 - INFO - Starting File Info Population process
2024-01-15 10:00:00 - INFO - Configuration: batch_size=5, max_size_tb=10.0, download_dir=/opt/sca/data/file_info_downloads
2024-01-15 10:00:00 - INFO - DRY RUN MODE - No actual changes will be made
2024-01-15 10:00:01 - INFO - Fetching archived datasets that need file info population...
2024-01-15 10:00:02 - INFO - Found 1500 archived datasets
2024-01-15 10:00:03 - INFO - Found 1200 datasets needing file info population
2024-01-15 10:00:03 - INFO - Starting batch 0 with 5 datasets
2024-01-15 10:00:03 - INFO - Remaining datasets after this batch: 1195
...
2024-01-15 10:05:00 - INFO - File Info Population process completed!
2024-01-15 10:05:00 - INFO - Summary:
2024-01-15 10:05:00 - INFO -   Total datasets found: 1200
2024-01-15 10:05:00 - INFO -   Successfully processed: 1180
2024-01-15 10:05:00 - INFO -   Failed: 20
```

## Notes

- The script is designed to run for several days given the large number of datasets (~2000)
- It's idempotent - can be safely restarted
- Uses existing worker infrastructure and API endpoints
- Respects the same configuration patterns as other worker scripts
- Includes comprehensive error handling and logging

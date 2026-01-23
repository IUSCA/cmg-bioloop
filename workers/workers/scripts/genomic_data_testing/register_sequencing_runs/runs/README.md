# Sequencing Run Registration Scripts

This directory contains individual Python scripts for downloading and registering specific sequencing runs as RAW_DATA.

## Available Scripts

### register_iseq_di.py

Downloads and registers the iSeq-DI dual-index demo run from 10x Genomics.

**Dataset:** iseq-DI  
**Source:** 10x Genomics spatial expression demultiplexing test data  
**Size:** ~541 MB  
**Type:** Illumina iSeq dual-index run folder  
**Purpose:** bcl2fastq conversion testing

**Usage:**
```bash
cd /opt/sca/cmg-bioloop/workers
poetry shell
python -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di
```

**Auto-generated Documentation:**
- Creates `../conversion_testing/iseq-DI---bcl2fastq.md`
- Includes recommended bcl2fastq flags, SampleSheet, and validation steps

## Adding New Sequencing Runs

To add a new sequencing run script:

1. Create a new Python script in this directory (e.g., `register_myrun.py`)
2. Import and use the `register_dataset` function from `utils.py`
3. Set `dataset_type='RAW_DATA'`
4. Provide conversion documentation content
5. Specify appropriate chunk size and headers for the download source

**Example template:**

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from workers.scripts.genomic_data_testing.utils import register_dataset

def main():
    register_dataset(
        url="https://example.com/myrun.tar.gz",
        filename="myrun.tar.gz",
        dataset_name="myrun",
        dataset_type='RAW_DATA',
        chunk_size=25 * 1024 * 1024,
        headers=None,
        should_extract=True,
        doc_content="# Your documentation here",
        doc_filename="myrun---pipeline.md"
    )

if __name__ == '__main__':
    main()
```

## Notes

- All scripts automatically check for existing dataset names
- Numeric suffixes (---1, ---2, etc.) are appended automatically if name conflicts exist
- Documentation is auto-generated in `../conversion_testing/`
- Scripts use chunked downloads with retry logic for reliability


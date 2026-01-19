# Sequencing Run Test Dataset Information

This document provides detailed information about the sequencing run test datasets available for testing the Bioloop data processing pipeline.

## Directory Structure

```
register_sequencing_runs/
├── runs/                           # Individual run registration scripts
│   └── register_iseq-DI.sh        # iSeq dual-index demo run
├── conversion_testing/             # Auto-generated conversion documentation
│   └── iseq-DI---bcl2fastq.md     # Created by register_iseq-DI.sh
└── dataset_info.md                # This file
```

## Using Run-Specific Scripts

Each sequencing run has its own registration script in the `runs/` directory. When you run a registration script:

1. It downloads the sequencing run data
2. Registers it as RAW_DATA with the watch.py script
3. Automatically generates conversion testing documentation in `conversion_testing/`

The conversion documentation includes recommended bcl2fastq flags, SampleSheets, and validation steps.

## Available Dataset

### iseq-DI - 10x Genomics Spatial Expression Demultiplexing Data

**File Type:** Compressed archive (tar.gz)  
**Description:** iSeq sequencing run data for spatial expression demultiplexing testing  
**Source:** 10x Genomics support data for spatial expression workflows  
**Dataset Type:** RAW_DATA (sequencing run)

#### Dataset Information

This dataset contains sequencing run output from a 10x Genomics spatial expression experiment. It's designed for testing demultiplexing and data processing workflows.

- **Platform:** iSeq (Illumina)
- **Application:** Spatial Expression
- **Purpose:** Demultiplexing workflow testing
- **Official Source:** https://www.10xgenomics.com/

#### Download Details

The download is configured with robust chunked downloading and retry capabilities:

- **Chunk Size:** 25 MB per chunk
- **Retries:** Up to 50 attempts per chunk with 2-second delays
- **Resume Support:** Automatically resumes from last successful chunk
- **User Agent:** Required for 10x Genomics server compatibility
- **Referer Header:** Required for proper authentication
- **Chunked Approach:** Prevents connection reset issues on large files

#### Manual Download Command (Chunked)

```bash
URL="https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz"
OUT="iseq-DI.tar.gz"
SIZE=566891317

# Current size in bytes (for resume)
CUR=$(stat -c%s "$OUT" 2>/dev/null || echo 0)
echo "Have $CUR / $SIZE bytes"

CHUNK=$((25*1024*1024))   # 25 MiB chunks
start=$CUR

while [ "$start" -lt "$SIZE" ]; do
  end=$((start + CHUNK - 1))
  if [ "$end" -ge "$SIZE" ]; then end=$((SIZE - 1)); fi
  echo "Fetching $start-$end"
  curl -L --fail --retry 50 --retry-delay 2 --retry-all-errors \
    -H 'User-Agent: Mozilla/5.0' \
    -H 'Referer: https://www.10xgenomics.com/' \
    -H "Range: bytes=$start-$end" \
    "$URL" >> "$OUT" || exit 1
  start=$((end + 1))
done
```

---

## Using the Registration Script

The run-specific scripts in the `runs/` directory will automatically download datasets and register them with the Bioloop system as RAW_DATA type datasets.

### Quick Start

```bash
# Download iSeq-DI run to default location (/opt/sca/data/origin/raw_data)
cd runs
./register_iseq-DI.sh

# Download to custom location
./register_iseq-DI.sh -d /opt/sca/data/origin/raw_data

# View help
./register_iseq-DI.sh -h
```

### Conversion Testing Documentation

When you run a registration script, it automatically creates detailed conversion documentation in the `conversion_testing/` directory. For example, `register_iseq-DI.sh` creates `conversion_testing/iseq-DI---bcl2fastq.md` with:

- Recommended bcl2fastq command and flags
- Required SampleSheet configuration
- Expected output structure
- Validation checklist
- Troubleshooting guide

### What Happens During Registration

1. **Download:** File is downloaded to `/tmp` inside the celery_worker container with retry logic
2. **Directory Creation:** A directory named `iseq-DI` is created in the destination
3. **Atomic Move:** The downloaded file is moved into the directory immediately
4. **Watch Detection:** The watch.py script detects the new directory as a RAW_DATA dataset
5. **Workflow Trigger:** The integrated workflow is automatically triggered for processing

---

## Expected Processing Workflow

Once registered, this dataset will trigger the RAW_DATA integrated workflow, which typically includes:

1. **Dataset Registration:** Create database entry for the dataset
2. **File Validation:** Verify file integrity and format
3. **Metadata Extraction:** Extract sequencing run metadata
4. **Staging:** Move/link files to staging area
5. **Quality Control:** Run QC checks (if configured)
6. **Conversion:** Process data through configured conversion pipelines (if applicable)

---

## Verification After Registration

After the script completes, you can verify the dataset was registered:

```bash
# Check the directory was created
docker-compose exec celery_worker ls -la /opt/sca/data/origin/raw_data/iseq-DI

# Check the file exists
docker-compose exec celery_worker find /opt/sca/data/origin/raw_data/iseq-DI -type f

# Check watch.py logs for registration events
docker-compose logs watch | grep iseq-DI

# Check API for dataset entry
curl http://localhost:3000/api/datasets | jq '.datasets[] | select(.name=="iseq-DI")'
```

---

## Notes

- The download includes resume capability, so interrupted downloads can be continued
- The script uses atomic file operations to ensure the watch.py script doesn't detect partially downloaded files
- This dataset is registered as RAW_DATA type and will trigger the full integrated workflow
- The dataset will be placed in its own directory (`iseq-DI/`) as required by the watch.py script

---

## Data Source

**10x Genomics Support Data:**  
https://www.10xgenomics.com/support/software/space-ranger/downloads

This is publicly available test data provided by 10x Genomics for testing spatial expression workflows and demultiplexing procedures.


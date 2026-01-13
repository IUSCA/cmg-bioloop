# Genomic Data Testing

This directory contains scripts and documentation for registering test datasets with the Bioloop system for development and testing purposes.

## Directory Structure

```
genomic_data_testing/
├── register_sequencing_runs/
│   ├── register_sequencing_runs.sh          # Download 10x Genomics sequencing run
│   └── dataset_info.md                      # Documentation for sequencing run dataset
└── register_data_products_suitable_for_genome_browser/
    ├── register_data_products_suitable_for_genome_browser.sh  # Download genome browser tracks
    └── data_products_info.md                # Documentation for genome browser datasets
```

## Available Test Datasets

### 1. Sequencing Runs (RAW_DATA)

**Location:** `register_sequencing_runs/`  
**Dataset:** 10x Genomics iSeq spatial expression data (iseq-DI.tar.gz)  
**Registration Type:** RAW_DATA

This dataset is used for testing the full data processing pipeline, including:
- Dataset registration and ingestion
- File validation and staging
- Workflow execution
- Quality control (if configured)
- Data conversion pipelines

```bash
cd register_sequencing_runs
./register_sequencing_runs.sh
```

### 2. Genome Browser Data Products (DATA_PRODUCT)

**Location:** `register_data_products_suitable_for_genome_browser/`  
**Datasets:** 3 genome browser compatible files (bigBed, bigWig, methylation data)  
**Registration Type:** DATA_PRODUCT

These datasets are used for testing genome browser integration:
- bigBed1 - Peaks/Features track
- h1.liftedtohg19.gz - Methylation data
- GSM429321.bigWig - H3K4me3 signal track

All mapped to hg19 genome assembly.

```bash
cd register_data_products_suitable_for_genome_browser
./register_data_products_suitable_for_genome_browser.sh
```

## Usage

### Prerequisites

- Docker and docker-compose must be installed
- The celery_worker service must be running: `docker-compose up -d`
- The watch.py script should be monitoring the appropriate directories

### General Workflow

1. Navigate to the appropriate test dataset directory
2. Run the registration script
3. The script will:
   - Download the dataset(s) to `/tmp` inside the celery_worker container
   - Create a directory for each dataset in the appropriate origin location
   - Move the downloaded file(s) atomically to avoid race conditions
   - The watch.py script will detect the new directory and trigger registration

### Monitoring Registration

After running a registration script, you can monitor the process:

```bash
# Watch the watch.py script logs
docker-compose logs -f watch

# Check celery worker logs
docker-compose logs -f celery_worker

# Query the API for registered datasets
curl http://localhost:3000/api/datasets | jq
```

## Data Origins

The registration scripts place datasets in the following locations by default:

- **RAW_DATA:** `/opt/sca/data/origin/raw_data/`
- **DATA_PRODUCT:** `/opt/sca/data/origin/data_products/`

These locations can be overridden using the `-d` or `--destination` option on each script.

## Customization

Both scripts support command-line options for customization:

```bash
# View help for any script
./register_sequencing_runs.sh -h
./register_data_products_suitable_for_genome_browser.sh -h

# Common options
-d, --destination DIR    # Override destination directory
-n, --number NUM         # Download only N datasets (data products only)
-h, --help              # Show help message
```

## Notes

- Scripts use atomic file operations to prevent the watch.py script from detecting partial downloads
- Downloads include retry logic and resume capabilities for reliability
- Each dataset is placed in its own directory as required by the watch.py script
- Registration happens automatically when the watch.py script detects the new directories

## Documentation

For detailed information about each dataset, including testable genomic ranges and data sources, see:

- `register_sequencing_runs/dataset_info.md` - Sequencing run details
- `register_data_products_suitable_for_genome_browser/data_products_info.md` - Genome browser data details



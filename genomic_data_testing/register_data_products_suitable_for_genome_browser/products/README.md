# Genome Browser Data Product Registration Scripts

This directory contains individual registration scripts for specific genome browser test data products.

## Using Product-Specific Scripts

Each data product has its own registration script in this directory. When you run a registration script:

1. It creates detailed documentation in `../product_docs/[productName].md`
2. Downloads the genome browser file
3. Names it with the proper extension for automatic track creation
4. Registers it as DATA_PRODUCT with the watch.py script

The product documentation includes testing ranges, usage examples, and verification steps.

## Available Data Products

### bigBed_test - Peaks/Features Track

**Script:** `register_bigBed.sh`  
**Source:** WashU Epigenome Browser Hub  
**Size:** ~805 KB  
**Purpose:** Testing BigBed format track loading

```bash
./register_bigBed.sh
```

Creates documentation at: `../product_docs/bigBed_test.md`

### methylation_h1_hg19 - MethylC-seq Track

**Script:** `register_methylation.sh`  
**Source:** WashU Public Data  
**Size:** ~few MB  
**Purpose:** Testing methylation bedGraph track loading

```bash
./register_methylation.sh
```

Creates documentation at: `../product_docs/methylation_h1_hg19.md`

### bigWig_h3k4me3_hg19 - H3K4me3 ChIP-seq Signal

**Script:** `register_bigWig.sh`  
**Source:** WashU Epigenome Browser Hub  
**Size:** ~several MB  
**Purpose:** Testing BigWig signal track loading

```bash
./register_bigWig.sh
```

Creates documentation at: `../product_docs/bigWig_h3k4me3_hg19.md`

## File Naming and Track Auto-Creation

Files must have proper extensions for automatic track creation:

| Extension | Format | Track Created? |
|-----------|--------|----------------|
| `.bigBed` or `.bb` | BigBed | ✅ Yes |
| `.bigWig` or `.bw` | BigWig | ✅ Yes |
| `.bam` | BAM alignment | ✅ Yes |
| `.vcf.gz` | VCF variants | ✅ Yes |
| `.bed.gz` | Compressed BED | ✅ Yes |
| *(no extension)* | Unknown | ❌ No |

**This is why `bigBed1` is renamed to `bigBed_test.bigBed`!**

## Usage

All scripts support these options:

```bash
# Download to default location
./register_[product].sh

# Download to custom location
./register_[product].sh -d /custom/path

# View help
./register_[product].sh -h
```

## Auto-Generated Documentation

Each script creates detailed product documentation in `../product_docs/`:

- Testing ranges (hg19 coordinates)
- Usage examples
- Verification steps
- Troubleshooting tips
- Browser compatibility details

## Testing Workflow

1. Run a registration script
2. Read the auto-generated documentation
3. Wait for watch.py to detect and register
4. Verify track was auto-created
5. Create a genome browser session
6. Navigate to test ranges from documentation
7. Verify data displays correctly

## Notes

- Scripts execute commands inside the celery_worker container via docker-compose
- Downloads use 10MB chunks with retry logic for reliability
- Files are moved atomically to prevent watch.py from detecting partial downloads
- Proper file extensions enable automatic track creation
- All datasets are compatible with both IGV and WashU Epigenome Browser


# Genome Browser Data Product Test Dataset Information

This document provides an overview of the genome browser test datasets available for testing track loading and visualization.

## Directory Structure

```
register_data_products_suitable_for_genome_browser/
├── products/                       # Individual product registration scripts
│   ├── register_bigBed.sh         # Creates ../product_docs/bigBed_test.md
│   ├── register_methylation.sh    # Creates ../product_docs/methylation_h1_hg19.md
│   └── register_bigWig.sh         # Creates ../product_docs/bigWig_h3k4me3_hg19.md
├── product_docs/                   # Auto-generated product documentation
│   ├── bigBed_test.md             # Created by register_bigBed.sh
│   ├── methylation_h1_hg19.md     # Created by register_methylation.sh
│   └── bigWig_h3k4me3_hg19.md     # Created by register_bigWig.sh
└── data_products_info.md          # This file
```

## Using Product-Specific Scripts

Each data product has its own registration script in the `products/` directory. When you run a registration script:

1. It creates detailed documentation in `product_docs/[productName].md`
2. Downloads the genome browser file with proper extension
3. Registers it as DATA_PRODUCT with the watch.py script
4. Enables automatic track creation via file extension detection

The product documentation includes recommended testing ranges, usage examples, and verification steps.

## Available Datasets

All datasets are mapped to the **hg19** genome assembly.

### bigBed_test - Peaks/Features Track

**File Type:** BigBed  
**Description:** Feature file containing genomic peaks or annotations  
**Size:** ~805 KB  
**Dataset Type:** DATA_PRODUCT

**Important:** The file is downloaded and renamed to `bigBed_test.bigBed` (with proper `.bigBed` extension) to enable automatic track creation.

#### Download

```bash
cd products
./register_bigBed.sh
```

#### Documentation

Detailed product documentation created at: `product_docs/bigBed_test.md`

---

### methylation_h1_hg19 - MethylC-seq Track

**File Type:** Methylation bedGraph (gzipped)  
**Description:** Methylation data from H1 cell line, lifted over to hg19  
**Size:** ~few MB  
**Dataset Type:** DATA_PRODUCT

#### Download

```bash
cd products
./register_methylation.sh
```

#### Documentation

Detailed product documentation created at: `product_docs/methylation_h1_hg19.md`

---

### bigWig_h3k4me3_hg19 - H3K4me3 ChIP-seq Signal

**File Type:** BigWig  
**Description:** H3K4me3 ChIP-seq signal track (histone modification)  
**Size:** ~several MB  
**Dataset Type:** DATA_PRODUCT

#### Download

```bash
cd products
./register_bigWig.sh
```

#### Documentation

Detailed product documentation created at: `product_docs/bigWig_h3k4me3_hg19.md`

---

## Using the Wrapper Script

You can also use the wrapper script to download multiple products:

```bash
# Download all 3 datasets
./register_data_products.sh

# Download only the first dataset (bigBed - smallest)
./register_data_products.sh -n 1

# Download to custom location
./register_data_products.sh -d /opt/sca/data/origin/data_products
```

## Critical: File Naming and Track Auto-Creation

**Files MUST have proper extensions for automatic track creation!**

The automatic track creation system relies on file extensions:

- ✅ `bigBed_test.bigBed` → Format: `BIGBED` → Role: `PRIMARY` → **Track created**
- ✅ `GSM429321.bigWig` → Format: `BIGWIG` → Role: `PRIMARY` → **Track created**
- ❌ `bigBed1` (no extension) → Format: `null` → Role: `null` → **NO track**

### Supported Extensions

- `.bigBed` or `.bb` → BigBed tracks
- `.bigWig` or `.bw` → BigWig tracks
- `.bam` → BAM alignment tracks
- `.cram` → CRAM alignment tracks
- `.vcf.gz` → VCF variant tracks
- `.bed.gz` → Compressed BED tracks
- `fragments.tsv.gz` → 10x ATAC fragments tracks

---

## Expected Processing Workflow

Once registered, these datasets will trigger the DATA_PRODUCT integrated workflow:

1. **Dataset Registration:** Create database entry
2. **File Metadata Extraction:** Extract file information
3. **Format Detection:** Determine file type from extension
4. **Role Assignment:** Mark as `PRIMARY` if track-capable format
5. **Track Auto-Creation:** Create track entry for PRIMARY files
6. **Staging:** Move files to staging area
7. **Validation:** Verify file integrity

---

## Verification After Registration

After a script completes, verify the dataset and track:

```bash
# Check dataset was created
docker-compose exec celery_worker ls -la /opt/sca/data/origin/data_products/[product_name]

# Check via API
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="[product_name]")'

# Check track was auto-created
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("[filename]"))'

# Check watch.py logs
docker-compose logs watch | grep [product_name]
```

---

## Notes

- All datasets are compatible with both IGV and WashU Epigenome Browser
- Files are placed in individual directories for automatic detection by watch.py
- Downloads use chunked approach (10MB chunks) with retry logic
- Files are moved atomically to prevent partial download detection
- Datasets are registered as DATA_PRODUCT type
- Tracks are automatically created for files with proper extensions

---

## Data Sources

- **WashU Epigenome Browser Hub Samples:** https://vizhub.wustl.edu/
- **WashU Public Data:** https://egg.wustl.edu/ and https://vizhub.wustl.edu/public/

These are publicly available test datasets provided by the WashU Epigenome Browser team for testing and demonstration purposes.


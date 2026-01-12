# Genome Browser Test Dataset Information

This document provides detailed information about the test datasets available for genome browser testing, including genomic coordinate ranges that can be used for verification and the download URLs.

## Available Datasets

All datasets are mapped to the **hg19** genome assembly.

---

### 1. bigBed1 - Peaks/Features Track

**File Type:** bigBed  
**Description:** Feature file containing genomic peaks or annotations  
**Approximate Size:** ~few KB

#### Testable Genomic Ranges (hg19)

Use these coordinates to verify the data appears correctly in the genome browser:

- `chr12:6643000-6648500`
- `chr7:5566000-5571000`
- `chr8:128700000-128900000`

#### Download Commands

```bash
# Full download
curl -L -O https://vizhub.wustl.edu/hubSample/hg19/bigBed1

# Inspect file header (first 64KB)
curl -sSIL -H "Range: bytes=0-65535" https://vizhub.wustl.edu/hubSample/hg19/bigBed1
```

---

### 2. h1.liftedtohg19.gz - MethylC-seq Track

**File Type:** Methylation bedGraph (gzipped)  
**Description:** Methylation data from H1 cell line, lifted over to hg19  
**Approximate Size:** ~few MB

#### Testable Genomic Ranges (hg19)

Use these coordinates to verify methylation data:

- `chr11:1950000-2120000`
- `chr19:58430000-58600000`
- `chr6:32500000-33000000`

#### Download Commands

```bash
# Full download
curl -L -O https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz

# Inspect file header (first 64KB)
curl -sSIL -H "Range: bytes=0-65535" https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz
```

---

### 3. GSM429321.bigWig - H3K4me3 Signal Track

**File Type:** bigWig  
**Description:** H3K4me3 ChIP-seq signal track (histone modification associated with active transcription)  
**Approximate Size:** ~several MB to larger

#### Testable Genomic Ranges (hg19)

Use these coordinates to verify signal tracks:

- `chr12:6643000-6648500`
- `chr7:5566000-5571000`
- `chr8:128748000-128756000`

#### Download Commands

```bash
# Full download
curl -L -O https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig

# Inspect file header (first 64KB)
curl -sSIL -H "Range: bytes=0-65535" https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig
```

---

## Using the Registration Script

The `register_genome_browser_suitable_data_products.sh` script in this directory will automatically download these datasets and register them with the Bioloop system as DATA_PRODUCT type datasets.

### Quick Start

```bash
# Download all 3 datasets
./register_genome_browser_suitable_data_products.sh

# Download only the smallest dataset (bigBed1)
./register_genome_browser_suitable_data_products.sh -n 1

# Download to custom location
./register_genome_browser_suitable_data_products.sh -d /opt/sca/data/origin/data_products
```

### Verification After Registration

Once datasets are registered and processed, you can test them in the genome browser by:

1. Creating a new genome browser session
2. Adding the tracks from these datasets
3. Navigating to the testable ranges listed above
4. Verifying that data appears correctly in the specified regions

---

## Notes

- All datasets are compatible with both IGV and WashU Epigenome Browser
- Files are downloaded and placed in individual directories for automatic detection by the watch.py script
- The registration script uses atomic file operations to ensure data integrity
- Downloads use chunked approach (10MB chunks) with retry logic to prevent connection reset issues
- Datasets are registered as DATA_PRODUCT type

---

## Data Sources

- **WashU Epigenome Browser Hub Samples:** https://vizhub.wustl.edu/
- **WashU Public Data:** https://egg.wustl.edu/ and https://vizhub.wustl.edu/public/

These are publicly available test datasets provided by the WashU Epigenome Browser team for testing and demonstration purposes.



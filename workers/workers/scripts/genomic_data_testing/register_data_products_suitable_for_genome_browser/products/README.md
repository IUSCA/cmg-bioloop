# Data Product Registration Scripts

This directory contains Python scripts for downloading and registering genome browser-compatible data products as DATA_PRODUCT.

## Available Scripts

### register_bigwig.py

Downloads H3K4me3 ChIP-seq signal track from WashU Epigenome Browser.

**Dataset:** bigWig_h3k4me3_hg19  
**File:** GSM429321.bigWig  
**Format:** BigWig  
**Genome:** hg19  
**Purpose:** Continuous signal visualization testing

**Usage:**
```bash
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigwig
```

---

### register_bigbed.py

Downloads BigBed peaks/features file from WashU Epigenome Browser.

**Dataset:** bigBed_test  
**File:** bigBed_test.bigBed (renamed from bigBed1)  
**Format:** BigBed  
**Genome:** hg19  
**Purpose:** Feature/peaks visualization testing

**Usage:**
```bash
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigbed
```

---

### register_methylation.py

Downloads H1 cell line MethylC-seq data from WashU Public Data.

**Dataset:** methylation_h1_hg19  
**File:** h1.liftedtohg19.gz  
**Format:** Methylation bedGraph (gzipped)  
**Genome:** hg19  
**Purpose:** Methylation visualization testing

**Usage:**
```bash
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_methylation
```

---

### register_gsm429321_h3k27ac.py

Downloads H3K27ac ChIP-seq signal track from WashU Epigenome Browser.

**Dataset:** bigWig_GSM429321_H3K27ac_hg19  
**File:** GSM429321_H3K27ac.bigWig  
**Format:** BigWig  
**Genome:** hg19  
**Purpose:** Enhancer/promoter visualization testing

**Usage:**
```bash
python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_gsm429321_h3k27ac
```

## Auto-generated Documentation

All scripts create detailed documentation in `../product_docs/` including:
- Dataset overview and specifications
- Testing genomic coordinates (hg19)
- Genome browser compatibility (IGV, WashU)
- Verification steps
- Data interpretation guidelines

## File Extensions for Auto-track Creation

For automatic track creation, files must have proper extensions:
- `.bigBed` or `.bb` → BIGBED format
- `.bigWig` or `.bw` → BIGWIG format
- `.bam` → BAM format
- `.vcf.gz` → VCF format
- `.bed.gz` → BED format
- `fragments.tsv.gz` → Fragments format

Files without proper extensions will **not** create tracks automatically.

## Adding New Data Products

To add a new data product script:

1. Create a new Python script in this directory
2. Import and use the `register_dataset` function from `utils.py`
3. Set `dataset_type='DATA_PRODUCT'`
4. Set `should_extract=False` for non-archived files
5. Provide product documentation content
6. Include testing ranges for the genome assembly

**Example template:**

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from workers.scripts.genomic_data_testing.utils import register_dataset

def main():
    register_dataset(
        url="https://example.com/mytrack.bigWig",
        filename="mytrack.bigWig",
        dataset_name="mytrack_hg19",
        dataset_type='DATA_PRODUCT',
        chunk_size=10 * 1024 * 1024,
        headers=None,
        should_extract=False,
        doc_content="# Your documentation here",
        doc_filename="mytrack_hg19.md"
    )

if __name__ == '__main__':
    main()
```

## Notes

- All DATA_PRODUCT datasets are registered for genome browser integration
- Scripts automatically check for duplicate dataset names
- Numeric suffixes are appended if conflicts exist
- Documentation includes testing ranges specific to each genome assembly
- All public data sources are from WashU Epigenome Browser or similar repositories


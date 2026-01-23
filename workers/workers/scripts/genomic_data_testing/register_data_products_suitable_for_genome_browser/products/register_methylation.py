#!/usr/bin/env python3
"""
Methylation Track Registration Script

Purpose:
    Downloads H1 cell line MethylC-seq data from WashU Public Data and
    registers it as DATA_PRODUCT for genome browser testing.

Usage:
    python register_methylation.py [--help]
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from workers.scripts.genomic_data_testing.utils import register_dataset

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


PRODUCT_DOC = '''# methylation_h1_hg19 - MethylC-seq Track

## Dataset Overview

**File:** `h1.liftedtohg19.gz`  
**Source:** https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz  
**Size:** ~few MB  
**Format:** Methylation bedGraph (gzipped)  
**Genome:** hg19  
**Cell Line:** H1 (human embryonic stem cells)

### What This Dataset Is

✅ **Real methylation data from MethylC-seq experiments**

✅ **H1 cell line data lifted over to hg19 coordinates**

✅ **Suitable for testing methylation visualization**

### Purpose

This dataset is used to test:
- Methylation bedGraph file format support
- Compressed file handling (.gz)
- Methylation signal visualization
- Epigenetic data display in genome browsers

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify methylation data:

### Test Region 1
```
chr11:1950000-2120000
```
- **Chromosome:** 11
- **Region Size:** ~170 KB
- **Expected:** Methylation signal patterns visible

### Test Region 2
```
chr19:58430000-58600000
```
- **Chromosome:** 19
- **Region Size:** ~170 KB
- **Expected:** Variable methylation levels

### Test Region 3
```
chr6:32500000-33000000
```
- **Chromosome:** 6
- **Region Size:** ~500 KB
- **Expected:** Broader methylation landscape

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in the origin directory.

### 2. File Format

The file is a gzipped bedGraph with methylation levels:
- Compressed with gzip
- BedGraph format: chrom, start, end, value
- Values represent methylation levels

### 3. Creating a Genome Browser Session

Once registered:
1. Navigate to `/sessions/new`
2. Select the methylation track
3. Set genome to `hg19`
4. Create the session
5. View methylation patterns in the browser

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="methylation_h1_hg19")'
```

### 2. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr11:1950000-2120000`
3. Verify methylation signal is visible
4. Compare patterns across different regions

---

## Data Source

**WashU Public Data - MethylC-seq**  
https://vizhub.wustl.edu/public/

Publicly available methylation data from H1 embryonic stem cells.
'''


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download and register methylation track for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.parse_args()
    
    print()
    print("=" * 50)
    print("Methylation Track Registration")
    print("=" * 50)
    print()
    
    url = "https://vizhub.wustl.edu/public/hg19/methylc2/h1.liftedtohg19.gz"
    filename = "h1.liftedtohg19.gz"
    dataset_name = "methylation_h1_hg19"
    
    try:
        final_path, final_name = register_dataset(
            url=url,
            filename=filename,
            dataset_name=dataset_name,
            dataset_type='DATA_PRODUCT',
            chunk_size=10 * 1024 * 1024,
            headers=None,
            should_extract=False,
            doc_content=PRODUCT_DOC,
            doc_filename=f"{dataset_name}.md"
        )
        
        print()
        print("=" * 50)
        print("Registration Complete!")
        print("=" * 50)
        print()
        print(f"Downloaded dataset is in directory at:")
        print(f"  {final_path}")
        print()
        print(f"Dataset name: {final_name}")
        print()
        print(f"Product documentation created at:")
        doc_dir = Path(__file__).parent.parent / 'product_docs'
        print(f"  {doc_dir / f'{dataset_name}.md'}")
        print()
        print("Testing ranges (hg19):")
        print("  - chr11:1950000-2120000")
        print("  - chr19:58430000-58600000")
        print("  - chr6:32500000-33000000")
        
    except Exception as e:
        logger.error(f"Failed to register dataset: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


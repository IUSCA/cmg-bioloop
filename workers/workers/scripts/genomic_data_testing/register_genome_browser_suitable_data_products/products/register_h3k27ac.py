#!/usr/bin/env python3
"""
H3K27ac BigWig Signal Track Registration Script

Purpose:
    Downloads H3K27ac ChIP-seq signal track from WashU Epigenome Browser and
    registers it as DATA_PRODUCT for genome browser testing.

Usage:
    python register_h3k27ac.py [--help]

Note:
    - This script adapts to APP_ENV in workers/.env
    - Creates detailed documentation in ../product_docs/
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


PRODUCT_DOC = '''# bigWig_h3k27ac_hg19 - H3K27ac ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321_H3K27ac.bigWig`  
**Source:** https://egg.wustl.edu/d/hg19/GSM429321_H3K27ac.bigWig  
**Size:** Variable (MB range)  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K27ac)

### What This Dataset Is

✅ **Real H3K27ac ChIP-seq signal data**

✅ **Histone modification marking active enhancers and promoters**

✅ **Suitable for testing continuous signal visualization**

✅ **Complementary to H3K4me3 for regulatory element mapping**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Enhancer and promoter identification
- Multi-track epigenomic analysis (when combined with H3K4me3)

---

## H3K27ac Background

**H3K27ac** (Histone H3 Lysine 27 Acetylation):

### Function & Significance
- **Primary Role:** Marks active enhancers and promoters
- **Cellular Process:** Associated with transcriptional activation
- **Genomic Distribution:** Found at both proximal and distal regulatory elements
- **Biological Importance:** Key marker for identifying active regulatory regions

### Interpretation Guidelines
- **High signal at promoters:** Active gene transcription
- **High signal distal to genes:** Active enhancers
- **Broad peaks:** Extended regulatory domains
- **Sharp peaks:** Well-defined regulatory elements

### Comparison with H3K4me3
| Feature | H3K27ac | H3K4me3 |
|---------|---------|---------|
| Primary Location | Enhancers + Promoters | Promoters only |
| Peak Width | Broader | Narrower/Sharp |
| Regulatory Role | Activation mark | TSS marker |
| Genomic Coverage | Wider distribution | TSS-focused |

**Combined Analysis:** Using both H3K27ac and H3K4me3 together allows distinction between:
- **Active promoters:** High H3K27ac + High H3K4me3
- **Active enhancers:** High H3K27ac + Low/No H3K4me3
- **Poised promoters:** Low H3K27ac + High H3K4me3

---

## Testing Recommendations

### Suggested Genomic Regions

While this file may have data across the genome, good testing regions typically include:

#### Known Enhancer-Rich Regions
```
chr8:128700000-128900000
```
- **Region Type:** Super-enhancer region
- **Expected Pattern:** Broad H3K27ac domains
- **Cell Type Specific:** May vary by sample

#### Active Gene Promoters
```
chr12:6640000-6650000
```
- **Region Type:** Gene promoter regions
- **Expected Pattern:** Sharp H3K27ac peaks at TSS
- **Compare with:** H3K4me3 signal (should overlap)

#### Random Sampling for Validation
```
chr7:5500000-5600000
```
- **Region Type:** General genomic region
- **Expected Pattern:** Mix of peaks and background
- **Use For:** Overall data quality assessment

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in the origin directory.

### 2. Track Auto-Creation

Because the file has the `.bigWig` extension:
- **Format Detection:** `normalizeFormatFromPath('GSM429321_H3K27ac.bigWig')` → `'BIGWIG'`
- **Role Assignment:** `getRoleFromFormat('BIGWIG')` → `'PRIMARY'`
- **Track Creation:** Automatic track created

### 3. Creating a Genome Browser Session

Once the track is created:
1. Navigate to `/sessions/new`
2. Select the `GSM429321_H3K27ac.bigWig` track
3. Optionally add the H3K4me3 track for comparison
4. Set genome to `hg19`
5. Create the session
6. View signal patterns in IGV or WashU

---

## Multi-Track Analysis

### Combining H3K27ac and H3K4me3

When viewing both marks together:

1. **Load both tracks** in the same session
2. **Use same vertical scale** for fair comparison
3. **Look for patterns:**
   - Overlapping peaks → Active promoters
   - H3K27ac only → Active enhancers
   - H3K4me3 only → Poised/bivalent promoters

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigWig_h3k27ac_hg19")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("H3K27ac"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to suggested genomic regions
3. Verify signal intensity is visible
4. Look for both sharp and broad peaks
5. Test zoom functionality
6. Compare with H3K4me3 track if available

---

## Data Source

**WashU Epigenome Browser Data**  
https://egg.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization and regulatory element mapping.
'''


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download and register H3K27ac signal track for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.parse_args()
    
    print()
    print("=" * 50)
    print("H3K27ac BigWig Track Registration")
    print("=" * 50)
    print()
    
    url = "https://egg.wustl.edu/d/hg19/GSM429321_H3K27ac.bigWig"
    filename = "GSM429321_H3K27ac.bigWig"
    dataset_name = "bigWig_h3k27ac_hg19"
    
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
        print("Suggested testing regions (hg19):")
        print("  - chr8:128700000-128900000 (super-enhancer region)")
        print("  - chr12:6640000-6650000 (promoter regions)")
        print("  - chr7:5500000-5600000 (general validation)")
        print()
        print("Tip: Load with H3K4me3 track for comparative analysis")
        
    except Exception as e:
        logger.error(f"Failed to register dataset: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


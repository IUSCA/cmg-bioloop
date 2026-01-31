#!/usr/bin/env python3
"""
BigWig Signal Track Registration Script

Purpose:
    Downloads H3K4me3 ChIP-seq signal track from WashU Epigenome Browser and
    registers it as DATA_PRODUCT for genome browser testing.

Usage:
    python register_bigwig.py [--help]
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from workers.scripts.genomic_data_testing.utils import register_dataset

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


PRODUCT_DOC = '''# bigWig_h3k4me3_hg19 - H3K4me3 ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321.bigWig`  
**Source:** https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig  
**Size:** ~several MB  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K4me3)

### What This Dataset Is

✅ **Real H3K4me3 ChIP-seq signal data**

✅ **Histone modification associated with active transcription**

✅ **Suitable for testing continuous signal visualization**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Signal intensity rendering

---

## H3K4me3 Background

**H3K4me3** (Histone H3 Lysine 4 Trimethylation):
- **Function:** Marks active gene promoters
- **Location:** Typically found at transcription start sites (TSS)
- **Interpretation:** High signal = active transcription region
- **Application:** Used to identify active genes and regulatory elements

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify signal tracks:

### Test Region 1
```
chr12:6643000-6648500
```
- **Chromosome:** 12
- **Region Size:** ~5.5 KB
- **Expected:** H3K4me3 signal peaks at promoter regions

### Test Region 2
```
chr7:5566000-5571000
```
- **Chromosome:** 7
- **Region Size:** ~5 KB
- **Expected:** Signal intensity patterns visible

### Test Region 3
```
chr8:128748000-128756000
```
- **Chromosome:** 8
- **Region Size:** ~8 KB
- **Expected:** Continuous signal display

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in the origin directory.

### 2. Track Auto-Creation

Because the file has the `.bigWig` extension:
- **Format Detection:** `normalizeFormatFromPath('GSM429321.bigWig')` → `'BIGWIG'`
- **Role Assignment:** `getRoleFromFormat('BIGWIG')` → `'PRIMARY'`
- **Track Creation:** Automatic track created

### 3. Creating a Genome Browser Session

Once the track is created:
1. Navigate to `/sessions/new`
2. Select the `GSM429321.bigWig` track
3. Set genome to `hg19`
4. Create the session
5. View signal patterns in IGV or WashU

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "wig",
  "format": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "color": "#2669a3",
  "height": 100
}
```

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "showOnHubLoad": true,
  "options": {
    "color": "#2669a3",
    "height": 100
  }
}
```

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigWig_h3k4me3_hg19")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("GSM429321"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr12:6643000-6648500`
3. Verify signal intensity is visible
4. Look for peaks at promoter regions
5. Test zoom functionality

---

## Interpreting the Data

When viewing this track:
- **High signal** → Active promoter/transcription start site
- **Low signal** → Inactive or repressed region
- **Sharp peaks** → Well-defined TSS
- **Broad signals** → Extended regulatory regions

Compare with gene annotations to verify H3K4me3 enrichment at promoters.

---

## Data Source

**WashU Epigenome Browser Hub Samples**  
https://vizhub.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization.
'''


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download and register BigWig signal track for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.parse_args()
    
    print()
    print("=" * 50)
    print("BigWig Signal Track Registration")
    print("=" * 50)
    print()
    
    url = "https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig"
    filename = "GSM429321.bigWig"
    dataset_name = "bigWig_h3k4me3_hg19"
    
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
        print("  - chr12:6643000-6648500")
        print("  - chr7:5566000-5571000")
        print("  - chr8:128748000-128756000")
        
    except Exception as e:
        logger.error(f"Failed to register dataset: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


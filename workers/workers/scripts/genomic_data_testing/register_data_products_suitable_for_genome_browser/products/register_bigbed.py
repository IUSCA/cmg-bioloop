#!/usr/bin/env python3
"""
BigBed Test Track Registration Script

Purpose:
    Downloads a bigBed peaks/features file from WashU Epigenome Browser and
    registers it as DATA_PRODUCT for genome browser testing.

Usage:
    python register_bigbed.py [--help]

Dataset Information:
    - Dataset: bigBed_test.bigBed
    - Source: WashU Epigenome Browser Hub Sample
    - Size: ~805 KB
    - Type: BigBed peaks/features file
    - Genome: hg19
    - Purpose: Testing BigBed track loading and display

Note:
    - File is renamed with .bigBed extension for automatic track creation
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


PRODUCT_DOC = '''# bigBed_test - Peaks/Features Track

## Dataset Overview

**File:** `bigBed_test.bigBed`  
**Source:** https://vizhub.wustl.edu/hubSample/hg19/bigBed1  
**Size:** ~805 KB  
**Format:** BigBed  
**Genome:** hg19

### What This Dataset Is

✅ **A real genomic peaks/features file in BigBed format**

✅ **Suitable for genome browser visualization testing**

✅ **Contains chromosome coordinates with associated features**

### Purpose

This dataset is used to test:
- BigBed file format support in genome browsers
- Track loading and rendering
- Feature visualization at specific genomic coordinates
- Automatic track creation from files with proper extensions

---

## File Naming Convention

**Original filename:** `bigBed1` (no extension)  
**Renamed to:** `bigBed_test.bigBed` (with extension)

### Why the Rename?

The automatic track creation system relies on file extensions to determine file type:

- ❌ `bigBed1` → Format: `null` → Role: `null` → **NO track created**
- ✅ `bigBed_test.bigBed` → Format: `BIGBED` → Role: `PRIMARY` → **Track auto-created**

**Supported extensions for auto-track creation:**
- `.bigBed` or `.bb`
- `.bigWig` or `.bw`
- `.bam`, `.cram`
- `.vcf.gz`
- `.bed.gz`
- `fragments.tsv.gz`

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify the data appears correctly:

### Test Region 1
```
chr12:6643000-6648500
```
- **Chromosome:** 12
- **Region Size:** ~5.5 KB
- **Expected:** Feature peaks should be visible

### Test Region 2
```
chr7:5566000-5571000
```
- **Chromosome:** 7
- **Region Size:** ~5 KB
- **Expected:** Feature annotations visible

### Test Region 3
```
chr8:128700000-128900000
```
- **Chromosome:** 8
- **Region Size:** ~200 KB
- **Expected:** Broader view of feature distribution

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in the origin directory.

The watch.py script will:
1. Detect the new directory
2. Register it as a DATA_PRODUCT dataset
3. Extract file metadata
4. Create dataset_file entries

### 2. Track Auto-Creation

Because the file has the `.bigBed` extension:

1. **Format Detection:** `normalizeFormatFromPath('bigBed_test.bigBed')` → `'BIGBED'`
2. **Role Assignment:** `getRoleFromFormat('BIGBED')` → `'PRIMARY'`
3. **Track Creation:** Automatic track created with:
   - `name`: `bigBed_test.bigBed`
   - `dataset_file_id`: [auto-assigned]

### 3. Creating a Genome Browser Session

Once the track is created, you can:

1. Navigate to `/sessions/new`
2. Select the `bigBed_test` track from the autocomplete
3. Set genome to `hg19`
4. Create the session
5. View in IGV or WashU browser

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "annotation",
  "format": "bigbed",
  "name": "bigBed_test.bigBed",
  "url": "/api/sessions/{id}/files/expose/staged/data_products/{hash}/bigBed_test.bigBed",
  "color": "#2669a3",
  "height": 100
}
```

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigbed",
  "name": "bigBed_test.bigBed",
  "url": "/api/sessions/{id}/files/expose/staged/data_products/{hash}/bigBed_test.bigBed",
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
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigBed_test")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("bigBed_test"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr12:6643000-6648500`
3. Verify features are visible
4. Try zooming in/out
5. Test in both IGV and WashU browsers

---

## Troubleshooting

### Issue: No track was created

**Cause:** File doesn't have proper extension or wasn't processed correctly

**Solution:**
Check file metadata to ensure format='BIGBED' and role='PRIMARY'

### Issue: Track exists but not visible in autocomplete

**Cause:** Track may not be associated with proper genomic attributes

**Solution:**
- Verify the dataset has genomic_details set
- Check that the track query includes proper filtering

### Issue: Browser can't load the file

**Cause:** File path resolution or authentication issues

**Solution:**
1. Check `dataset.metadata.stage_alias` is set
2. Verify file is in staged location
3. Check session cookie is set before loading
4. Verify file hasn't been compressed

---

## Data Source

**WashU Epigenome Browser Hub Samples**  
https://vizhub.wustl.edu/

This is publicly available test data provided by the WashU Epigenome Browser team for testing and demonstration purposes.
'''


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download and register BigBed test track for genome browser testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.parse_args()
    
    print()
    print("=" * 50)
    print("BigBed Test Track Registration")
    print("=" * 50)
    print()
    
    url = "https://vizhub.wustl.edu/hubSample/hg19/bigBed1"
    filename = "bigBed_test.bigBed"
    dataset_name = "bigBed_test"
    
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
        print("The watch.py script should detect this new directory and register it as DATA_PRODUCT.")
        print("A track will be automatically created because the file has the .bigBed extension.")
        print()
        print("Testing ranges (hg19):")
        print("  - chr12:6643000-6648500")
        print("  - chr7:5566000-5571000")
        print("  - chr8:128700000-128900000")
        
    except Exception as e:
        logger.error(f"Failed to register dataset: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


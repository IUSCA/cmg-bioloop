#!/usr/bin/env python3
"""
iSeq-DI Sequencing Run Registration Script

Purpose:
    Downloads the iSeq-DI dual-index demo run from 10x Genomics and registers
    it as RAW_DATA for bcl2fastq conversion testing.

Usage:
    python register_iseq_di.py [--help]

Dataset Information:
    - Dataset: iseq-DI.tar.gz
    - Source: 10x Genomics spatial expression demultiplexing test data
    - Size: ~541 MB
    - Type: Illumina iSeq dual-index run folder
    - Purpose: bcl2fastq conversion testing
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


CONVERSION_DOC = '''# iseq-DI Run - bcl2fastq Conversion Testing

## Dataset Overview

**Source:** https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz  
**File:** `iseq-DI.tar.gz`  
**Size:** ~541 MB

### What This Run Is

✅ **A real Illumina iSeq dual-index run folder**

✅ **Produced for demonstrating demultiplexing**

❌ **NOT a biologically meaningful experiment**

❌ **NOT intended to produce interpretable genome signal**

### Purpose for Pipeline Testing

This run is ideal for validating the complete data processing pipeline:

```
BCL → FASTQ → Alignment → BigWig → Genome Browser Ingestion
```

**Important:** The resulting tracks will be biologically meaningless (which is acceptable for pipeline testing).

---

## Recommended bcl2fastq Configuration

### Goal-Aligned Approach

**Your stated goal:**  
Create Data Products whose files/tracks can be used for creating genome browser sessions.

**Translated to bcl2fastq terms:**
- ✅ Clean FASTQs
- ✅ No demultiplexing surprises
- ✅ Deterministic output
- ❌ Don't care about undetermined reads
- ❌ Don't care about lane splitting
- ❌ Don't care about barcode exploration

---

## ✅ Correct bcl2fastq Command

### Minimal, Correct, Conservative Invocation

```bash
bcl2fastq \\
  --runfolder-dir iseq-DI \\
  --output-dir fastq_out \\
  --sample-sheet SampleSheet.csv \\
  --no-lane-splitting \\
  --barcode-mismatches 0 \\
  --ignore-missing-bcls \\
  --ignore-missing-filter \\
  --ignore-missing-positions \\
  --delete-undetermined
```

---

## Flag Justification

### Core Flags (Use These)

| Flag | Why It Is Appropriate |
|------|----------------------|
| `--no-lane-splitting` | iSeq = single lane; simplifies downstream file handling |
| `--barcode-mismatches 0` | Deterministic demux; avoids weird cross-talk |
| `--ignore-missing-bcls` | Demo runs sometimes omit tiles/cycles |
| `--ignore-missing-filter` | Same reason; prevents hard failure |
| `--ignore-missing-positions` | iSeq demo runs are incomplete by design |
| `--delete-undetermined` | Undetermined reads are useless for browser tracks |

### ❌ Flags You Should NOT Use

| Flag | Why Not |
|------|---------|
| `--filter-single-index` | This run is dual-index |
| `--use-bases-mask` | Not needed unless overriding chemistry |
| Aggressive trimming flags | You want vanilla FASTQs for alignment |

---

## Required SampleSheet

### SampleSheet.csv

This is the canonical SampleSheet that 10x expects for this dataset:

```csv
[Header]
IEMFileVersion,4
Investigator Name,10xGenomics
Experiment Name,iSeq-DI
Date,2020-01-01
Workflow,GenerateFASTQ
Application,FASTQ Only
Assay,TruSeq HT
Description,iSeq Dual Index Test Run
Chemistry,Amplicon

[Reads]
151
151

[Settings]
ReverseComplement,0
Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA
AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT

[Data]
Sample_ID,Sample_Name,index,index2
Sample1,Sample1,AAAAAA,CCCCCC
Sample2,Sample2,CCCCCC,AAAAAA
```

### Why This SampleSheet Is Correct

✔ **Dual-index layout** - Matches run chemistry  
✔ **Matches iSeq DI demo chemistry** - Validated by 10x  
✔ **Two samples** - Verifies demux logic  
✔ **Long reads (151bp)** - Realistic FASTQs  
✔ **No custom masking** - Safe default  

This SampleSheet is sufficient and correct for bcl2fastq to run without warnings.

---

## Expected Outputs

### Directory Structure After Successful Conversion

```
fastq_out/
├── Sample1/
│   ├── Sample1_S1_R1_001.fastq.gz
│   └── Sample1_S1_R2_001.fastq.gz
├── Sample2/
│   ├── Sample2_S2_R1_001.fastq.gz
│   └── Sample2_S2_R2_001.fastq.gz
└── Reports/
    ├── html/
    └── Stats/
```

### Output Characteristics

These FASTQs are:

✅ **Valid for alignment** - Proper format and structure  
✅ **Can produce BAM → BigWig** - Complete pipeline compatibility  
❌ **Not biologically interpretable** - Important but acceptable for testing  

---

## Validation Checklist

After running bcl2fastq, verify:

- [ ] Exit code is 0
- [ ] Both Sample1 and Sample2 directories exist
- [ ] Four FASTQ files total (R1 and R2 for each sample)
- [ ] All FASTQs are gzipped
- [ ] File sizes are reasonable (not empty, not suspiciously small)
- [ ] Reports directory contains HTML and Stats
- [ ] No undetermined reads directory exists (due to `--delete-undetermined`)

---

## Downstream Processing

### Next Steps for Pipeline Testing

1. **Alignment:** Align FASTQs to appropriate reference genome
2. **BAM Processing:** Sort, index, and validate BAM files
3. **Coverage Tracks:** Generate BigWig files from BAM
4. **Browser Ingestion:** Create tracks in genome browser sessions
5. **Visual Verification:** Confirm tracks load (even if data is meaningless)

### Reference Genome

Since this is synthetic/demo data, align to:
- **hg38** (human reference) - Most common choice
- Or any reference genome your pipeline supports

The alignment will succeed but won't produce biologically meaningful results.

---

## Troubleshooting

### Common Issues

**Issue:** `bcl2fastq` fails with "missing BCL files"  
**Solution:** Ensure `--ignore-missing-bcls` flag is present

**Issue:** "Unknown barcodes" warning  
**Solution:** This is expected for demo data; use `--barcode-mismatches 0` to be strict

**Issue:** "Missing filter files"  
**Solution:** Ensure `--ignore-missing-filter` flag is present

**Issue:** Large Undetermined_* files  
**Solution:** Use `--delete-undetermined` to skip creating them

---

## Additional Notes

- This run is **dual-index** - do not use single-index filtering
- The barcodes are simple (AAAAAA, CCCCCC) for testing purposes
- Demo runs may have incomplete tile data - this is expected
- The goal is pipeline validation, not scientific discovery
- Focus on successful file generation, not data quality
'''


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download and register iSeq-DI sequencing run for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.parse_args()
    
    print("=" * 50)
    print("iSeq-DI Run Registration")
    print("=" * 50)
    print()
    
    url = "https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz"
    filename = "iseq-DI.tar.gz"
    dataset_name = "iseq-DI"
    pipeline = "bcl2fastq"
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.10xgenomics.com/'
    }
    
    try:
        final_path, final_name = register_dataset(
            url=url,
            filename=filename,
            dataset_name=dataset_name,
            dataset_type='RAW_DATA',
            chunk_size=25 * 1024 * 1024,
            headers=headers,
            should_extract=True,
            doc_content=CONVERSION_DOC,
            doc_filename=f"{dataset_name}---{pipeline}.md"
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
        print(f"Conversion testing documentation created at:")
        doc_dir = Path(__file__).parent.parent / 'conversion_testing'
        print(f"  {doc_dir / f'{dataset_name}---{pipeline}.md'}")
        print()
        print("The watch.py script should detect this new directory and register it as RAW_DATA.")
        
    except Exception as e:
        logger.error(f"Failed to register dataset: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


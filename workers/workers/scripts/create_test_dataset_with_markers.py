#!/usr/bin/env python3
"""
Create a test dataset with proper completion markers for await_stability.

PRODUCTION SAFETY:
- Only writes to the configured test directory
- Creates small files (configurable size)
- Safe to run multiple times (uses unique names)

USAGE:
    # Create RAW_DATA with RTAComplete.txt marker
    python create_test_dataset_with_markers.py --type RAW_DATA

    # Create RAW_DATA with CopyComplete marker
    python create_test_dataset_with_markers.py --type RAW_DATA --marker CopyComplete

    # Create DATA_PRODUCT (no markers needed)
    python create_test_dataset_with_markers.py --type DATA_PRODUCT

    # Create with realistic Illumina structure (RunInfo.xml, etc.)
    python create_test_dataset_with_markers.py --type RAW_DATA --realistic

    # Create nanopore-style dataset (uses different path pattern)
    python create_test_dataset_with_markers.py --type RAW_DATA --nanopore

    # Dry run
    python create_test_dataset_with_markers.py --type RAW_DATA --dry-run

COMPLETION MARKERS (for RAW_DATA, non-nanopore):
    The await_stability task checks for these files in the root of the dataset:
    - Any file matching pattern '*CopyComplete*' (e.g., CopyComplete.txt, SequenceCopyComplete.txt)
    - Exact file 'RTAComplete.txt'
    
    If neither is found within 12 hours, the workflow fails.

STABILITY CHECK:
    After completion markers are found (or skipped for DATA_PRODUCT/nanopore),
    the dataset must be "stable" - no file modifications for the recency threshold:
    - Standard: 300 seconds (5 minutes) in production
    - Nanopore: 21600 seconds (6 hours)
"""

import argparse
import datetime
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from workers.config import config


def get_test_directory(dataset_type: str, is_nanopore: bool = False) -> Path:
    """Get the appropriate test directory from config."""
    if dataset_type == 'RAW_DATA':
        if is_nanopore:
            # For nanopore simulation, still use test dir but note it won't be detected as nanopore
            # because nanopore detection is path-based
            print("WARNING: Nanopore detection is path-based. Using test dir won't auto-detect as nanopore.")
            print("         The dataset will still require completion markers unless in a nanopore source path.")
        
        test_dir = config.get('registration', {}).get('RAW_DATA', {}).get('source_dir_test')
        if not test_dir:
            raise ValueError("No source_dir_test configured for RAW_DATA")
        return Path(test_dir)
    
    elif dataset_type == 'DATA_PRODUCT':
        # DATA_PRODUCT uses scratch or project paths
        # For testing, create a sibling directory to RAW_DATA test
        raw_test = config.get('registration', {}).get('RAW_DATA', {}).get('source_dir_test')
        if raw_test:
            dp_test = Path(raw_test).parent / 'data_products_test'
            return dp_test
        
        # Fallback to scratch
        scratch = config.get('registration', {}).get('DATA_PRODUCT', {}).get('source_dir_scratch')
        if scratch:
            return Path(scratch)
        raise ValueError("No test directory available for DATA_PRODUCT")
    
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")


def generate_dataset_name(dataset_type: str, prefix: str = None) -> str:
    """Generate a unique dataset name."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    type_prefix = prefix or dataset_type.lower().replace('_', '')
    return f"test_{type_prefix}_{timestamp}"


def create_illumina_structure(dataset_path: Path) -> List[Dict]:
    """
    Create files simulating a minimal Illumina run structure.
    
    Returns list of file definitions.
    """
    now = datetime.datetime.now().isoformat()
    
    return [
        {
            'path': dataset_path / 'RunInfo.xml',
            'content': f'''<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="4">
  <Run Id="test_run" Number="1">
    <Flowcell>TEST</Flowcell>
    <Instrument>TEST</Instrument>
    <Date>{now}</Date>
  </Run>
</RunInfo>
'''
        },
        {
            'path': dataset_path / 'RunParameters.xml',
            'content': f'''<?xml version="1.0"?>
<RunParameters xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <Setup>
    <SupportMultipleSurfacesInUI>true</SupportMultipleSurfacesInUI>
    <RunStartDate>{now}</RunStartDate>
  </Setup>
</RunParameters>
'''
        },
        {
            'path': dataset_path / 'SampleSheet.csv',
            'content': '''[Header]
Date,2024-01-01
Workflow,GenerateFASTQ

[Reads]
151
151

[Data]
Sample_ID,Sample_Name,index
TestSample,TestSample,ATCGATCG
'''
        },
    ]


def create_data_product_structure(dataset_path: Path) -> List[Dict]:
    """
    Create files for a minimal data product.
    
    Returns list of file definitions.
    """
    now = datetime.datetime.now().isoformat()
    
    return [
        {
            'path': dataset_path / 'sample.fastq.gz',
            # Create a minimal gzip header (empty gzip)
            'content': None,  # Will write binary
            'binary': b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        },
        {
            'path': dataset_path / 'metadata.json',
            'content': f'{{"created": "{now}", "type": "test_data_product"}}\n'
        },
    ]


def create_completion_marker(dataset_path: Path, marker_type: str) -> Dict:
    """Create a completion marker file definition."""
    now = datetime.datetime.now().isoformat()
    
    if marker_type == 'RTAComplete':
        return {
            'path': dataset_path / 'RTAComplete.txt',
            'content': f'''RTA 2.0 completed at {now}
Run completed successfully.
'''
        }
    elif marker_type == 'CopyComplete':
        return {
            'path': dataset_path / 'CopyComplete.txt',
            'content': f'''Copy completed at {now}
All files transferred successfully.
'''
        }
    elif marker_type == 'SequenceCopyComplete':
        return {
            'path': dataset_path / 'SequenceCopyComplete.txt',
            'content': f'''Sequence copy completed at {now}
'''
        }
    else:
        raise ValueError(f"Unknown marker type: {marker_type}")


def create_test_dataset(
    dataset_type: str,
    name: str = None,
    marker_type: str = 'RTAComplete',
    realistic: bool = False,
    is_nanopore: bool = False,
    dry_run: bool = False
) -> Path:
    """
    Create a test dataset with completion markers.
    
    Args:
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        name: Dataset name (auto-generated if not provided)
        marker_type: Type of completion marker ('RTAComplete', 'CopyComplete', 'SequenceCopyComplete')
        realistic: If True, create realistic directory structure
        is_nanopore: If True, create nanopore-style dataset
        dry_run: If True, only print what would be created
        
    Returns:
        Path to the created dataset directory
    """
    test_dir = get_test_directory(dataset_type, is_nanopore)
    dataset_name = name or generate_dataset_name(dataset_type, 'nanopore' if is_nanopore else None)
    dataset_path = test_dir / dataset_name
    
    print(f"Configuration:")
    print(f"  Dataset type: {dataset_type}")
    print(f"  Test directory: {test_dir}")
    print(f"  Dataset name: {dataset_name}")
    print(f"  Full path: {dataset_path}")
    print(f"  Realistic structure: {realistic}")
    print(f"  Nanopore style: {is_nanopore}")
    print()
    
    # Determine what files to create
    files_to_create = []
    
    # Add structure files based on type
    if dataset_type == 'RAW_DATA':
        if realistic:
            files_to_create.extend(create_illumina_structure(dataset_path))
        else:
            # Minimal test file
            files_to_create.append({
                'path': dataset_path / 'test_data.txt',
                'content': f'Test RAW_DATA created at {datetime.datetime.now().isoformat()}\n'
            })
        
        # Add completion marker for non-nanopore RAW_DATA
        if not is_nanopore:
            files_to_create.append(create_completion_marker(dataset_path, marker_type))
            print(f"Completion marker: {marker_type} (REQUIRED for standard RAW_DATA)")
        else:
            print(f"Completion marker: NONE (nanopore datasets skip this check)")
            print(f"WARNING: Path-based detection may still require markers if not in nanopore source dir")
    
    elif dataset_type == 'DATA_PRODUCT':
        if realistic:
            files_to_create.extend(create_data_product_structure(dataset_path))
        else:
            files_to_create.append({
                'path': dataset_path / 'test_data.txt',
                'content': f'Test DATA_PRODUCT created at {datetime.datetime.now().isoformat()}\n'
            })
        print(f"Completion marker: NOT REQUIRED (DATA_PRODUCT)")
    
    print()
    print("Files to create:")
    for f in files_to_create:
        rel_path = f['path'].relative_to(test_dir)
        size_info = "binary" if f.get('binary') else f"{len(f.get('content', ''))} bytes"
        print(f"  - {rel_path} ({size_info})")
    
    if dry_run:
        print()
        print("[DRY RUN] No files created")
        return dataset_path
    
    # Safety check: ensure we're writing to expected test area
    expected_prefix = '/home/cmguser/cmg-bioloop-ingestion-test'
    if not str(test_dir).startswith(expected_prefix):
        raise ValueError(
            f"SAFETY ERROR: Test directory {test_dir} does not start with {expected_prefix}. "
            "Refusing to create files outside of test area."
        )
    
    # Create dataset directory
    print()
    print(f"Creating directory: {dataset_path}")
    dataset_path.mkdir(parents=True, exist_ok=True)
    
    # Create files
    for f in files_to_create:
        print(f"Creating file: {f['path'].name}")
        if f.get('binary'):
            f['path'].write_bytes(f['binary'])
        else:
            f['path'].write_text(f['content'])
    
    # Print summary
    print()
    print("=" * 50)
    print("SUCCESS: Test dataset created")
    print("=" * 50)
    print()
    
    # Calculate expected behavior
    recency_threshold = config['registration']['recency_threshold_seconds']
    if is_nanopore:
        recency_threshold = config['registration']['recency_threshold_seconds_nanopore']
    
    print("AWAIT_STABILITY BEHAVIOR:")
    print()
    if dataset_type == 'RAW_DATA' and not is_nanopore:
        print(f"  1. Completion markers: REQUIRED")
        print(f"     - Looking for: '*CopyComplete*' or 'RTAComplete.txt'")
        print(f"     - Created: {marker_type}")
        print(f"     - Max wait time: 12 hours (then fails)")
    else:
        print(f"  1. Completion markers: SKIPPED")
        if dataset_type == 'DATA_PRODUCT':
            print(f"     - Reason: Dataset type is DATA_PRODUCT")
        else:
            print(f"     - Reason: Nanopore dataset (path-based detection)")
    
    print()
    print(f"  2. Stability check: REQUIRED")
    print(f"     - Recency threshold: {recency_threshold} seconds")
    print(f"     - Dataset must have no modifications for this duration")
    
    print()
    print(f"ESTIMATED TIME TO PASS: ~{recency_threshold} seconds (after last file write)")
    print()
    print("WORKFLOW: intake_integrated (from watch.py obs5)")
    print("  Steps: await_stability -> inspect -> archive -> stage -> validate -> setup_download")
    
    return dataset_path


def main():
    parser = argparse.ArgumentParser(
        description='Create a test dataset with completion markers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--type', '-t',
        choices=['RAW_DATA', 'DATA_PRODUCT'],
        default='RAW_DATA',
        help='Dataset type (default: RAW_DATA)'
    )
    parser.add_argument(
        '--name', '-n',
        help='Dataset name (auto-generated if not provided)'
    )
    parser.add_argument(
        '--marker', '-m',
        choices=['RTAComplete', 'CopyComplete', 'SequenceCopyComplete'],
        default='RTAComplete',
        help='Completion marker type for RAW_DATA (default: RTAComplete)'
    )
    parser.add_argument(
        '--realistic', '-r',
        action='store_true',
        help='Create realistic directory structure (Illumina for RAW_DATA, FASTQ for DATA_PRODUCT)'
    )
    parser.add_argument(
        '--nanopore',
        action='store_true',
        help='Create nanopore-style dataset (WARNING: path detection may still require markers)'
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Show what would be created without creating anything'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("TEST DATASET CREATOR WITH COMPLETION MARKERS")
    print("=" * 60)
    print()
    
    try:
        create_test_dataset(
            dataset_type=args.type,
            name=args.name,
            marker_type=args.marker,
            realistic=args.realistic,
            is_nanopore=args.nanopore,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

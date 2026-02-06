#!/usr/bin/env python3
"""
Create a minimal test dataset that passes await_stability in the least amount of time.

PRODUCTION SAFETY:
- Only writes to the configured test directory
- Creates minimal files (< 1KB total)
- Safe to run multiple times (uses unique names)

USAGE:
    # Create a DATA_PRODUCT (no completion markers needed, fastest)
    python create_minimal_test_dataset.py --type DATA_PRODUCT

    # Create a RAW_DATA with completion markers
    python create_minimal_test_dataset.py --type RAW_DATA

    # Specify custom name
    python create_minimal_test_dataset.py --type DATA_PRODUCT --name my_test_dataset

    # Dry run (show what would be created)
    python create_minimal_test_dataset.py --type RAW_DATA --dry-run

NOTES:
- DATA_PRODUCT datasets do NOT require completion markers (await_stability skips that check)
- RAW_DATA (non-nanopore) datasets REQUIRE completion markers to pass await_stability
- Both types still need to wait for the recency threshold (stability check)

RECENCY THRESHOLDS (from production.py):
- Standard: 300 seconds (5 minutes)
- Nanopore: 21600 seconds (6 hours)
"""

import argparse
import datetime
import os
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from workers.config import config


def get_test_directory(dataset_type: str) -> Path:
    """Get the test directory from config."""
    if dataset_type == 'RAW_DATA':
        test_dir = config.get('registration', {}).get('RAW_DATA', {}).get('source_dir_test')
        if not test_dir:
            raise ValueError("No source_dir_test configured for RAW_DATA in production.py")
        return Path(test_dir)
    elif dataset_type == 'DATA_PRODUCT':
        # DATA_PRODUCT doesn't have a dedicated test path, use scratch
        # But for safety, we'll use a subdirectory of RAW_DATA test path
        raw_test = config.get('registration', {}).get('RAW_DATA', {}).get('source_dir_test')
        if raw_test:
            # Use sibling directory for DATA_PRODUCT tests
            return Path(raw_test).parent / 'data_products_test'
        raise ValueError("No test directory configured")
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")


def generate_dataset_name(prefix: str = "test") -> str:
    """Generate a unique dataset name with timestamp."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}"


def create_minimal_dataset(
    dataset_type: str,
    name: str = None,
    dry_run: bool = False,
    include_completion_marker: bool = True
) -> Path:
    """
    Create a minimal test dataset.
    
    Args:
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        name: Dataset name (auto-generated if not provided)
        dry_run: If True, only print what would be created
        include_completion_marker: If True, include completion marker for RAW_DATA
        
    Returns:
        Path to the created dataset directory
    """
    test_dir = get_test_directory(dataset_type)
    dataset_name = name or generate_dataset_name(f"minimal_{dataset_type.lower()}")
    dataset_path = test_dir / dataset_name
    
    print(f"Dataset type: {dataset_type}")
    print(f"Test directory: {test_dir}")
    print(f"Dataset name: {dataset_name}")
    print(f"Full path: {dataset_path}")
    print()
    
    # Determine what files to create
    files_to_create = []
    
    # Always create a small test file
    files_to_create.append({
        'path': dataset_path / 'test_file.txt',
        'content': f'Minimal test dataset created at {datetime.datetime.now().isoformat()}\n'
    })
    
    # For RAW_DATA, add completion marker if requested
    if dataset_type == 'RAW_DATA' and include_completion_marker:
        files_to_create.append({
            'path': dataset_path / 'RTAComplete.txt',
            'content': f'RTA completed at {datetime.datetime.now().isoformat()}\n'
        })
        print("Completion marker: RTAComplete.txt (required for RAW_DATA)")
    elif dataset_type == 'DATA_PRODUCT':
        print("Completion marker: NOT REQUIRED (DATA_PRODUCT skips this check)")
    
    print()
    print("Files to create:")
    for f in files_to_create:
        print(f"  - {f['path'].relative_to(test_dir)}")
    
    if dry_run:
        print()
        print("[DRY RUN] No files created")
        return dataset_path
    
    # Safety check: ensure we're writing to the expected test directory
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
        f['path'].write_text(f['content'])
    
    print()
    print("SUCCESS: Minimal test dataset created")
    print()
    print("NEXT STEPS:")
    print(f"  1. The watch.py observer will detect this directory")
    print(f"  2. A dataset will be registered via the API")
    print(f"  3. The 'intake_integrated' workflow will start")
    print(f"  4. await_stability will run:")
    if dataset_type == 'RAW_DATA':
        print(f"     - Check for completion markers: WILL FIND RTAComplete.txt")
    else:
        print(f"     - Check for completion markers: SKIPPED (DATA_PRODUCT)")
    print(f"     - Wait for recency threshold: {config['registration']['recency_threshold_seconds']} seconds")
    print()
    print(f"ESTIMATED TIME TO PASS await_stability: ~{config['registration']['recency_threshold_seconds']} seconds")
    
    return dataset_path


def main():
    parser = argparse.ArgumentParser(
        description='Create a minimal test dataset for await_stability testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--type', '-t',
        choices=['RAW_DATA', 'DATA_PRODUCT'],
        default='DATA_PRODUCT',
        help='Dataset type (default: DATA_PRODUCT - fastest, no completion markers needed)'
    )
    parser.add_argument(
        '--name', '-n',
        help='Dataset name (auto-generated if not provided)'
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Show what would be created without creating anything'
    )
    parser.add_argument(
        '--no-completion-marker',
        action='store_true',
        help='For RAW_DATA: skip creating completion marker (dataset will wait 12h then fail)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("MINIMAL TEST DATASET CREATOR")
    print("=" * 60)
    print()
    
    try:
        create_minimal_dataset(
            dataset_type=args.type,
            name=args.name,
            dry_run=args.dry_run,
            include_completion_marker=not args.no_completion_marker
        )
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

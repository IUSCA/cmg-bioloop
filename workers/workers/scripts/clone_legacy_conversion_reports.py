#!/usr/bin/env python3
"""
Copy legacy CMG conversion reports to new location.

This script scans the legacy conversion-reports-container directory and copies
all Reports directories to a new structured location.

Source path pattern: /N/project/CMG-SCA/production/conversion/{conversion_id}/{dataset_id}/Reports/
Target path pattern: {target_base}/conversions/{conversion_id}/{dataset_id}/Reports/

Usage:
    python clone_legacy_conversion_reports.py [--source-base PATH] [--target-base PATH] [--dry-run] [--verbose] [--list-sizes]

Options:
    --source-base PATH  Source base path to conversion directory (default: /N/project/CMG-SCA/production/conversion)
    --target-base PATH  Target base path for copied reports (REQUIRED)
    --dry-run          Show what would be copied without actually copying
    --verbose          Show detailed progress
    --list-sizes       Calculate and display directory sizes (requires --verbose)
    --skip-existing    Skip conversions where target already exists
    --continue-on-error Continue processing even if individual copies fail
"""

import argparse
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def format_bytes(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def get_directory_size(directory: Path) -> int:
    """
    Calculate total size of all files in a directory recursively.
    
    Args:
        directory: Path to directory
        
    Returns:
        Total size in bytes
    """
    total_size = 0
    try:
        for entry in os.scandir(directory):
            if entry.is_file(follow_symlinks=False):
                try:
                    total_size += entry.stat().st_size
                except OSError as e:
                    logger.warning(f"Could not stat file {entry.path}: {e}")
            elif entry.is_dir(follow_symlinks=False):
                total_size += get_directory_size(Path(entry.path))
    except PermissionError as e:
        logger.warning(f"Permission denied accessing {directory}: {e}")
    except OSError as e:
        logger.warning(f"Error accessing {directory}: {e}")
    
    return total_size


def find_reports_directories(source_base: Path) -> List[Tuple[str, str, Path]]:
    """
    Find all Reports directories in the conversion structure.
    
    Path structure: {source_base}/{conversion_id}/{dataset_id}/Reports/
    
    Args:
        source_base: Source base conversion directory path
        
    Returns:
        List of tuples: (conversion_id, dataset_id, reports_path)
    """
    reports_dirs = []
    
    if not source_base.exists():
        logger.error(f"Source base path does not exist: {source_base}")
        return reports_dirs
    
    if not source_base.is_dir():
        logger.error(f"Source base path is not a directory: {source_base}")
        return reports_dirs
    
    logger.info(f"Scanning source base path: {source_base}")
    
    # Walk through conversion_id directories
    try:
        conversion_dirs = [d for d in source_base.iterdir() if d.is_dir()]
        logger.info(f"Found {len(conversion_dirs)} conversion directories")
        
        for conversion_dir in conversion_dirs:
            conversion_id = conversion_dir.name
            
            # Walk through dataset_id directories
            try:
                dataset_dirs = [d for d in conversion_dir.iterdir() if d.is_dir()]
                
                for dataset_dir in dataset_dirs:
                    dataset_id = dataset_dir.name
                    reports_path = dataset_dir / 'Reports'
                    
                    # Check if Reports directory exists
                    if reports_path.exists() and reports_path.is_dir():
                        reports_dirs.append((conversion_id, dataset_id, reports_path))
                        logger.debug(f"Found Reports dir: {conversion_id}/{dataset_id}/Reports")
            
            except PermissionError as e:
                logger.warning(f"Permission denied accessing {conversion_dir}: {e}")
            except OSError as e:
                logger.warning(f"Error accessing {conversion_dir}: {e}")
    
    except PermissionError as e:
        logger.warning(f"Permission denied accessing {source_base}: {e}")
    except OSError as e:
        logger.warning(f"Error accessing {source_base}: {e}")
    
    return reports_dirs


def copy_reports(
    source_base: Path,
    target_base: Path,
    dry_run: bool = False,
    skip_existing: bool = False,
    continue_on_error: bool = False,
    verbose: bool = False,
    list_sizes: bool = False
) -> Dict:
    """
    Copy all conversion reports from source to target location.
    
    Args:
        source_base: Source base conversion directory path
        target_base: Target base path for copied reports
        dry_run: If True, show what would be copied without copying
        skip_existing: Skip conversions where target already exists
        continue_on_error: Continue processing even if individual copies fail
        verbose: Show detailed progress
        list_sizes: Calculate and display sizes (only with verbose)
        
    Returns:
        Dictionary with copy results
    """
    reports_dirs = find_reports_directories(source_base)
    
    if not reports_dirs:
        logger.warning("No Reports directories found")
        return {
            'total_found': 0,
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'total_size_bytes': 0
        }
    
    logger.info(f"Found {len(reports_dirs)} Reports directories to process")
    
    if dry_run:
        logger.info("DRY RUN MODE - No files will be copied")
    
    # Create target base directory if it doesn't exist
    if not dry_run:
        try:
            target_base.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured target base directory exists: {target_base}")
        except OSError as e:
            logger.error(f"Failed to create target base directory: {e}")
            return {
                'total_found': len(reports_dirs),
                'copied': 0,
                'skipped': 0,
                'failed': len(reports_dirs),
                'total_size_bytes': 0,
                'error': str(e)
            }
    
    copied_count = 0
    skipped_count = 0
    failed_count = 0
    total_copied_size = 0
    failed_reports = []
    
    for idx, (conversion_id, dataset_id, source_reports_path) in enumerate(reports_dirs, 1):
        # Construct target path: {target_base}/conversions/{conversion_id}/{dataset_id}/Reports
        target_reports_path = target_base / 'conversions' / conversion_id / dataset_id / 'Reports'
        
        if verbose:
            logger.info(f"Processing {idx}/{len(reports_dirs)}: {conversion_id}/{dataset_id}")
            logger.info(f"  Source: {source_reports_path}")
            logger.info(f"  Target: {target_reports_path}")
        
        # Check if target already exists
        if target_reports_path.exists():
            if skip_existing:
                logger.info(f"  Skipping (target exists): {conversion_id}/{dataset_id}")
                skipped_count += 1
                continue
            else:
                logger.warning(f"  Target already exists: {target_reports_path}")
                if not dry_run:
                    logger.warning(f"  Will overwrite existing directory")
        
        # Calculate source size only if requested for display
        source_size = None
        if verbose and list_sizes:
            source_size = get_directory_size(source_reports_path)
            logger.info(f"  Source size: {format_bytes(source_size)}")
        
        if dry_run:
            if source_size is not None:
                logger.info(f"  [DRY RUN] Would copy {format_bytes(source_size)}")
                total_copied_size += source_size
            else:
                logger.info(f"  [DRY RUN] Would copy")
            copied_count += 1
            continue
        
        # Perform actual copy
        try:
            # Create parent directories
            target_reports_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove target if it exists (for overwrite)
            if target_reports_path.exists():
                shutil.rmtree(target_reports_path)
            
            # Copy directory tree
            shutil.copytree(source_reports_path, target_reports_path, symlinks=False)
            
            if source_size is not None:
                logger.info(f"  Successfully copied: {conversion_id}/{dataset_id} ({format_bytes(source_size)})")
                total_copied_size += source_size
            else:
                logger.info(f"  Successfully copied: {conversion_id}/{dataset_id}")
            copied_count += 1
        
        except PermissionError as e:
            logger.error(f"  Permission denied: {e}")
            failed_count += 1
            failed_reports.append({
                'conversion_id': conversion_id,
                'dataset_id': dataset_id,
                'error': str(e)
            })
            if not continue_on_error:
                logger.error("Stopping due to error (use --continue-on-error to continue)")
                break
        
        except OSError as e:
            logger.error(f"  Copy failed: {e}")
            failed_count += 1
            failed_reports.append({
                'conversion_id': conversion_id,
                'dataset_id': dataset_id,
                'error': str(e)
            })
            if not continue_on_error:
                logger.error("Stopping due to error (use --continue-on-error to continue)")
                break
        
        except Exception as e:
            logger.error(f"  Unexpected error: {e}")
            failed_count += 1
            failed_reports.append({
                'conversion_id': conversion_id,
                'dataset_id': dataset_id,
                'error': str(e)
            })
            if not continue_on_error:
                logger.error("Stopping due to error (use --continue-on-error to continue)")
                break
    
    result = {
        'total_found': len(reports_dirs),
        'copied': copied_count,
        'skipped': skipped_count,
        'failed': failed_count,
        'failed_reports': failed_reports
    }
    
    # Only include size information if it was calculated
    if list_sizes:
        result['total_size_bytes'] = total_copied_size
        result['total_size_human'] = format_bytes(total_copied_size)
    
    return result


def print_summary(results: Dict, dry_run: bool = False) -> None:
    """Print copy operation summary."""
    print("\n" + "=" * 80)
    print("CONVERSION REPORTS COPY SUMMARY")
    if dry_run:
        print("(DRY RUN - No files were actually copied)")
    print("=" * 80)
    
    print(f"\nTotal Reports directories found: {results['total_found']}")
    print(f"Successfully copied: {results['copied']}")
    print(f"Skipped (already exists): {results['skipped']}")
    print(f"Failed: {results['failed']}")
    
    # Only show size summary if sizes were calculated
    if 'total_size_bytes' in results and results['copied'] > 0:
        action = "Would copy" if dry_run else "Copied"
        print(f"\nTotal size {action.lower()}: {results['total_size_human']} ({results['total_size_bytes']:,} bytes)")
    
    if results.get('failed_reports'):
        print("\n" + "-" * 80)
        print("FAILED REPORTS:")
        print("-" * 80)
        for failed in results['failed_reports']:
            print(f"  {failed['conversion_id']}/{failed['dataset_id']}")
            print(f"    Error: {failed['error']}")
    
    print("\n" + "=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Copy legacy CMG conversion reports to new location.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--source-base',
        type=str,
        default='/N/project/CMG-SCA/production/conversion',
        help='Source base path to conversion directory (default: /N/project/CMG-SCA/production/conversion)'
    )
    parser.add_argument(
        '--target-base',
        type=str,
        required=True,
        help='Target base path for copied reports (REQUIRED)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be copied without actually copying'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed progress'
    )
    parser.add_argument(
        '--list-sizes',
        action='store_true',
        help='Calculate and display directory sizes (requires --verbose)'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip conversions where target already exists'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue processing even if individual copies fail'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    source_base = Path(args.source_base)
    target_base = Path(args.target_base)
    
    logger.info(f"Starting copy operation")
    logger.info(f"Source base: {source_base}")
    logger.info(f"Target base: {target_base}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE ENABLED")
    
    # Validate that list_sizes requires verbose
    if args.list_sizes and not args.verbose:
        logger.warning("--list-sizes requires --verbose to display sizes, enabling verbose mode")
        args.verbose = True
    
    try:
        results = copy_reports(
            source_base=source_base,
            target_base=target_base,
            dry_run=args.dry_run,
            skip_existing=args.skip_existing,
            continue_on_error=args.continue_on_error,
            verbose=args.verbose,
            list_sizes=args.list_sizes
        )
        print_summary(results, dry_run=args.dry_run)
        
        # Exit with error code if any copies failed
        if results['failed'] > 0:
            logger.warning(f"{results['failed']} copy operation(s) failed")
            return 1
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("\nCopy operation interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error during copy operation: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

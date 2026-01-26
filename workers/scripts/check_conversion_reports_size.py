#!/usr/bin/env python3
"""
Check cumulative size of all legacy CMG conversion reports.

This script scans the conversion-reports-container directory and calculates
the total size of all Reports directories across all conversions.

Path pattern: /N/project/CMG-SCA/production/conversion/{conversion_id}/{dataset_name}/Reports/

Usage:
    python check_conversion_reports_size.py [--base-path PATH] [--verbose]

Options:
    --base-path PATH  Base path to conversion directory (default: /N/project/CMG-SCA/production/conversion)
    --verbose         Show detailed per-conversion report sizes
    --summary-only    Show only the final summary
"""

import argparse
import logging
import os
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


def find_reports_directories(base_path: Path) -> List[Tuple[str, str, Path]]:
    """
    Find all Reports directories in the conversion structure.
    
    Path structure: {base_path}/{conversion_id}/{dataset_name}/Reports/
    
    Args:
        base_path: Base conversion directory path
        
    Returns:
        List of tuples: (conversion_id, dataset_name, reports_path)
    """
    reports_dirs = []
    
    if not base_path.exists():
        logger.error(f"Base path does not exist: {base_path}")
        return reports_dirs
    
    if not base_path.is_dir():
        logger.error(f"Base path is not a directory: {base_path}")
        return reports_dirs
    
    logger.info(f"Scanning base path: {base_path}")
    
    # Walk through conversion_id directories
    try:
        conversion_dirs = [d for d in base_path.iterdir() if d.is_dir()]
        logger.info(f"Found {len(conversion_dirs)} conversion directories")
        
        for conversion_dir in conversion_dirs:
            conversion_id = conversion_dir.name
            
            # Walk through dataset_name directories
            try:
                dataset_dirs = [d for d in conversion_dir.iterdir() if d.is_dir()]
                
                for dataset_dir in dataset_dirs:
                    dataset_name = dataset_dir.name
                    reports_path = dataset_dir / 'Reports'
                    
                    # Check if Reports directory exists
                    if reports_path.exists() and reports_path.is_dir():
                        reports_dirs.append((conversion_id, dataset_name, reports_path))
                        logger.debug(f"Found Reports dir: {conversion_id}/{dataset_name}/Reports")
            
            except PermissionError as e:
                logger.warning(f"Permission denied accessing {conversion_dir}: {e}")
            except OSError as e:
                logger.warning(f"Error accessing {conversion_dir}: {e}")
    
    except PermissionError as e:
        logger.warning(f"Permission denied accessing {base_path}: {e}")
    except OSError as e:
        logger.warning(f"Error accessing {base_path}: {e}")
    
    return reports_dirs


def analyze_reports(base_path: Path, verbose: bool = False, summary_only: bool = False) -> Dict:
    """
    Analyze all conversion reports and calculate sizes.
    
    Args:
        base_path: Base conversion directory path
        verbose: Show detailed per-conversion sizes
        summary_only: Show only final summary
        
    Returns:
        Dictionary with analysis results
    """
    reports_dirs = find_reports_directories(base_path)
    
    if not reports_dirs:
        logger.warning("No Reports directories found")
        return {
            'total_reports_dirs': 0,
            'total_size_bytes': 0,
            'total_size_human': '0 B',
            'reports': []
        }
    
    logger.info(f"Found {len(reports_dirs)} Reports directories")
    
    if not summary_only:
        logger.info("Calculating sizes...")
    
    reports_data = []
    total_size = 0
    
    for idx, (conversion_id, dataset_name, reports_path) in enumerate(reports_dirs, 1):
        if not summary_only and verbose:
            logger.info(f"Processing {idx}/{len(reports_dirs)}: {conversion_id}/{dataset_name}")
        
        size_bytes = get_directory_size(reports_path)
        total_size += size_bytes
        
        reports_data.append({
            'conversion_id': conversion_id,
            'dataset_name': dataset_name,
            'path': str(reports_path),
            'size_bytes': size_bytes,
            'size_human': format_bytes(size_bytes)
        })
        
        if verbose and not summary_only:
            logger.info(f"  Size: {format_bytes(size_bytes)}")
    
    # Sort by size (largest first)
    reports_data.sort(key=lambda x: x['size_bytes'], reverse=True)
    
    return {
        'total_reports_dirs': len(reports_dirs),
        'total_size_bytes': total_size,
        'total_size_human': format_bytes(total_size),
        'reports': reports_data
    }


def print_report(results: Dict, verbose: bool = False, summary_only: bool = False) -> None:
    """Print analysis results."""
    print("\n" + "=" * 80)
    print("CONVERSION REPORTS SIZE ANALYSIS")
    print("=" * 80)
    
    print(f"\nTotal Reports directories found: {results['total_reports_dirs']}")
    print(f"Total cumulative size: {results['total_size_human']} ({results['total_size_bytes']:,} bytes)")
    
    if not summary_only and verbose and results['reports']:
        print("\n" + "-" * 80)
        print("TOP 10 LARGEST REPORTS:")
        print("-" * 80)
        print(f"{'Conversion ID':<40} {'Dataset Name':<30} {'Size':>10}")
        print("-" * 80)
        
        for report in results['reports'][:10]:
            print(f"{report['conversion_id']:<40} {report['dataset_name']:<30} {report['size_human']:>10}")
    
    if not summary_only and results['reports']:
        print("\n" + "-" * 80)
        print("SIZE DISTRIBUTION:")
        print("-" * 80)
        
        # Calculate size buckets
        sizes = [r['size_bytes'] for r in results['reports']]
        if sizes:
            avg_size = sum(sizes) / len(sizes)
            max_size = max(sizes)
            min_size = min(sizes)
            
            print(f"Average size: {format_bytes(avg_size)}")
            print(f"Largest: {format_bytes(max_size)}")
            print(f"Smallest: {format_bytes(min_size)}")
    
    print("\n" + "=" * 80)
    print(f"TOTAL SIZE: {results['total_size_human']} ({results['total_size_bytes']:,} bytes)")
    print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Check cumulative size of all legacy CMG conversion reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--base-path',
        type=str,
        default='/N/project/CMG-SCA/production/conversion',
        help='Base path to conversion directory (default: /N/project/CMG-SCA/production/conversion)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed per-conversion report sizes'
    )
    parser.add_argument(
        '--summary-only',
        action='store_true',
        help='Show only the final summary (no detailed output during processing)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Optional: Write results to JSON file'
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
    if args.summary_only:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    base_path = Path(args.base_path)
    
    logger.info(f"Starting analysis of conversion reports in: {base_path}")
    
    try:
        results = analyze_reports(base_path, verbose=args.verbose, summary_only=args.summary_only)
        print_report(results, verbose=args.verbose, summary_only=args.summary_only)
        
        # Optionally write to JSON
        if args.output:
            import json
            output_path = Path(args.output)
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Results written to: {output_path}")
    
    except KeyboardInterrupt:
        logger.info("\nAnalysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

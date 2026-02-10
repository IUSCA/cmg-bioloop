#!/usr/bin/env python3
"""
Test script for CMG API integration.

Usage:
    python test_cmg_api_integration.py --origin-path /path/to/dataset --type RAW_DATA
    python test_cmg_api_integration.py --origin-path /path/to/dataset --type DATA_PRODUCT
"""
import argparse
import sys
import logging
from pathlib import Path

# Add workers to path
sys.path.insert(0, str(Path(__file__).parent))

import workers.cmg_api as cmg_api

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_dataset_query(origin_path: str):
    """Test querying CMG for a dataset by origin_path."""
    logger.info(f'\n{"="*80}')
    logger.info(f'Testing RAW_DATA query for: {origin_path}')
    logger.info(f'{"="*80}')
    
    try:
        dataset = cmg_api.get_dataset_by_origin_path(origin_path)
        
        if dataset:
            logger.info('✅ Dataset found in CMG!')
            logger.info(f'  CMG ID: {dataset.get("_id")}')
            logger.info(f'  Name: {dataset.get("name")}')
            logger.info(f'  Archived: {dataset.get("archived")}')
            logger.info(f'  Taken: {dataset.get("taken")}')
            logger.info(f'  Inspected: {dataset.get("inspected")}')
            logger.info(f'  Archive Path: {dataset.get("paths", {}).get("archive")}')
            
            # Check if archived
            is_archived = cmg_api.is_dataset_archived_in_cmg(origin_path, 'RAW_DATA')
            logger.info(f'\n  Archival Status: {"✅ ARCHIVED" if is_archived else "⏳ NOT ARCHIVED"}')
            
            if not is_archived and dataset.get('taken'):
                worker = dataset.get('taken', {})
                logger.info(f'  Worker Processing: {worker.get("name")} ({worker.get("status")})')
        else:
            logger.info('❌ Dataset NOT found in CMG')
            logger.info('  Action: Bioloop can safely register and archive')
            
    except Exception as e:
        logger.error(f'❌ Error querying CMG API: {e}', exc_info=True)
        return False
    
    return True


def test_dataproduct_query(origin_path: str):
    """Test querying CMG for a dataproduct by origin_path."""
    logger.info(f'\n{"="*80}')
    logger.info(f'Testing DATA_PRODUCT query for: {origin_path}')
    logger.info(f'{"="*80}')
    
    try:
        dataproduct = cmg_api.get_dataproduct_by_origin_path(origin_path)
        
        if dataproduct:
            logger.info('✅ Dataproduct found in CMG!')
            logger.info(f'  CMG ID: {dataproduct.get("_id")}')
            logger.info(f'  Name: {dataproduct.get("name")}')
            logger.info(f'  Origin Path: {dataproduct.get("origin_path")}')
            logger.info(f'  Archive Path: {dataproduct.get("paths", {}).get("archive")}')
            logger.info(f'  Staged Path: {dataproduct.get("paths", {}).get("staged")}')
            logger.info(f'  Upload: {dataproduct.get("upload")}')
            logger.info(f'  Visible: {dataproduct.get("visible")}')
            
            # Check if archived
            is_archived = cmg_api.is_dataset_archived_in_cmg(origin_path, 'DATA_PRODUCT')
            logger.info(f'\n  Archival Status: {"✅ ARCHIVED" if is_archived else "⏳ NOT ARCHIVED"}')
        else:
            logger.info('❌ Dataproduct NOT found in CMG')
            logger.info('  Note: Legacy dataproducts (before 2026-02-08) have empty origin_path')
            logger.info('  Action: Bioloop can safely register and archive')
            
    except Exception as e:
        logger.error(f'❌ Error querying CMG API: {e}', exc_info=True)
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Test CMG API integration for concurrent archival coordination',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test RAW_DATA dataset query
  python test_cmg_api_integration.py \\
    --origin-path /opt/sca/cmg/data/source/250117_M70445_3027_000000000-KLMNO5769 \\
    --type RAW_DATA
  
  # Test DATA_PRODUCT dataproduct query
  python test_cmg_api_integration.py \\
    --origin-path /opt/sca/cmg/data/uploads/user_upload_123 \\
    --type DATA_PRODUCT
  
  # Test with trailing slash (should be normalized)
  python test_cmg_api_integration.py \\
    --origin-path /opt/sca/cmg/data/source/dataset_name/ \\
    --type RAW_DATA
        """
    )
    parser.add_argument(
        '--origin-path',
        required=True,
        help='Origin path of the dataset/dataproduct to query'
    )
    parser.add_argument(
        '--type',
        required=True,
        choices=['RAW_DATA', 'DATA_PRODUCT'],
        help='Type of entity to query'
    )
    
    args = parser.parse_args()
    
    logger.info('\n' + '='*80)
    logger.info('CMG API Integration Test')
    logger.info('='*80)
    logger.info(f'Origin Path: {args.origin_path}')
    logger.info(f'Type: {args.type}')
    
    # Run appropriate test
    if args.type == 'RAW_DATA':
        success = test_dataset_query(args.origin_path)
    else:
        success = test_dataproduct_query(args.origin_path)
    
    logger.info('\n' + '='*80)
    if success:
        logger.info('✅ Test completed successfully')
    else:
        logger.error('❌ Test failed')
        sys.exit(1)
    logger.info('='*80 + '\n')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Bioloop File Info Population Script

This script processes archived datasets that are missing file information by:
1. Downloading datasets from SDA to a configured location
2. Running the 'file_info_population' workflow on each dataset
3. Processing datasets in batches with configurable size limits
4. Tracking progress and supporting resumption after interruption

The workflow includes: inspect, archive, stage, validate, delete_source

Usage:
    python -m workers.scripts.populate_file_info [OPTIONS]

Options:
    --batch-size: Number of datasets to process per batch (default: from config)
    --max-size-tb: Maximum download directory size in TB (default: from config)
    --download-dir: Directory for downloading datasets (default: from config)
    --dry-run: Simulate the process without making changes
    --resume: Resume from the last incomplete batch
    --force-restart: Start from the beginning, ignoring previous state
    --log-level: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
"""

import argparse
import json
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from celery import current_app
from sca_rhythm import Workflow

import workers.api as api
import workers.sda as sda
import workers.workflow_utils as wf_utils
from workers.config import config

# Setup logging
logger = logging.getLogger(__name__)

class FileInfoPopulationManager:
    """Manages the file info population process for archived datasets."""
    
    def __init__(self, 
                 batch_size: int = None,
                 max_size_tb: float = None,
                 download_dir: str = None,
                 dry_run: bool = False,
                 resume: bool = False,
                 force_restart: bool = False):
        
        # Load configuration
        self.config = config.get('file_info_population', {})
        
        # Set parameters with fallbacks to config
        self.batch_size = batch_size or self.config.get('batch_size', 10)
        self.max_size_tb = max_size_tb or self.config.get('max_download_size_tb', 10)
        self.max_size_bytes = int(self.max_size_tb * 1024 * 1024 * 1024 * 1024)  # TB to bytes
        self.download_dir = Path(download_dir or self.config.get('download_dir', '/opt/sca/data/file_info_downloads'))
        self.state_file = Path(self.config.get('state_file', '/opt/sca/data/file_info_population_state.json'))
        self.poll_interval = self.config.get('poll_interval_seconds', 300)
        self.max_retries = self.config.get('max_retries_per_dataset', 3)
        
        self.dry_run = dry_run
        self.resume = resume
        self.force_restart = force_restart
        
        # Initialize state
        self.state = self._load_state()
        if force_restart:
            self.state = self._create_initial_state()
        
        # Ensure download directory exists
        if not dry_run:
            self.download_dir.mkdir(parents=True, exist_ok=True)
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
    
    def _create_initial_state(self) -> Dict:
        """Create initial state structure."""
        return {
            'current_batch': 0,
            'processed_datasets': [],
            'failed_datasets': [],
            'current_batch_datasets': [],
            'last_updated': datetime.now().isoformat(),
            'total_datasets_found': 0,
            'completed': False
        }
    
    def _load_state(self) -> Dict:
        """Load state from file or create initial state."""
        if self.state_file.exists() and not self.force_restart:
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.info(f"Loaded existing state from {self.state_file}")
                return state
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}. Starting fresh.")
        
        return self._create_initial_state()
    
    def _save_state(self):
        """Save current state to file."""
        if not self.dry_run:
            self.state['last_updated'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.debug(f"State saved to {self.state_file}")
    
    def get_datasets_needing_file_info(self) -> List[Dict]:
        """
        Get archived datasets that need file info population.
        
        A dataset needs file info if:
        1. It's archived (has archive_path)
        2. It has no files metadata or empty files list
        3. It hasn't been processed yet
        """
        logger.info("Fetching archived datasets that need file info population...")
        
        # Get all archived datasets
        archived_datasets = api.get_all_datasets(archived=True, bundle=True)
        logger.info(f"Found {len(archived_datasets)} archived datasets")
        
        datasets_needing_info = []
        processed_ids = set(self.state['processed_datasets'] + 
                          [d['id'] for d in self.state['failed_datasets']] +
                          [d['id'] for d in self.state['current_batch_datasets']])
        
        for dataset in archived_datasets:
            # Skip if already processed
            if dataset['id'] in processed_ids:
                continue
                
            # Check if dataset needs file info
            if self._dataset_needs_file_info(dataset):
                datasets_needing_info.append(dataset)
        
        logger.info(f"Found {len(datasets_needing_info)} datasets needing file info population")
        return datasets_needing_info
    
    def _dataset_needs_file_info(self, dataset: Dict) -> bool:
        """Check if a dataset needs file info population."""
        # Must be archived
        if not dataset.get('archive_path'):
            return False
        
        # Check if it has file metadata
        try:
            dataset_with_files = api.get_dataset(dataset_id=dataset['id'], files=True)
            files = dataset_with_files.get('files', [])
            
            # Needs file info if no files or empty files list
            return len(files) == 0
        except Exception as e:
            logger.warning(f"Error checking files for dataset {dataset['id']}: {e}")
            return True  # Assume it needs processing if we can't check
    
    def _get_current_download_size(self) -> int:
        """Get current size of download directory in bytes."""
        if not self.download_dir.exists():
            return 0
        
        total_size = 0
        try:
            for path in self.download_dir.rglob('*'):
                if path.is_file():
                    total_size += path.stat().st_size
        except Exception as e:
            logger.warning(f"Error calculating download directory size: {e}")
        
        return total_size
    
    def _can_download_dataset(self, dataset: Dict) -> bool:
        """Check if we can download a dataset without exceeding size limits."""
        current_size = self._get_current_download_size()
        dataset_size = dataset.get('du_size', 0)
        
        if isinstance(dataset_size, str):
            try:
                dataset_size = int(dataset_size)
            except (ValueError, TypeError):
                dataset_size = 0
        
        would_exceed = (current_size + dataset_size) > self.max_size_bytes
        
        if would_exceed:
            logger.warning(f"Cannot download dataset {dataset['id']} ({dataset_size} bytes). "
                         f"Would exceed limit: {current_size + dataset_size} > {self.max_size_bytes}")
        
        return not would_exceed
    
    def download_dataset_from_sda(self, dataset: Dict) -> Tuple[bool, str, Optional[Path]]:
        """
        Download a dataset from SDA.
        
        Returns:
            (success: bool, message: str, local_path: Optional[Path])
        """
        dataset_id = dataset['id']
        dataset_name = dataset['name']
        archive_path = dataset['archive_path']
        
        if not archive_path:
            return False, f"Dataset {dataset_id} has no archive_path", None
        
        # Create local download path
        local_path = self.download_dir / f"{dataset_name}_{dataset_id}.tar"
        
        try:
            logger.info(f"Downloading dataset {dataset_id} from SDA: {archive_path}")
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would download {archive_path} to {local_path}")
                return True, "Dry run - download simulated", local_path
            
            # Download from SDA
            sda.get(sda_file=archive_path, local_file=str(local_path), verify_checksum=True)
            
            if not local_path.exists():
                return False, f"Download failed - file not found at {local_path}", None
            
            # Verify checksum if bundle info is available
            if dataset.get('bundle') and dataset['bundle'].get('md5'):
                expected_checksum = dataset['bundle']['md5']
                import workers.utils as utils
                actual_checksum = utils.checksum(local_path)
                
                if actual_checksum != expected_checksum:
                    local_path.unlink(missing_ok=True)
                    return False, f"Checksum mismatch: expected {expected_checksum}, got {actual_checksum}", None
            
            logger.info(f"Successfully downloaded dataset {dataset_id} to {local_path}")
            return True, "Download successful", local_path
            
        except Exception as e:
            # Clean up partial download
            if local_path.exists():
                local_path.unlink(missing_ok=True)
            return False, f"Download failed: {str(e)}", None
    
    def extract_and_prepare_dataset(self, dataset: Dict, downloaded_path: Path) -> Tuple[bool, str, Optional[Path]]:
        """
        Extract downloaded dataset and prepare for processing.
        
        Returns:
            (success: bool, message: str, extracted_path: Optional[Path])
        """
        dataset_id = dataset['id']
        dataset_name = dataset['name']
        
        # Create extraction directory
        extract_dir = self.download_dir / f"{dataset_name}_{dataset_id}_extracted"
        
        try:
            if self.dry_run:
                logger.info(f"DRY RUN: Would extract {downloaded_path} to {extract_dir}")
                return True, "Dry run - extraction simulated", extract_dir
            
            # Remove existing extraction directory
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            # Extract tar file
            import tarfile
            logger.info(f"Extracting {downloaded_path} to {extract_dir}")
            
            with tarfile.open(downloaded_path, 'r') as tar:
                tar.extractall(path=extract_dir)
            
            # Find the actual dataset directory (should be the only directory in extract_dir)
            extracted_items = list(extract_dir.iterdir())
            if len(extracted_items) == 1 and extracted_items[0].is_dir():
                actual_dataset_path = extracted_items[0]
            else:
                actual_dataset_path = extract_dir
            
            logger.info(f"Dataset {dataset_id} extracted to {actual_dataset_path}")
            return True, "Extraction successful", actual_dataset_path
            
        except Exception as e:
            # Clean up on failure
            if extract_dir.exists():
                shutil.rmtree(extract_dir, ignore_errors=True)
            return False, f"Extraction failed: {str(e)}", None
    
    def update_dataset_origin_path(self, dataset: Dict, new_path: Path) -> bool:
        """Update dataset's origin_path to point to downloaded location."""
        try:
            if self.dry_run:
                logger.info(f"DRY RUN: Would update dataset {dataset['id']} origin_path to {new_path}")
                return True
            
            update_data = {'origin_path': str(new_path)}
            api.update_dataset(dataset_id=dataset['id'], update_data=update_data)
            logger.info(f"Updated dataset {dataset['id']} origin_path to {new_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to update dataset {dataset['id']} origin_path: {e}")
            return False
    
    def start_file_info_workflow(self, dataset: Dict) -> Tuple[bool, str]:
        """Start the file_info_population workflow for a dataset."""
        dataset_id = dataset['id']
        
        try:
            if self.dry_run:
                logger.info(f"DRY RUN: Would start file_info_population workflow for dataset {dataset_id}")
                return True, "Dry run - workflow start simulated"
            
            # Check if workflow is already running
            dataset_with_workflows = api.get_dataset(dataset_id=dataset_id, workflows=True)
            active_workflows = [wf for wf in dataset_with_workflows['workflows'] 
                              if wf['name'] == 'file_info_population' and wf['status'] not in ['SUCCESS', 'FAILURE', 'REVOKED']]
            
            if active_workflows:
                return True, f"Workflow already running: {active_workflows[0]['id']}"
            
            # Start new workflow
            logger.info(f"Starting file_info_population workflow for dataset {dataset_id}")
            workflow_body = wf_utils.get_wf_body(wf_name='file_info_population')
            workflow = Workflow(celery_app=current_app, **workflow_body)
            workflow_id = workflow.workflow['_id']
            
            api.add_workflow_to_dataset(dataset_id=dataset_id, workflow_id=workflow_id)
            workflow.start(dataset_id)
            
            logger.info(f"Started workflow {workflow_id} for dataset {dataset_id}")
            return True, f"Workflow started: {workflow_id}"
            
        except Exception as e:
            return False, f"Failed to start workflow: {str(e)}"
    
    def check_batch_completion(self, batch_datasets: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Check completion status of datasets in current batch.
        
        Returns:
            (completed_datasets, failed_datasets, still_running_datasets)
        """
        completed = []
        failed = []
        running = []
        
        for dataset in batch_datasets:
            try:
                dataset_with_workflows = api.get_dataset(dataset_id=dataset['id'], workflows=True)
                workflows = dataset_with_workflows.get('workflows', [])
                
                # Find file_info_population workflows
                file_info_workflows = [wf for wf in workflows if wf['name'] == 'file_info_population']
                
                if not file_info_workflows:
                    # No workflow found - consider it failed
                    failed.append(dataset)
                    continue
                
                # Check most recent workflow
                latest_workflow = max(file_info_workflows, key=lambda x: x.get('created_at', ''))
                status = latest_workflow.get('status', 'UNKNOWN')
                
                if status == 'SUCCESS':
                    completed.append(dataset)
                elif status in ['FAILURE', 'REVOKED']:
                    failed.append(dataset)
                else:
                    running.append(dataset)
                    
            except Exception as e:
                logger.error(f"Error checking workflow status for dataset {dataset['id']}: {e}")
                failed.append(dataset)
        
        return completed, failed, running
    
    def cleanup_dataset_files(self, dataset: Dict):
        """Clean up downloaded files for a dataset."""
        dataset_id = dataset['id']
        dataset_name = dataset['name']
        
        # Clean up downloaded tar file
        tar_path = self.download_dir / f"{dataset_name}_{dataset_id}.tar"
        if tar_path.exists():
            try:
                tar_path.unlink()
                logger.info(f"Cleaned up downloaded tar: {tar_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up tar file {tar_path}: {e}")
        
        # Clean up extracted directory
        extract_dir = self.download_dir / f"{dataset_name}_{dataset_id}_extracted"
        if extract_dir.exists():
            try:
                shutil.rmtree(extract_dir)
                logger.info(f"Cleaned up extracted directory: {extract_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up extracted directory {extract_dir}: {e}")
    
    def process_batch(self, datasets: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Process a batch of datasets.
        
        Returns:
            (successfully_started, failed_to_start)
        """
        logger.info(f"Processing batch of {len(datasets)} datasets")
        
        successfully_started = []
        failed_to_start = []
        
        for dataset in datasets:
            dataset_id = dataset['id']
            dataset_name = dataset['name']
            
            logger.info(f"Processing dataset {dataset_id}: {dataset_name}")
            
            # Check if we can download without exceeding size limits
            if not self._can_download_dataset(dataset):
                logger.warning(f"Skipping dataset {dataset_id} due to size constraints")
                failed_to_start.append(dataset)
                continue
            
            # Download dataset
            success, message, downloaded_path = self.download_dataset_from_sda(dataset)
            if not success:
                logger.error(f"Failed to download dataset {dataset_id}: {message}")
                failed_to_start.append(dataset)
                continue
            
            # Extract dataset
            success, message, extracted_path = self.extract_and_prepare_dataset(dataset, downloaded_path)
            if not success:
                logger.error(f"Failed to extract dataset {dataset_id}: {message}")
                self.cleanup_dataset_files(dataset)
                failed_to_start.append(dataset)
                continue
            
            # Update dataset origin_path
            if not self.update_dataset_origin_path(dataset, extracted_path):
                logger.error(f"Failed to update origin_path for dataset {dataset_id}")
                self.cleanup_dataset_files(dataset)
                failed_to_start.append(dataset)
                continue
            
            # Start workflow
            success, message = self.start_file_info_workflow(dataset)
            if not success:
                logger.error(f"Failed to start workflow for dataset {dataset_id}: {message}")
                self.cleanup_dataset_files(dataset)
                failed_to_start.append(dataset)
                continue
            
            logger.info(f"Successfully started processing for dataset {dataset_id}: {message}")
            successfully_started.append(dataset)
        
        return successfully_started, failed_to_start
    
    def wait_for_batch_completion(self, batch_datasets: List[Dict]):
        """Wait for all datasets in batch to complete processing."""
        logger.info(f"Waiting for batch of {len(batch_datasets)} datasets to complete...")
        
        remaining_datasets = batch_datasets.copy()
        
        while remaining_datasets:
            logger.info(f"Checking status of {len(remaining_datasets)} remaining datasets...")
            
            completed, failed, still_running = self.check_batch_completion(remaining_datasets)
            
            # Update state with completed datasets
            for dataset in completed:
                self.state['processed_datasets'].append(dataset['id'])
                self.cleanup_dataset_files(dataset)
                logger.info(f"Dataset {dataset['id']} completed successfully")
            
            # Update state with failed datasets
            for dataset in failed:
                self.state['failed_datasets'].append({
                    'id': dataset['id'],
                    'name': dataset['name'],
                    'failed_at': datetime.now().isoformat(),
                    'batch': self.state['current_batch']
                })
                self.cleanup_dataset_files(dataset)
                logger.error(f"Dataset {dataset['id']} failed processing")
            
            # Update remaining datasets
            remaining_datasets = still_running
            
            if remaining_datasets:
                logger.info(f"{len(remaining_datasets)} datasets still running. "
                          f"Waiting {self.poll_interval} seconds before next check...")
                if not self.dry_run:
                    time.sleep(self.poll_interval)
                else:
                    # In dry run, simulate completion
                    break
            
            # Save state after each check
            self._save_state()
        
        logger.info("Batch processing completed")
    
    def run(self):
        """Main execution method."""
        logger.info("Starting File Info Population process")
        logger.info(f"Configuration: batch_size={self.batch_size}, "
                   f"max_size_tb={self.max_size_tb}, download_dir={self.download_dir}")
        
        if self.dry_run:
            logger.info("DRY RUN MODE - No actual changes will be made")
        
        # Get datasets that need processing
        datasets_to_process = self.get_datasets_needing_file_info()
        
        if not datasets_to_process:
            logger.info("No datasets need file info population. Exiting.")
            return
        
        self.state['total_datasets_found'] = len(datasets_to_process)
        
        # Resume from current batch if specified
        if self.resume and self.state['current_batch_datasets']:
            logger.info(f"Resuming from batch {self.state['current_batch']} with "
                       f"{len(self.state['current_batch_datasets'])} datasets")
            
            # Wait for current batch to complete
            self.wait_for_batch_completion(self.state['current_batch_datasets'])
            
            # Clear current batch
            self.state['current_batch_datasets'] = []
            self.state['current_batch'] += 1
        
        # Process remaining datasets in batches
        processed_count = len(self.state['processed_datasets'])
        remaining_datasets = datasets_to_process[processed_count:]
        
        while remaining_datasets:
            # Create next batch
            batch = remaining_datasets[:self.batch_size]
            remaining_datasets = remaining_datasets[self.batch_size:]
            
            logger.info(f"Starting batch {self.state['current_batch']} with {len(batch)} datasets")
            logger.info(f"Remaining datasets after this batch: {len(remaining_datasets)}")
            
            # Process batch
            successfully_started, failed_to_start = self.process_batch(batch)
            
            # Update state
            self.state['current_batch_datasets'] = successfully_started
            for dataset in failed_to_start:
                self.state['failed_datasets'].append({
                    'id': dataset['id'],
                    'name': dataset['name'],
                    'failed_at': datetime.now().isoformat(),
                    'batch': self.state['current_batch'],
                    'reason': 'Failed to start processing'
                })
            
            self._save_state()
            
            if successfully_started:
                # Wait for batch to complete
                self.wait_for_batch_completion(successfully_started)
            
            # Clear current batch and move to next
            self.state['current_batch_datasets'] = []
            self.state['current_batch'] += 1
            self._save_state()
        
        # Mark as completed
        self.state['completed'] = True
        self.state['completed_at'] = datetime.now().isoformat()
        self._save_state()
        
        # Print summary
        total_processed = len(self.state['processed_datasets'])
        total_failed = len(self.state['failed_datasets'])
        
        logger.info("File Info Population process completed!")
        logger.info(f"Summary:")
        logger.info(f"  Total datasets found: {self.state['total_datasets_found']}")
        logger.info(f"  Successfully processed: {total_processed}")
        logger.info(f"  Failed: {total_failed}")
        
        if total_failed > 0:
            logger.info("Failed datasets:")
            for failed in self.state['failed_datasets']:
                logger.info(f"  - {failed['id']} ({failed['name']}): {failed.get('reason', 'Unknown')}")


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'/tmp/file_info_population_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Populate file info for archived datasets in Bioloop")
    
    parser.add_argument("--batch-size", type=int, help="Number of datasets to process per batch")
    parser.add_argument("--max-size-tb", type=float, help="Maximum download directory size in TB")
    parser.add_argument("--download-dir", help="Directory for downloading datasets")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the process without making changes")
    parser.add_argument("--resume", action="store_true", help="Resume from the last incomplete batch")
    parser.add_argument("--force-restart", action="store_true", help="Start from the beginning, ignoring previous state")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
                       help="Set the logging level")
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    try:
        manager = FileInfoPopulationManager(
            batch_size=args.batch_size,
            max_size_tb=args.max_size_tb,
            download_dir=args.download_dir,
            dry_run=args.dry_run,
            resume=args.resume,
            force_restart=args.force_restart
        )
        
        manager.run()
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

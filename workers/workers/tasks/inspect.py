import re
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm.progress import Progress

import workers.api as api
import workers.cmd as cmd
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
from workers import exceptions as exc
from workers.config import config
from workers.legacy_migration import get_retrieved_archive_extraction_path, is_legacy_dataset

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def is_nanopore_dataset(origin_path: str) -> bool:
    """
    Determine if a dataset is from a nanopore source based on its origin path.
    
    Args:
        origin_path: The origin path of the dataset
        
    Returns:
        bool: True if the dataset is from a nanopore source, False otherwise
    """
    # Get nanopore source paths from config
    nanopore_paths = []
    reg_config = config.get('registration', {}).get('RAW_DATA', {})
    
    # Collect all nanopore source directories from config
    for key, value in reg_config.items():
        if key.startswith('source_dir_nanopore') and isinstance(value, str):
            nanopore_paths.append(value)
    
    # Check if origin_path starts with any nanopore path
    for nanopore_path in nanopore_paths:
        if origin_path.startswith(nanopore_path):
            return True
    
    return False


def check_completion_markers(source: Path) -> bool:
    """
    Check if a dataset directory contains completion marker files.
    Required for standard Illumina datasets, not required for nanopore datasets.
    
    Completion markers:
    - Any file matching pattern '*CopyComplete*'
    - Exact file 'RTAComplete.txt'
    
    Args:
        source: Path to the dataset directory
        
    Returns:
        bool: True if completion markers are found, False otherwise
    """
    try:
        # Iterate through all files in the directory (not recursive)
        for item in source.iterdir():
            if item.is_file():
                filename = item.name
                
                # Check for CopyComplete pattern (case-sensitive)
                if re.search('CopyComplete', filename):
                    logger.info(f'Found completion marker: {filename}')
                    return True
                
                # Check for exact RTAComplete.txt match
                if filename == 'RTAComplete.txt':
                    logger.info(f'Found completion marker: {filename}')
                    return True
        
        return False
    except Exception as e:
        logger.error(f'Error checking completion markers in {source}: {e}')
        return False


def generate_metadata(celery_task, source: Path):
    """
    source is a directory that exists and has to readable and executable (see files inside)
    all the files and directories under source should be readable
    returns:    number of files, 
                number of directories, 
                sum of stat size of all files, 
                number of genome data files,
                the md5 digest and relative filenames of genome data files
    """
    num_files, num_directories, size, num_genome_files = 0, 0, 0, 0
    metadata = []
    errors = []
    if not utils.is_readable(source):
        msg = f'source {source} is either not readable or not traversable'
        raise exc.InspectionFailed(msg)

    paths = list(source.rglob('*'))
    progress = Progress(celery_task=celery_task, name='', units='items')

    for p in progress(paths):
        if utils.is_readable(p):
            if p.is_file():
                num_files += 1
                # if symlink, only add the size of the symlink, not the pointed file
                file_size = p.lstat().st_size
                size += file_size
                # do not compute checksum for symlinks
                hex_digest = utils.checksum(p) if not p.is_symlink() else None
                relpath = p.relative_to(source)
                metadata.append({
                    'path': str(relpath),
                    'md5': hex_digest,
                    'size': file_size,
                    'type': utils.filetype(p)
                })
                if ''.join(p.suffixes) in config['genome_file_types'] and not p.is_symlink():
                    num_genome_files += 1
            elif p.is_dir():
                num_directories += 1
        else:
            errors.append(f'{p} is not readable/traversable')

    if len(errors) > 0:
        raise exc.InspectionFailed(errors)

    return num_files, num_directories, size, num_genome_files, metadata


def inspect_dataset(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id)
    
    # For legacy datasets, use extracted archive path instead of origin_path
    if is_legacy_dataset(dataset):
        source = get_retrieved_archive_extraction_path(dataset)
        logger.info(f'Inspecting legacy dataset from extracted archive path: {source}')
        
        # Verify the path exists
        if not source.exists():
            raise exc.RetryableException(
                f'Extracted archive path does not exist: {source}. '
                'Ensure retrieve_archive completed successfully.'
            )
    else:
        source = Path(dataset['origin_path']).resolve()
        origin_path = dataset['origin_path']
        logger.info(f'Inspecting dataset from origin path: {source}')
        
        # Check if this is a new RAW_DATA dataset that needs completion marker verification
        # Skip this check for:
        # - Legacy datasets (already handled above)
        # - DATA_PRODUCT datasets
        # - Datasets that are not in their origin location
        is_raw_data = dataset.get('type') == 'RAW_DATA'
        is_nanopore = is_nanopore_dataset(origin_path)
        
        if is_raw_data and not is_nanopore:
            # Standard Illumina dataset - check for completion markers
            logger.info(f'Checking for completion markers (standard Illumina dataset)')
            has_completion_markers = check_completion_markers(source)
            
            if not has_completion_markers:
                error_msg = (
                    f'Dataset {dataset["name"]} does not have required completion markers. '
                    f'Looking for: "*CopyComplete*" or "RTAComplete.txt" in {source}'
                )
                logger.error(error_msg)
                raise exc.InspectionFailed(error_msg)
            
            logger.info(f'Completion markers found - proceeding with inspection')
        elif is_raw_data and is_nanopore:
            # Nanopore dataset - no completion markers required
            logger.info(f'Nanopore dataset detected - skipping completion marker check')
    
    du_size = cmd.total_size(source)
    num_files, num_directories, size, num_genome_files, metadata = generate_metadata(celery_task, source)

    update_data = {
        'du_size': du_size,
        'size': size,
        'num_files': num_files,
        'num_directories': num_directories,
        'metadata': {
            'num_genome_files': num_genome_files,
        }

    }
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    # split metadata into batches and add to dataset
    # this is to avoid large payloads to the API
    for batch in utils.batched(metadata, n=config['inspect']['file_metadata_batch_size']):
        api.add_files_to_dataset(dataset_id=dataset_id, files=batch)
    
    # Add INSPECTED state to track inspection completion
    # This is used by stage_migrated workflow to track progress
    api.add_state_to_dataset(dataset_id=dataset_id, state='INSPECTED')

    return dataset_id,

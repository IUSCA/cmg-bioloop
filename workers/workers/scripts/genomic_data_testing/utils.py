import logging
import os
import shutil
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Tuple

import requests

import workers.api as api
from workers.config import config

logger = logging.getLogger(__name__)


def get_origin_path(dataset_type: str) -> Path:
    """
    Get the origin path for a dataset type based on current configuration.
    
    Args:
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        
    Returns:
        Path object for the origin directory
    """
    source_dir = config['registration'][dataset_type]['source_dir']
    return Path(source_dir)


def check_dataset_exists(dataset_type: str, dataset_name: str) -> bool:
    """
    Check if a dataset with the given name already exists.
    
    Args:
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        dataset_name: Name of the dataset to check
        
    Returns:
        True if dataset exists, False otherwise
    """
    try:
        datasets = api.get_all_datasets(
            dataset_type=dataset_type,
            name=dataset_name,
            match_name_exact=True
        )
        return len(datasets) > 0
    except Exception as e:
        logger.warning(f"Error checking if dataset exists: {e}")
        return False


def get_unique_dataset_name(dataset_type: str, base_name: str) -> str:
    """
    Get a unique dataset name by appending numeric suffix if needed.
    
    Args:
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        base_name: Base name for the dataset
        
    Returns:
        Unique dataset name (may have ---1, ---2, etc. appended)
    """
    if not check_dataset_exists(dataset_type, base_name):
        return base_name
    
    counter = 1
    while True:
        candidate_name = f"{base_name}---{counter}"
        if not check_dataset_exists(dataset_type, candidate_name):
            logger.info(f"Dataset '{base_name}' already exists, using '{candidate_name}'")
            return candidate_name
        counter += 1


def download_file_chunked(url: str, destination: Path, chunk_size: int = 25 * 1024 * 1024,
                          headers: Optional[dict] = None) -> None:
    """
    Download a file in chunks with resume capability.
    
    Args:
        url: URL to download from
        destination: Path where file should be saved
        chunk_size: Size of each chunk in bytes (default: 25MB)
        headers: Optional additional headers for the request
    """
    logger.info(f"Downloading from: {url}")
    logger.info(f"Destination: {destination}")
    
    # Get file size if available
    head_headers = headers.copy() if headers else {}
    response = requests.head(url, headers=head_headers, allow_redirects=True, timeout=30)
    
    has_content_length = 'content-length' in response.headers
    
    if has_content_length:
        # Server provides file size - use chunked download with resume capability
        total_size = int(response.headers['content-length'])
        logger.info(f"Total size: {total_size} bytes")
        
        # Create destination file if it doesn't exist
        destination.touch(exist_ok=True)
        current_size = destination.stat().st_size
        
        if current_size > 0:
            logger.info(f"Resuming download from byte {current_size}")
        
        # Download in chunks with range requests
        start = current_size
        session = requests.Session()
        
        with open(destination, 'ab') as f:
            while start < total_size:
                end = min(start + chunk_size - 1, total_size - 1)
                
                logger.info(f"Fetching bytes {start}-{end}")
                
                chunk_headers = headers.copy() if headers else {}
                chunk_headers['Range'] = f'bytes={start}-{end}'
                
                try:
                    response = session.get(url, headers=chunk_headers, timeout=60, stream=True)
                    response.raise_for_status()
                    
                    for data_chunk in response.iter_content(chunk_size=8192):
                        f.write(data_chunk)
                    
                    start = end + 1
                except requests.RequestException as e:
                    logger.error(f"Download chunk failed: {e}")
                    raise
    else:
        # Server uses chunked encoding - download without range requests
        logger.warning("Server does not provide Content-Length, downloading without resume capability")
        
        session = requests.Session()
        request_headers = headers.copy() if headers else {}
        
        try:
            response = session.get(url, headers=request_headers, timeout=60, stream=True)
            response.raise_for_status()
            
            downloaded_size = 0
            with open(destination, 'wb') as f:
                for data_chunk in response.iter_content(chunk_size=8192):
                    if data_chunk:
                        f.write(data_chunk)
                        downloaded_size += len(data_chunk)
                        
                        # Log progress every ~25MB
                        if downloaded_size % (25 * 1024 * 1024) < 8192:
                            logger.info(f"Downloaded {downloaded_size / (1024*1024):.1f} MB")
            
            logger.info(f"Download complete: {downloaded_size / (1024*1024):.1f} MB")
        except requests.RequestException as e:
            logger.error(f"Download failed: {e}")
            raise
    
    logger.info("Download complete")


def extract_archive(archive_path: Path, extract_to: Path) -> None:
    """
    Extract an archive file (tar.gz, tar.bz2, tar.xz, zip, gz).
    
    Args:
        archive_path: Path to the archive file
        extract_to: Directory to extract to
    """
    archive_name = archive_path.name
    logger.info(f"Extracting {archive_name}...")
    
    if archive_name.endswith(('.tar.gz', '.tgz')):
        with tarfile.open(archive_path, 'r:gz') as tar:
            tar.extractall(path=extract_to)
        archive_path.unlink()
        logger.info("Extraction complete (.tar.gz)")
        
    elif archive_name.endswith(('.tar.bz2', '.tbz2')):
        with tarfile.open(archive_path, 'r:bz2') as tar:
            tar.extractall(path=extract_to)
        archive_path.unlink()
        logger.info("Extraction complete (.tar.bz2)")
        
    elif archive_name.endswith(('.tar.xz', '.txz')):
        with tarfile.open(archive_path, 'r:xz') as tar:
            tar.extractall(path=extract_to)
        archive_path.unlink()
        logger.info("Extraction complete (.tar.xz)")
        
    elif archive_name.endswith('.tar'):
        with tarfile.open(archive_path, 'r') as tar:
            tar.extractall(path=extract_to)
        archive_path.unlink()
        logger.info("Extraction complete (.tar)")
        
    elif archive_name.endswith('.gz') and not archive_name.endswith('.tar.gz'):
        import gzip
        output_path = extract_to / archive_name[:-3]
        with gzip.open(archive_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        archive_path.unlink()
        logger.info("Decompression complete (.gz)")
        
    elif archive_name.endswith('.zip'):
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        archive_path.unlink()
        logger.info("Extraction complete (.zip)")
    else:
        logger.info("File is not compressed, will move as-is")


def flatten_single_directory(directory: Path) -> None:
    """
    If directory contains only a single subdirectory, flatten it.
    
    Args:
        directory: Directory to check and flatten
    """
    items = list(directory.iterdir())
    
    if len(items) == 1 and items[0].is_dir():
        single_dir = items[0]
        logger.info(f"Flattening single extracted directory: {single_dir.name}")
        
        for item in single_dir.iterdir():
            item.rename(directory / item.name)
        
        single_dir.rmdir()


def organize_dataset(
    temp_dir: Path,
    destination_base: Path,
    dataset_name: str,
    dataset_type: str
) -> Tuple[Path, str]:
    """
    Organize downloaded/extracted files into final dataset directory.
    
    Args:
        temp_dir: Temporary directory containing files
        destination_base: Base destination directory
        dataset_name: Desired name for the dataset
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        
    Returns:
        Tuple of (final_dataset_path, final_dataset_name)
    """
    final_name = get_unique_dataset_name(dataset_type, dataset_name)
    final_path = destination_base / final_name
    
    logger.info(f"Creating directory: {final_path}")
    final_path.mkdir(parents=True, exist_ok=False)
    
    logger.info(f"Moving contents to: {final_path}")
    for item in temp_dir.iterdir():
        shutil.move(str(item), str(final_path / item.name))
    
    return final_path, final_name


def create_documentation(doc_path: Path, content: str) -> None:
    """
    Create a markdown documentation file.
    
    Args:
        doc_path: Path where documentation should be created
        content: Content of the documentation file
    """
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    doc_path.write_text(content)
    logger.info(f"Created documentation: {doc_path}")


def register_dataset(
    url: str,
    filename: str,
    dataset_name: str,
    dataset_type: str,
    chunk_size: int = 25 * 1024 * 1024,
    headers: Optional[dict] = None,
    should_extract: bool = True,
    doc_content: Optional[str] = None,
    doc_filename: Optional[str] = None
) -> Tuple[Path, str]:
    """
    Complete dataset registration workflow: download, extract, organize.
    
    Args:
        url: URL to download from
        filename: Name of the file to download
        dataset_name: Name for the dataset
        dataset_type: 'RAW_DATA' or 'DATA_PRODUCT'
        chunk_size: Size of download chunks
        headers: Optional headers for download
        should_extract: Whether to extract archives
        doc_content: Optional documentation content
        doc_filename: Optional documentation filename
        
    Returns:
        Tuple of (final_dataset_path, final_dataset_name)
    """
    logger.info(f"Starting registration for: {dataset_name}")
    logger.info(f"Dataset type: {dataset_type}")
    
    # Get destination path
    destination_base = get_origin_path(dataset_type)
    
    # Create documentation if provided
    if doc_content and doc_filename:
        script_dir = Path(__file__).parent
        if dataset_type == 'RAW_DATA':
            doc_dir = script_dir / 'register_sequencing_runs' / 'conversion_testing'
        else:
            doc_dir = script_dir / 'register_data_products_suitable_for_genome_browser' / 'product_docs'
        doc_path = doc_dir / doc_filename
        create_documentation(doc_path, doc_content)
    
    # Download and process in temp directory
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        download_path = temp_dir / filename
        
        download_file_chunked(url, download_path, chunk_size, headers)
        
        if not download_path.exists():
            raise FileNotFoundError(f"File not found after download: {download_path}")
        
        file_size = download_path.stat().st_size
        logger.info(f"File size: {file_size} bytes")
        
        if should_extract:
            extract_archive(download_path, temp_dir)
            flatten_single_directory(temp_dir)
        
        final_path, final_name = organize_dataset(
            temp_dir, destination_base, dataset_name, dataset_type
        )
    
    logger.info(f"Successfully created dataset: {final_name} at {final_path}")
    return final_path, final_name


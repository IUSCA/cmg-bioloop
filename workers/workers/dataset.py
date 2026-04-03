import hashlib
from pathlib import Path

from glom import glom

import workers.workflow_utils as wf_utils
from workers import api
from workers.config import config

# Single place to change if the bundle format ever moves away from tar.
_BUNDLE_EXTENSION = '.tar'


def deterministic_uuid(input_string: str) -> str:
    # Convert the input string to bytes (encoding is important for consistent results)
    input_bytes = input_string.encode('utf-8')

    # Use SHA-256 hashing algorithm
    sha256_hash = hashlib.sha256(input_bytes)

    # Get the hexadecimal representation of the hash
    hex_hash = sha256_hash.hexdigest()

    # Take the first 32 characters of the hash to get a 32-character UUID-like string
    uuid_like_string = hex_hash[:32]

    return uuid_like_string


def stage_alias(dataset: dict) -> str:
    salt = config['stage']['alias_salt']
    return deterministic_uuid(f'{dataset["id"]}{dataset["name"]}{salt}')


def compute_staging_path(dataset: dict) -> tuple[Path, str]:
    dataset_type = dataset['type']
    staging_dir = Path(config['paths'][dataset_type]['stage']).resolve()
    alias = glom(dataset, 'metadata.stage_alias', default=stage_alias(dataset))
    return staging_dir / alias / dataset['name'], alias


def get_archive_path(dataset: dict) -> str:
    """Full path where archive_dataset stores the generated bundle.

    Formula (mirrors workers/tasks/archive.py):
        get_archive_dir(type) / {name}{_BUNDLE_EXTENSION}
    """
    archive_dir = wf_utils.get_archive_dir(dataset['type'], create=False)
    return f'{archive_dir}/{get_archive_bundle_name(dataset)}'


def get_archive_bundle_name(dataset: dict) -> str:
    """Filename of the bundle produced by archive_dataset.

    Distinct from get_bundle_name() which is the staged copy's filename.
    """
    return f"{dataset['name']}{_BUNDLE_EXTENSION}"


def get_bundle_staged_path(dataset: dict) -> str:
    """Path of the bundle downloaded into bundle/stage during stage_dataset."""
    return f'{config["paths"][dataset["type"]]["bundle"]["stage"]}/{get_bundle_name(dataset)}'


def get_bundle_name(dataset: dict) -> str:
    """Filename of the staged bundle: {name}.{type}{_BUNDLE_EXTENSION}"""
    return f"{dataset['name']}.{dataset['type']}{_BUNDLE_EXTENSION}"


def get_dataset_download_path(dataset: dict) -> Path:
    """Symlink created by setup_dataset_download for the staged dataset directory."""
    alias = glom(dataset, 'metadata.stage_alias')
    return Path(config['paths']['download_dir']).resolve() / alias


def get_bundle_download_path(dataset: dict) -> Path:
    """Symlink created by setup_dataset_download for the staged bundle."""
    return Path(config['paths']['download_dir']).resolve() / get_bundle_name(dataset)


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

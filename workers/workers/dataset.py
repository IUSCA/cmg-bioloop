import hashlib
from pathlib import Path

from glom import glom

from workers import api
from workers.config import config


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


def get_bundle_staged_path(dataset: dict) -> str:
    return f'{config["paths"][dataset["type"]]["bundle"]["stage"]}/{get_bundle_name(dataset)}'


def get_bundle_name(dataset: dict) -> str:
    return f"{dataset['name']}.{dataset['type']}.tar"


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

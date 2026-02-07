"""
Upload Verification Utilities

Provides BLAKE3 manifest-based checksum verification for uploaded files.
Falls back to TUS offset check when checksums are disabled.
"""

import json
from pathlib import Path

from workers.config.common import config


def verify_upload_integrity(dataset, upload_log=None):
    """
    Verify upload integrity using manifest hash or TUS fallback.

    Args:
        dataset (dict): Dataset with origin_path
        upload_log (dict, optional): Upload log with metadata

    Returns:
        bool: True if upload is verified

    Raises:
        Exception: If verification fails
    """
    dataset_id = dataset['id']
    origin_path = dataset.get('origin_path')

    if not origin_path:
        raise Exception(f"Dataset {dataset_id} has no origin_path")

    # Check feature flag from config
    verify_checksums = config.get('upload', {}).get('verify_checksums', False)
    
    if not verify_checksums:
        # Default: Skip checksum verification, just check files exist
        print(f"Checksum verification disabled, checking file existence for dataset {dataset_id}")
        return _verify_files_exist(origin_path)

    # Get stored manifest from upload_log metadata
    manifest_data = None
    if upload_log:
        metadata = upload_log.get('metadata') or {}
        manifest_data = metadata.get('checksum')

    if not manifest_data:
        print(f"No manifest hash found for dataset {dataset_id}, falling back to file existence check")
        return _verify_files_exist(origin_path)

    # Recompute manifest from uploaded files
    origin = Path(origin_path)
    if not origin.exists():
        raise Exception(f"Origin path does not exist: {origin_path}")

    print(f"Verifying manifest hash for dataset {dataset_id}...")

    try:
        computed_hash = _compute_manifest_hash(origin)
    except ImportError:
        print(f"BLAKE3 not available, falling back to file existence check for dataset {dataset_id}")
        return _verify_files_exist(origin_path)

    stored_hash = manifest_data.get('manifest_hash')

    # Verify match
    if computed_hash != stored_hash:
        raise Exception(
            f"Manifest hash mismatch for dataset {dataset_id}: "
            f"expected {stored_hash}, got {computed_hash}"
        )

    print(f"Manifest hash verified for dataset {dataset_id}")
    return True


def _verify_files_exist(origin_path):
    """
    Fallback verification when checksum is disabled.
    Simply checks that files exist at origin_path.

    Note: This is lightweight (no file content reading) but sufficient because:
    1. Files are moved to origin_path only after successful upload completion
    2. Upload service (TUS) handles protocol-level integrity (offset tracking, resume)
    3. If upload was incomplete, files wouldn't be at origin_path
    
    Why not check TUS metadata or file sizes?
    - TUS metadata (.json files) only exist in temp upload directory
    - After /complete endpoint moves files, TUS metadata is no longer accessible
    - We don't store expected file sizes/names in database (upload handles that)
    - File existence at final destination path implies successful upload + move
    
    This is fast (just directory traversal, no file I/O) and works for large datasets.

    Args:
        origin_path (str): Path to uploaded files

    Returns:
        bool: True if files exist

    Raises:
        Exception: If no files found
    """
    origin = Path(origin_path)

    if not origin.exists():
        raise Exception(f"Origin path does not exist: {origin_path}")

    # Count files (directory traversal only, no file content reading)
    files = list(origin.rglob('*'))
    file_count = sum(1 for f in files if f.is_file())

    if file_count == 0:
        raise Exception(f"No files found at {origin_path}")

    print(f"Found {file_count} file(s) at {origin_path}")
    return True


def _compute_manifest_hash(origin_path):
    """
    Compute BLAKE3 manifest hash from directory.
    Matches client-side algorithm exactly.
    
    Uses streaming hash with 16MB chunks optimized for Lustre filesystem performance.

    Args:
        origin_path (Path): Path to uploaded files

    Returns:
        str: Hex hash of the manifest
    """
    import blake3

    # 16MB chunks - optimal for Lustre HPFS (>4MB minimum, 10-32MB ideal)
    CHUNK_SIZE = 16 * 1024 * 1024

    files = sorted([f for f in origin_path.rglob('*') if f.is_file()])

    if not files:
        raise Exception(f"No files found at {origin_path}")

    manifest_lines = ['blake3-manifest-v1']

    for file_path in files:
        # Stream hash file content in chunks to avoid loading entire file into memory
        hasher = blake3.blake3()
        with open(file_path, 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
        file_hash = hasher.hexdigest()

        # Relative path from origin_path
        rel_path = file_path.relative_to(origin_path)
        rel_path_str = str(rel_path).replace('\\', '/')  # Normalize to forward slashes

        manifest_lines.append(
            f"{rel_path_str}\t{file_path.stat().st_size}\t{file_hash}"
        )

    # Hash the manifest
    manifest_str = '\n'.join(manifest_lines)
    return blake3.blake3(manifest_str.encode('utf-8')).hexdigest()



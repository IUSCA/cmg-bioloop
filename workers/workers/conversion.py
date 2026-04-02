import hashlib
from pathlib import Path

from glom import glom

from workers import api
from workers.config import config


def get_conversion_run_dir(conversion: dict) -> Path:
    conversions_output_dir = Path(conversion['definition']['output_directory'])
    conversion_run_dir = conversions_output_dir / f'{conversion["id"]}'
    return conversion_run_dir


def get_conversion_output_dir(conversion: dict) -> Path:
    conversion_run_dir = get_conversion_run_dir(conversion)
    conversion_output_dir = conversion_run_dir / f'{conversion["dataset"]["name"]}'
    return conversion_output_dir

def get_genomic_qc_output_dir(conversion: dict) -> Path:
    conversion_output_dir = get_conversion_output_dir(conversion)
    genomic_qc_output_dir = conversion_output_dir / 'qc' / 'fastqc'
    return genomic_qc_output_dir


def get_conversion_qc_reports_dir(conversion: dict, dataset_name: str) -> Path:
    """
    Returns the unified QC reports directory for a conversion + data product.
    Used for both legacy (bigbang-copied) and new conversion QC reports.

    Path: <qc_reports_base>/<conversion_identifier>/<dataset_name>/
    - Legacy conversions: <conversion_identifier> = cmg_id
    - New conversions: <conversion_identifier> = conversion.id
    """
    qc_reports_base = Path(config['paths']['conversion']['qc_reports'])
    conversion_identifier = conversion.get('cmg_id') or str(conversion['id'])
    return qc_reports_base / conversion_identifier / dataset_name

# def setup_reports_access(reports_dir: Path):
#   # todo - docker/dev modes?
#   if config['mode'] != 'production':
#     return
#   else:
#     reports_symlink_path = Path(config['paths']['conversion']['reports_access'])
#     reports_symlink_path.symlink_to(reports_dir, target_is_directory=True)

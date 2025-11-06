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

# def setup_reports_access(reports_dir: Path):
#   # todo - docker/dev modes?
#   if config['mode'] != 'production':
#     return
#   else:
#     reports_symlink_path = Path(config['paths']['conversion']['reports_access'])
#     reports_symlink_path.symlink_to(reports_dir, target_is_directory=True)

from __future__ import annotations

import shutil
from pathlib import Path

from celery import Celery

import workers.api as api
import workers.config.celeryconfig as celeryconfig
from workers.config import config
from workers.conversion import get_conversion_output_dir

app = Celery("tasks")
app.config_from_object(celeryconfig)


def copy(celery_task, dataset_id_conversion_id, **kwargs):
    conversion = api.get_conversion(conversion_id=dataset_id_conversion_id['conversion_id'], include_dataset=True)
    dataset = api.get_dataset(dataset_id=dataset_id_conversion_id['dataset_id'])
    
    conversion_output_dir = get_conversion_output_dir(conversion)
    print(f"conversion_output_dir: {conversion_output_dir}")

    reports_target_dir = Path(config['paths']['conversion']['reports']) / str(conversion['id']) / dataset['name']

    if reports_target_dir.exists():
        print(f"Found existing reports_target_dir: {reports_target_dir}")
        shutil.rmtree(reports_target_dir)
        print(f"Removed reports_target_dir: {reports_target_dir}")
    reports_target_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created reports_target_dir: {reports_target_dir}")

    src_reports = conversion_output_dir / 'Reports'
    dst_reports = reports_target_dir / 'Reports'

    # Perform any operations that may be needed to grant users access to the reports directory.
    # setup_reports_access(reports_dir=dst_reports)

    if src_reports.exists():
        shutil.copytree(src_reports, dst_reports)
        print(f"Copied reports from {src_reports} to {dst_reports}")
    else:
        print(f"No reports found in {src_reports}")

    return {'dataset_id': dataset['id'], 'conversion_id': conversion['id']},

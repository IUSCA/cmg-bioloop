from __future__ import annotations

import os
from pathlib import Path

from celery import Celery
from requests import get

import workers.api as api
import workers.config.celeryconfig as celeryconfig
from workers.config import config
from workers.conversion import (get_conversion_output_dir,
                                get_genomic_qc_output_dir)
from workers.tasks.qc import create_report

app = Celery("tasks")
app.config_from_object(celeryconfig)


def generate_qc(celery_task, dataset_id_conversion_id, **kwargs):
    # Check if QC is enabled in configuration
    qc_enabled = config.get('genomic_conversion', {}).get('qc', {}).get('enabled', True)
    if not qc_enabled:
        print("QC generation is disabled in configuration, skipping...")
        return {'dataset_id': dataset_id_conversion_id['dataset_id'], 
                'conversion_id': dataset_id_conversion_id['conversion_id']},
    
    conversion: dict = api.get_conversion(conversion_id=dataset_id_conversion_id['conversion_id'], include_dataset=True)
    dataset: dict = api.get_dataset(dataset_id=dataset_id_conversion_id['dataset_id'])

    conversion_output_dir: Path = get_conversion_output_dir(conversion)
    print(f"conversion_output_dir: {conversion_output_dir}")

    if not conversion_output_dir.exists():
        raise Exception(f"Conversion output directory does not exist: {conversion_output_dir}")
    
    print("--------------------------------")
    print("contents of conversion_output_dir:")
    for item in conversion_output_dir.iterdir():
        print(f"  {item}")
    print("--------------------------------")

    qc_source_dirs: list[Path] = [d for d in conversion_output_dir.iterdir() if
                     not d.is_dir() and
                     '_' in d.name and
                     d.name not in {'Reports', 'Stats'}]
    
    print(f"qc_source_dir: {qc_source_dirs}")
    print("contents of qc_source_dir:")
    print("--------------------------------")
    print("contents of qc_source_dirs:")
    for item in qc_source_dirs:
        print(f"  {item}")
    print("--------------------------------")

    qc_output_dir: Path = get_genomic_qc_output_dir(conversion)
    print(f"qc_target_dir: {qc_output_dir}")
    qc_output_dir.mkdir(parents=True, exist_ok=True)

    print("--------------------------------")
    print("contents of qc_target_dir:")
    for item in qc_output_dir.iterdir():
        print(f"  {item}")
    print("--------------------------------")
                
    report_id = create_report(
        celery_task=celery_task,
        dataset_dir=conversion_output_dir,
        dataset_qc_dir=qc_output_dir,
        report_id=(dataset.get('metadata', {}) or {}).get('report_id', None)
    )

    print("--------------------------------")
    print("contents of qc_source_dir:")
    for item in qc_source_dirs:
        print(f"  {item}")
    print("--------------------------------")

    print("--------------------------------")
    print("contents of qc_target_dir:")
    for item in qc_output_dir.iterdir():
        print(f"  {item}")
    print("--------------------------------")

    report_filename = qc_output_dir / 'multiqc_report.html'

    # if the report is created successfully
    if report_filename.exists():
        print("--------------------------------")
        print("report_filename exists")
        print("--------------------------------")
        update_data = {
            'metadata': {
                'report_id': report_id
            }
        }
        api.update_dataset(dataset_id=conversion['dataset_id'], update_data=update_data)
        api.upload_report(dataset_id=conversion['dataset_id'], report_filename=report_filename)
        api.add_state_to_dataset(dataset_id=conversion['dataset_id'], state='QC')
    else:
        print("--------------------------------")
        print("report_filename does not exist")
        print("--------------------------------")
        print(f"QC report not found for directory {qc_source_dirs}")
        print("--------------------------------")

    return {'dataset_id': dataset['id'], 'conversion_id': conversion['id']},

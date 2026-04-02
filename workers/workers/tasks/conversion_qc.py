from __future__ import annotations

import os
from pathlib import Path

from celery import Celery
from requests import get

import workers.api as api
import workers.config.celeryconfig as celeryconfig
from workers.config import config
from workers.conversion import (get_conversion_output_dir,
                                get_conversion_qc_reports_dir,
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

    # Filter sample directories: directories with underscore, excluding Reports/Stats
    # Fixed: Changed "not d.is_dir()" to "d.is_dir()" to filter FOR directories
    qc_source_dirs: list[Path] = [d for d in conversion_output_dir.iterdir() if
                     d.is_dir() and
                     '_' in d.name and
                     d.name not in {'Reports', 'Stats'}]
    
    print(f"qc_source_dirs (filtered sample directories): {qc_source_dirs}")
    print("--------------------------------")

    # Base QC output directory: use the unified conversion QC reports dir when configured,
    # falling back to the per-conversion-output dir.
    use_conversion_dirs = (
        config.get('genomic_conversion', {})
        .get('qc', {})
        .get('use_conversion_dirs', False)
    )
    if use_conversion_dirs and config.get('paths', {}).get('conversion', {}).get('qc_reports'):
        qc_base_dir = get_conversion_qc_reports_dir(conversion, dataset['name'])
        print(f"qc_base_dir (unified conversion qc_reports): {qc_base_dir}")
    else:
        qc_base_dir = get_genomic_qc_output_dir(conversion)
        print(f"qc_base_dir (conversion output): {qc_base_dir}")
    qc_base_dir.mkdir(parents=True, exist_ok=True)

    # Run QC on EACH sample directory separately (matches CMG behavior)
    # Each sample gets its own MultiQC report
    report_ids = []
    successful_reports = []
    
    for sample_dir in qc_source_dirs:
        print(f"Running QC for sample: {sample_dir.name}")
        
        # Create per-sample QC output directory
        sample_qc_dir = qc_base_dir / sample_dir.name
        sample_qc_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Run FastQC + MultiQC for this sample only
            report_id = create_report(
                celery_task=celery_task,
                dataset_dir=sample_dir,  # Per-sample directory
                dataset_qc_dir=sample_qc_dir,
                report_id=None  # Each sample gets new report
            )
            
            report_filename = sample_qc_dir / 'multiqc_report.html'
            
            if report_filename.exists():
                print(f"QC report created for {sample_dir.name}: {report_filename}")
                report_ids.append(report_id)
                successful_reports.append(str(report_filename))
            else:
                print(f"Warning: QC report not found for {sample_dir.name}")
                
        except Exception as e:
            print(f"Error running QC for {sample_dir.name}: {e}")
            # Continue with other samples even if one fails

    print("--------------------------------")
    print(f"QC completed for {len(successful_reports)} of {len(qc_source_dirs)} samples")
    print("--------------------------------")

    # Update dataset with QC state if any reports were created
    if successful_reports:
        # Store first report ID (or could store all IDs as array)
        update_data = {
            'metadata': {
                'report_id': report_ids[0] if report_ids else None,
                'qc_reports': successful_reports  # Store all report paths
            }
        }
        api.update_dataset(dataset_id=conversion['dataset_id'], update_data=update_data)
        
        # Upload the first report (or could upload all)
        first_report = Path(successful_reports[0])
        api.upload_report(dataset_id=conversion['dataset_id'], report_filename=first_report)
        api.add_state_to_dataset(dataset_id=conversion['dataset_id'], state='QC')
    else:
        print("--------------------------------")
        print("No QC reports were created successfully")
        print("--------------------------------")

    return {'dataset_id': dataset['id'], 'conversion_id': conversion['id']},

from __future__ import annotations

import uuid
from pathlib import Path

from celery import Celery
from sca_rhythm import WorkflowTask
from sca_rhythm.progress import Progress

import workers.api as api
import workers.cmd as cmd
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
from workers.config import config

app = Celery("tasks")
app.config_from_object(celeryconfig)


def run_fastqc(celery_task: WorkflowTask, source_dir, output_dir):
    """
    Run the FastQC tool to check the quality of all fastq files 
    (.fastq.gz) in the source directory recursively.

    @param celery_task: WorkflowTask
    @param source_dir: (pathlib.Path): The dataset / sequencing run directory
    @param output_dir: (pathlib.Path): where to create the reports (a .zip and .html file)
    @return: None

    """
    NUM_THREADS = 8
    BATCH_SIZE = NUM_THREADS
    fastq_files = [str(p) for p in source_dir.glob('**/*.fastq.gz')]
    prog = Progress(celery_task=celery_task, name='fastqc', total=len(fastq_files), units='items')

    done = 0
    prog.update(done=done)
    for batch in utils.batched(fastq_files, n=BATCH_SIZE):
        cmd.fastqc_parallel(fastq_files=batch, output_dir=output_dir, num_threads=NUM_THREADS)
        done += len(batch)
        prog.update(done=done)


def create_report(celery_task: WorkflowTask, dataset_dir: Path, dataset_qc_dir: Path, report_id: str = None) -> str:
    """
    Runs fastqc and multiqc on dataset files. The qc files are placed in dataset_qc_dir

    @param celery_task: WorkflowTask
    @param dataset_dir: (Path): Staged dataset directory path
    @param dataset_qc_dir: (Path): directory to generate the qc reports in
    @param report_id: (str): report_id of the last generated report to be reused. (optional)
    @return: The report ID (UUID4)

    """
    report_id = report_id or str(uuid.uuid4())
    dataset_qc_dir.mkdir(parents=True, exist_ok=True)

    run_fastqc(celery_task, dataset_dir, dataset_qc_dir)
    cmd.multiqc(dataset_qc_dir, dataset_qc_dir)

    return report_id


def _generate_qc(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id)
    dataset_type = dataset['type']
    dataset_qc_dir = Path(config['paths'][dataset_type]['qc']) / dataset['name'] / 'qc'
    staged_path = Path(dataset['staged_path'])

    # todo: ensure fastqc is being run at the same path in CMG
    report_id = create_report(
        celery_task=celery_task,
        dataset_dir=staged_path,
        dataset_qc_dir=dataset_qc_dir,
        report_id=(dataset.get('metadata', {}) or {}).get('report_id', None)
    )

    report_filename = dataset_qc_dir / 'multiqc_report.html'

    # if the report is created successfully
    if report_filename.exists():
        update_data = {
            'metadata': {
                'report_id': report_id
            }
        }
        api.update_dataset(dataset_id=dataset_id, update_data=update_data)
        api.upload_report(dataset_id=dataset_id, report_filename=report_filename)
        api.add_state_to_dataset(dataset_id=dataset_id, state='QC')
    else:
        pass
        # TODO: fail the task if there is no report?
        # nonRetryable exception

    return dataset_id,


def generate_qc(celery_task, dataset_id, **kwargs):
    """
    Conditional QC wrapper - only runs QC for eligible datasets.
    
    Matches CMG behavior: QC runs for DATA_PRODUCT datasets with analysis_type 'FASTQ'.
    
    This is the unified QC step used in the integrated workflow for all dataproducts
    (imports, conversions, etc.). Simpler architecture than CMG's separate workers.
    """
    dataset = api.get_dataset(dataset_id=dataset_id)
    dataset_type = dataset.get('type', '')
    
    # Check dataset type - only DATA_PRODUCT needs QC
    if dataset_type != 'DATA_PRODUCT':
        print(f"Dataset {dataset_id} type '{dataset_type}' does not need QC (not a DATA_PRODUCT), skipping...")
        return dataset_id,
    
    # Check analysis_type - only FASTQ needs QC (matches CMG: if file_type == 'fastq')
    # analysis_type is a relation: { id, name, extension }
    analysis_type = dataset.get('analysis_type')
    if not analysis_type:
        print(f"Dataset {dataset_id} has no analysis_type, skipping QC...")
        return dataset_id,
    
    analysis_type_name = analysis_type.get('name', '').upper()
    
    # Check if analysis type is FASTQ (case-insensitive)
    if analysis_type_name != 'FASTQ':
        print(f"Dataset {dataset_id} analysis_type '{analysis_type_name}' does not need QC (not FASTQ), skipping...")
        return dataset_id,
    
    # Run QC for DATA_PRODUCT with analysis_type 'FASTQ'
    print(f"Running QC for DATA_PRODUCT dataset {dataset_id} (analysis_type: {analysis_type_name})")
    return _generate_qc(celery_task, dataset_id, **kwargs)

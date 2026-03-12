import logging
import uuid
from pathlib import Path

from workers.legacy_migration.xenium import is_legacy_xenium_dataset

logger = logging.getLogger(__name__)


def upload_static_content(celery_task, dataset_id: int, **kwargs):
    """
    Upload analysis_summary.html from staged_path to the API report endpoint.

    Args:
        dataset_id: The Bioloop dataset ID
    """
    import workers.api as api

    dataset = api.get_dataset(dataset_id=dataset_id)
    dataset_name = dataset.get('name', str(dataset_id))

    if not is_legacy_xenium_dataset(dataset):
        # Non-Xenium datasets: skip gracefully.
        logger.info(f'{dataset_name} - not a xenium dataset, skipping upload_static_content')
        return

    staged_path = dataset.get('staged_path')
    if not staged_path:
        logger.info(f'{dataset_name} - no staged_path, skipping upload_static_content')
        return

    analysis_html = Path(staged_path) / 'analysis_summary.html'
    if not analysis_html.exists():
        logger.info(f'{dataset_name} - analysis_summary.html not found at {analysis_html}, skipping')
        return

    metadata = dict(dataset.get('metadata') or {})
    if not metadata.get('analysis_summary_file_dir'):
        analysis_summary_file_dir = str(uuid.uuid4())
        metadata['analysis_summary_file_dir'] = analysis_summary_file_dir
        api.update_dataset(dataset_id=dataset_id, update_data={'metadata': metadata})
        logger.info(f'{dataset_name} - initialized analysis_summary_file_dir={analysis_summary_file_dir}')

    api.upload_report(dataset_id=dataset_id, report_filename=analysis_html)
    logger.info(f'{dataset_name} - uploaded analysis_summary.html to API report endpoint')

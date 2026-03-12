import logging
from pathlib import Path

from workers.constants.workflow import WORKFLOWS
from workers.config import config

logger = logging.getLogger(__name__)


def _is_legacy_source_active(source_name: str) -> bool:
    cfg = config.get('legacy_application_active', False)
    if isinstance(cfg, bool):
        return cfg
    if isinstance(cfg, dict):
        return bool(cfg.get(source_name, False))
    return False


def _build_dataproduct_name(parent_dataset_name: str, subdir_name: str) -> str:
    return f'{parent_dataset_name}--{subdir_name}'


def initiate_subdir_workflows(celery_task, dataset_id: int, **kwargs):
    """
    Register each subdirectory of a Xenium RAW_DATA dataset as a DATA_PRODUCT
    and launch intake_integrated on each.

    Args:
        dataset_id: The Bioloop dataset ID of the RAW_DATA parent dataset
    """
    import workers.api as api

    dataset = api.get_dataset(dataset_id=dataset_id)
    dataset_name = dataset.get('name', str(dataset_id))
    origin_path = dataset.get('origin_path')
    if not origin_path:
        raise ValueError(f'{dataset_name} - origin_path is not set')

    raw_dir = Path(origin_path)
    if not raw_dir.is_dir():
        raise ValueError(f'{dataset_name} - origin_path does not exist: {raw_dir}')

    subdirs = sorted([d for d in raw_dir.iterdir() if d.is_dir()], key=lambda p: p.name)
    logger.info(f'{dataset_name} - found {len(subdirs)} subdirectories under {raw_dir}')

    legacy_xenium_active = _is_legacy_source_active('xenium')
    metadata_origin = 'legacy_xenium' if legacy_xenium_active else 'bioloop'

    launched_count = 0
    for subdir in subdirs:
        child_name = _build_dataproduct_name(dataset_name, subdir.name)
        child_origin_path = str(subdir.resolve())
        child_dataset = None

        existing_with_name = api.get_all_datasets(
            dataset_type='DATA_PRODUCT',
            name=child_name,
            deleted=False,
            match_name_exact=True,
        )
        for candidate in existing_with_name:
            if candidate.get('origin_path') == child_origin_path:
                child_dataset = candidate
                break

        if child_dataset is None:
            payload = {
                'name': child_name,
                'type': 'DATA_PRODUCT',
                'origin_path': child_origin_path,
                'metadata': {
                    'origin': metadata_origin,
                    'parent_raw_dataset_id': dataset_id,
                    'parent_raw_dataset_name': dataset_name,
                },
            }
            try:
                child_dataset = api.create_dataset(payload)
                logger.info(f'{dataset_name} - registered DATA_PRODUCT {child_name} ({child_dataset["id"]})')
            except api.DatasetAlreadyExistsError:
                refreshed = api.get_all_datasets(
                    dataset_type='DATA_PRODUCT',
                    name=child_name,
                    deleted=False,
                    match_name_exact=True,
                )
                child_dataset = next(
                    (d for d in refreshed if d.get('origin_path') == child_origin_path),
                    None
                )

        if child_dataset is None:
            logger.warning(f'{dataset_name} - could not resolve DATA_PRODUCT for subdir {subdir}')
            continue

        api.create_dataset_hierarchy([{
            'source_id': dataset_id,
            'derived_id': child_dataset['id'],
        }])

        try:
            api.initiate_workflow(child_dataset['id'], WORKFLOWS['INTAKE_INTEGRATED'])
            launched_count += 1
        except Exception as error:
            logger.warning(
                f'{dataset_name} - failed to launch intake_integrated for child {child_dataset["id"]}: {error}'
            )

    logger.info(f'{dataset_name} - initiate_subdir_workflows complete (launched={launched_count})')

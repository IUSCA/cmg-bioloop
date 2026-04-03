import logging
from pathlib import Path

from celery import current_app
from sca_rhythm import Workflow

import workers.api as api
import workers.workflow_utils as wf_utils
from workers.api import DatasetAlreadyExistsError

logger = logging.getLogger(__name__)


def register_dataproduct(raw_data, candidate: Path):
    dataset_type = 'DATA_PRODUCT'
    logger.info(f'registering {dataset_type} dataset - {candidate.name}')
    dataset = {
        'name': candidate.name,
        'type': dataset_type,
        'origin_path': str(candidate.resolve()),
    }
    try:
        data_product = api.create_dataset(dataset)
        data_product_id = data_product['id']

        # create association between raw data and data products
        associations = [
            {
                'source_id': raw_data['id'],
                'derived_id': data_product_id
            }
        ]
        api.add_associations(associations=associations)

        # launch workflow for created dataset
        integrated_wf_body = wf_utils.get_wf_body(wf_name='integrated')
        int_wf = Workflow(celery_app=current_app, **integrated_wf_body)
        api.add_workflow_to_dataset(dataset_id=data_product_id, workflow_id=int_wf.workflow['_id'])
        int_wf.start(data_product_id)
    except DatasetAlreadyExistsError as e:
        logger.error(f'Unable to register subdirectory. Dataset already exists: {e}')


def initiate_subdir_workflows(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id)
    origin_path = Path(dataset['origin_path'])

    sub_dirs = [f for f in origin_path.iterdir() if f.is_dir()]

    for d in sub_dirs:
        register_dataproduct(dataset, d)

    return dataset_id,

from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.config.celeryconfig as celeryconfig

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def begin_migration(celery_task, dataset_id, **kwargs):
    """
    Begin migration process for legacy CMG datasets.
    Sets MIGRATION_INITIATED state and prepares dataset for hydration.
    
    Args:
        celery_task: WorkflowTask instance
        dataset_id: ID of the dataset to migrate
    
    Returns:
        dataset_id: For passing to next workflow step
    """
    logger.info(f'Beginning migration for dataset {dataset_id}')
    
    # Set state to indicate migration has begun
    api.add_state_to_dataset(dataset_id=dataset_id, state='MIGRATION_INITIATED')
    
    logger.info(f'Migration initiated for dataset {dataset_id}')
    return dataset_id,


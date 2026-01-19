from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.config.celeryconfig as celeryconfig

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def end_migration(celery_task, dataset_id, **kwargs):
    """
    End migration process for legacy CMG datasets.
    Sets MIGRATED state to indicate successful completion of all migration steps.
    
    This is the final step in the stage_migrated workflow and indicates that:
    - Dataset has been retrieved from archive
    - Metadata (bundle info, dataset_files, tracks) has been populated
    - Dataset has been staged and validated
    - Download access has been configured
    
    Args:
        celery_task: WorkflowTask instance
        dataset_id: ID of the dataset that was migrated
    
    Returns:
        dataset_id: For workflow completion
    """
    logger.info(f'Completing migration for dataset {dataset_id}')
    
    # Set final state to indicate migration is complete
    api.add_state_to_dataset(dataset_id=dataset_id, state='MIGRATED')
    
    logger.info(f'Migration completed successfully for dataset {dataset_id}')
    return dataset_id,


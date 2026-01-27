from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.config.celeryconfig as celeryconfig

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def finish_session_hydration(celery_task, session_id, **kwargs):
    """
    Complete session hydration by setting metadata.is_hydrated to True.
    
    Args:
        celery_task: WorkflowTask instance
        session_id: ID of the session that has been hydrated
    
    Returns:
        session_id: For workflow completion
    """
    logger.info(f'Finishing hydration for session {session_id}')
    
    # Update session metadata to mark as hydrated
    try:
        api.update_session(session_id, {
            'metadata': {
                'is_hydrated': True
            }
        })
        logger.info(f'Successfully marked session {session_id} as hydrated')
    except Exception as e:
        logger.error(f'Failed to mark session {session_id} as hydrated: {e}')
        raise
    
    return session_id,

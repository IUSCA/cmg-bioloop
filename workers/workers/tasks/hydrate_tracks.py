from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.cmg_api as cmg_api
import workers.config.celeryconfig as celeryconfig

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def hydrate_session_tracks(celery_task, session_id, **kwargs):
    """
    Hydrate session tracks from CMG API.
    
    This task:
    1. Retrieves legacy track info from CMG API using session's cmg_id
    2. Finds corresponding dataset_file objects in Bioloop database
    3. Finds existing track records for those files
    4. Associates tracks with the session via session_tracks table
    
    Note: This assumes tracks already exist in the track table.
    
    Args:
        celery_task: WorkflowTask instance
        session_id: ID of the session to hydrate (Bioloop session ID)
    
    Returns:
        session_id: For passing to next workflow step
    """
    logger.info(f'Hydrating tracks for session {session_id}')
    
    # Get the session to retrieve cmg_id
    session = api.get_session(session_id)
    cmg_session_id = session.get('cmg_id')
    
    if not cmg_session_id:
        logger.error(f'Session {session_id} has no cmg_id - cannot hydrate')
        raise ValueError(f'Session {session_id} is not a legacy CMG session')
    
    logger.info(f'Session {session_id} has CMG ID: {cmg_session_id}')
    
    # Retrieve CMG session data including tracks
    try:
        cmg_session_data = cmg_api.get_session(cmg_session_id,
                                               use_auth=False)
    except Exception as e:
        logger.error(f'Failed to retrieve CMG session {cmg_session_id}: {e}')
        raise
    
    cmg_tracks = cmg_session_data.get('tracks', [])
    logger.info(f'Retrieved {len(cmg_tracks)} tracks from CMG API')
    
    if not cmg_tracks:
        logger.warning(f'No tracks found in CMG session {cmg_session_id}')
        return session_id,
    
    # Process each track
    track_ids_to_associate = []
    errors = []
    
    for cmg_track in cmg_tracks:
        filename = cmg_track.get('filename')
        cmg_dataproduct_id = cmg_track.get('dataproduct')
        
        if not filename or not cmg_dataproduct_id:
            logger.warning(f'Skipping track with missing filename or dataproduct: {cmg_track}')
            continue
        
        logger.info(f'Processing track: filename={filename}, cmg_dataproduct_id={cmg_dataproduct_id}')
        
        # Find the Bioloop dataset that corresponds to this CMG dataproduct
        # CMG dataproduct ID is stored as cmg_id in Bioloop dataset table
        dataset = api.get_dataset_by_cmg_id(cmg_dataproduct_id)
        
        if not dataset:
            msg = f'No dataset found for CMG dataproduct {cmg_dataproduct_id}'
            logger.warning(msg)
            errors.append((filename, msg))
            continue
        
        dataset_id = dataset['id']
        logger.info(f'Found dataset {dataset_id} for CMG dataproduct {cmg_dataproduct_id}')
        
        # Find the dataset_file with matching filename in this dataset
        dataset_file = api.get_dataset_file_by_name_and_dataset(filename, dataset_id)
        
        if not dataset_file:
            msg = f'No dataset_file found for filename {filename} in dataset {dataset_id}'
            logger.warning(msg)
            errors.append((filename, msg))
            continue
        
        dataset_file_id = dataset_file['id']
        logger.info(f'Found dataset_file {dataset_file_id} for filename {filename}')
        
        # Find the track for this dataset_file (must already exist)
        track = api.get_track_by_dataset_file_id(dataset_file_id)
        
        if not track:
            msg = f'No track found for dataset_file {dataset_file_id} - tracks must exist before hydration'
            logger.warning(msg)
            errors.append((filename, msg))
            continue
        
        track_id = track['id']
        logger.info(f'Found track {track_id} for dataset_file {dataset_file_id}')
        
        track_ids_to_associate.append(track_id)
    
    logger.info(f'Found {len(track_ids_to_associate)} tracks to associate with session {session_id}')
    
    # If we had errors processing ALL tracks, fail the task
    if errors and not track_ids_to_associate:
        error_summary = '\n'.join([f'  - {filename}: {msg}' for filename, msg in errors])
        raise ValueError(f'Failed to process all {len(cmg_tracks)} tracks for session {session_id}:\n{error_summary}')
    
    # If we had partial errors, log them but continue
    if errors:
        error_summary = '\n'.join([f'  - {filename}: {msg}' for filename, msg in errors])
        logger.warning(f'Failed to process {len(errors)} of {len(cmg_tracks)} tracks:\n{error_summary}')
    
    # Associate all tracks with the session
    if track_ids_to_associate:
        api.update_session_tracks(session_id, track_ids_to_associate)
        logger.info(f'Successfully associated {len(track_ids_to_associate)} tracks with session {session_id}')
    else:
        logger.warning(f'No tracks to associate with session {session_id}')
    
    logger.info(f'Completed track hydration for session {session_id}')
    
    # Manually trigger next step since session workflows aren't auto-orchestrated
    from workers.tasks.declarations import finish_session_hydration
    logger.info(f'Triggering finish_session_hydration for session {session_id}')
    finish_session_hydration.delay(session_id)
    
    return session_id,

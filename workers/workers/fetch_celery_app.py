from celery import Celery
from sca_rhythm import WorkflowTask

import workers.config.celeryconfig as celeryconfig
from workers import exceptions as exc

app = Celery("tasks")
app.config_from_object(celeryconfig)


@app.task(base=WorkflowTask, bind=True, name='stage_dataset',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def stage_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.stage import stage_dataset as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='validate_dataset',
          autoretry_for=(exc.RetryableException,),
          max_retries=3,
          default_retry_delay=5)
def validate_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.validate import validate_dataset as task_body
    try:
        return task_body(celery_task, dataset_id, **kwargs)
    except exc.ValidationFailed:
        raise
    except Exception as e:
        raise exc.RetryableException(e)


@app.task(base=WorkflowTask, bind=True, name='setup_dataset_download',
          autoretry_for=(exc.RetryableException,),
          max_retries=3,
          default_retry_delay=5)
def setup_dataset_download(celery_task, dataset_id, **kwargs):
    from workers.tasks.download import setup_download as task_body
    try:
        return task_body(celery_task, dataset_id, **kwargs)
    except exc.ValidationFailed:
        raise
    except Exception as e:
        raise exc.RetryableException(e)


@app.task(base=WorkflowTask, bind=True, name='download_illumina_dataset',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def download_illumina_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.illumina_download import \
        download_illumina_dataset as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='generate_qc',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def generate_qc(celery_task, dataset_id, **kwargs):
    from workers.tasks.qc import generate_qc as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='populate_file_metadata',
          autoretry_for=(exc.RetryableException,),
          max_retries=3,
          default_retry_delay=5
          )
def populate_file_metadata(celery_task, dataset_id, **kwargs):
    from workers.tasks.populate_file_metadata import \
        populate_file_metadata as task_body
    try:
        return task_body(celery_task, dataset_id, **kwargs)
    except exc.InspectionFailed:
        raise
    except Exception as e:
        raise exc.RetryableException(e)


# Legacy Migration Tasks

@app.task(base=WorkflowTask, bind=True, name='begin_migration',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def begin_migration(celery_task, dataset_id, **kwargs):
    from workers.tasks.begin_migration import begin_migration as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='retrieve_archive_dataset',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def retrieve_archive_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.retrieve_archive import \
        retrieve_archive_dataset as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='populate_metadata_dataset',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def populate_metadata_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.populate_metadata import \
        populate_metadata_dataset as task_body
    return task_body(celery_task, dataset_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='end_migration',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5)
def end_migration(celery_task, dataset_id, **kwargs):
    from workers.tasks.end_migration import end_migration as task_body
    return task_body(celery_task, dataset_id, **kwargs)


# Session Hydration Tasks

@app.task(base=WorkflowTask, bind=True, name='hydrate_session_tracks',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5
          )
def hydrate_session_tracks(celery_task, session_id, **kwargs):
    from workers.tasks.hydrate_tracks import \
        hydrate_session_tracks as task_body
    return task_body(celery_task, session_id, **kwargs)


@app.task(base=WorkflowTask, bind=True, name='finish_session_hydration',
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=5
          )
def finish_session_hydration(celery_task, session_id, **kwargs):
    from workers.tasks.finish_hydration import \
        finish_session_hydration as task_body
    return task_body(celery_task, session_id, **kwargs)

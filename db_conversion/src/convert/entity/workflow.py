import logging
from datetime import datetime

# from bson.int32 import Int32
# Removed BSON import - using regular Python int
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_bioloop_dataset
from ..constants.common import app_id

logger = logging.getLogger(__name__)

stage_wf_steps = [
      {'name': 'stage', 'task': 'stage_dataset'},
      {'name': 'validate', 'task': 'validate_dataset'},
      {'name': 'setup_download', 'task': 'setup_dataset_download'}
    ]


def get_step_done_time(cmg_dataproduct: dict, step: str) -> datetime:
  logger.info(f"Getting step done time for step {step} with cmg_dataproduct {cmg_dataproduct['_id']}")
  for event in cmg_dataproduct['events']:
    logger.info(f"Event: {event}")
    if event['description'] == f"{step} - finish":
      logger.info(f"Found step done time for step {step} with cmg_dataproduct {cmg_dataproduct['_id']}: {event['stamp']}")
      return event['stamp']
  logger.info(f"Step {step} not found in events for {cmg_dataproduct['_id']}")
  return None
  # raise ValueError(f"Step {step} not found in events for {cmg_dataproduct['_id']}")


def create_celery_task(
  rhythm_db: Database,
  step: dict,
  dataset_id: int,
  workflow_id: str,
  previous_task_id: str,
  cmg_dataproduct: dict
  ) -> str:
  """Create a celery_taskmeta entry and return the task_id"""
  
  logger.info(f"Creating celery task for step {step['name']} with dataset_id {dataset_id}")
  logger.info(f"Previous task id: {previous_task_id}")
  logger.info(f"Workflow id: {workflow_id}")
  logger.info(f"Cmg dataproduct: {cmg_dataproduct['_id']}")

  celery_taskmeta = {
    "status": "SUCCESS",
    "result": f"[{dataset_id}]" if step['task'] != 'validate_dataset' else f"[{dataset_id}, []]",
    "traceback": None,
    "children": [],
    "date_done": get_step_done_time(cmg_dataproduct, step['name']),
    "name": step['task'],
    # "args": [Int32(dataset_id)], # Using regular Python int - PyMongo handles conversion
    "args": [dataset_id], # Using regular Python int - PyMongo handles conversion
    "kwargs": {
      "workflow_id": str(workflow_id),
      "step": step['name'],
      "app_id": app_id
    },
    "worker": "cmg-test-celery-w1@mmge2.carbonate.uits.iu.edu",
    "retries": 0,
    "queue": "cmg-test.sca.iu.edu.q",
    "parent_id": None if previous_task_id is None else str(previous_task_id)
  }

  task_result = rhythm_db.celery_taskmeta.insert_one(celery_taskmeta)
  logger.info(f"Created celery task for step {step['name']} with dataset_id {dataset_id}: {task_result.inserted_id}")
  return task_result.inserted_id


def create_workflows_for_past_stagings(
  pg_cursor: cursor,
  mongo_db: Database,
  rhythm_db: Database,
):
  logger.info(f"Creating workflows for past stagings")
  for cmg_dataproduct in mongo_db.dataproducts.find():
    logger.info(f"Creating staging record for Dataproduct: {cmg_dataproduct['_id']}")
    dataset = find_corresponding_bioloop_dataset(pg_cursor, cmg_dataproduct['_id'])
    logger.info(f"Found corresponding dataset - ID: {dataset['id']}, name: {dataset['name']}")
    dataset_id = dataset['id']

    # Create workflow_meta document
    workflow_meta = {
      "_status": "SUCCESS",
      "app_id": app_id,
      "created_at": datetime.utcnow(),
      "description": None,
      "name": "stage",
      "steps": [],
      "updated_at": datetime.utcnow()
    }

    # Insert a record for the entire workflow
    workflow_result = rhythm_db.workflow_meta.insert_one(workflow_meta)
    workflow_id = workflow_result.inserted_id
    logger.info(f"Created workflow - ID: {workflow_id}")
    pg_cursor.execute(
      "INSERT INTO workflow (id, dataset_id) VALUES (%s, %s)",
      (str(workflow_id), dataset_id)
    )
    logger.info(f"Inserted dataset-workflow association - Workflow ID: {workflow_id}, Dataset ID: {dataset_id} into PostgreSQL")

    previous_task_id = None # The 'parent_id' of a Workflow Step
    workflow_steps = []

    logger.info(f"Creating celery tasks for steps: {stage_wf_steps}")
    for step in stage_wf_steps:
      logger.info(f"Creating celery task for step: {step['task']}")
      # Create celery task and get the task_id
      task_id = create_celery_task(rhythm_db, step, dataset_id, workflow_id, previous_task_id, cmg_dataproduct)

      logger.info(f"Created celery task for step: {step['task']} - ID: {task_id}")

      # Add step to workflow_meta steps array
      workflow_steps.append({
        "name": step['name'],
        "task": step['task'],
        "queue": "cmg-test.sca.iu.edu.q",
        "kwargs": None,
        "task_runs": [{
          "date_start": get_step_done_time(cmg_dataproduct, step['name']),
          "task_id": str(task_id)
        }]
      })
      logger.info(f"Added step to workflow_meta steps array: {workflow_steps}")

      # Store the current task_id for the next iteration
      previous_task_id = task_id

    # Update the workflow_meta document with the steps and updated timestamp
    rhythm_db.workflow_meta.update_one(
      {"_id": workflow_id}, 
      {"$set": {
        "steps": workflow_steps,
        "updated_at": datetime.utcnow()
      }}
    )
    logger.info(f"Updated workflow_meta document with the steps: {workflow_steps}")

  # logger.info(f"Created workflows for {len(archived_datasets)} datasets.")

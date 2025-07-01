from datetime import datetime
from typing import Literal
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..constants.common import app_id


def create_workflows(pg_cursor: cursor, rhythm_db: Database):
  # Fetch all datasets with archive_path from Bioloop
  pg_cursor.execute("SELECT id, archive_path FROM dataset WHERE archive_path IS NOT NULL")
  archived_datasets = pg_cursor.fetchall()

  for dataset in archived_datasets:
    dataset_id, archive_path = dataset

    # Create workflow_meta document
    workflow_meta = {
      "created_at": datetime.utcnow(),
      "updated_at": datetime.utcnow(),
      "steps": []
    }

    # Insert workflow_meta document and get the generated _id
    workflow_result = rhythm_db.workflow_meta.insert_one(workflow_meta)
    workflow_id = workflow_result.inserted_id

    # Insert a record into the PostgreSQL workflow table
    pg_cursor.execute(
      "INSERT INTO workflow (id, dataset_id) VALUES (%s, %s)",
      (str(workflow_id), dataset_id)
    )

    # Registration steps
    registration_steps = ["inspect", "archive", "stage", "validate"]
    previous_task_id = None

    for _, step in enumerate(registration_steps):
      celery_taskmeta = {
        "status": "SUCCESS",
        "result": f"[{dataset_id}]",
        "traceback": None,
        "children": [],
        "date_done": datetime.utcnow().isoformat(),
        "name": f"scaworkers.workers.{step}.{step}_dataset",
        "args": [dataset_id],
        "kwargs": {
          "workflow_id": str(workflow_id),
          "step": step,
          "app_id": app_id
        },
        "worker": "celery@localhost",
        "retries": 0,
        "queue": "celery"
      }

      # Add parent_id for all steps except 'inspect'
      if step != 'inspect':
        celery_taskmeta["parent_id"] = None if previous_task_id is None else str(previous_task_id)

      # Insert celery_taskmeta document and get the generated _id
      task_result = rhythm_db.celery_taskmeta.insert_one(celery_taskmeta)
      task_id = task_result.inserted_id

      # Store the current task_id for the next iteration
      previous_task_id = task_id

      # Add step to workflow_meta
      workflow_meta["steps"].append({
        "name": step,
        "task": f"scaworkers.workers.{step}.{step}_dataset",
        "task_runs": [{
          "date_start": datetime.utcnow(),
          "task_id": str(task_id)
        }]
      })

    # Update the workflow_meta document with the steps
    rhythm_db.workflow_meta.update_one({"_id": workflow_id}, {"$set": {"steps": workflow_meta["steps"]}})

  print(f"Created workflows for {len(archived_datasets)} datasets.")

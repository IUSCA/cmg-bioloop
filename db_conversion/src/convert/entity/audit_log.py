import logging
import trace
import traceback
from typing import Literal

from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_bioloop_dataset
from ..exceptions.exceptions import CMGDatasetNotFoundException
from .user import get_bioloop_cmguser_id

logger = logging.getLogger(__name__)

def events_to_audit_logs(pg_cursor: cursor, mongo_db: Database):
  dataset_events_to_audit_logs(pg_cursor, mongo_db)
  # system_events_to_audit_logs(pg_cursor, mongo_db)


def dataset_events_to_audit_logs(pg_cursor: cursor, mongo_db: Database):
  """
    Convert Events from CMG MongoDB to Audit Logs in Bioloop PostgreSQL.

    This function iterates through Dataset and Dataproduct collections in CMG,
    finds corresponding datasets in BIoloop PostgreSQL, and creates audit log entries
    based on the Events associated with each CMG Dataset.

    Args:
        pg_cursor (cursor): PostgreSQL database cursor
        mongo_db (Database): MongoDB database object

    Returns:
        None
    """
  cmg_user_id = get_bioloop_cmguser_id(pg_cursor)

  for dataset_type in ["RAW_DATA", "DATA_PRODUCT"]:
    # logger.info(f"Processing events for dataset type: {dataset_type}")

    collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

    for cmg_dataset in collection.find():
      if 'name' not in cmg_dataset or not cmg_dataset['name']:
        # logger.info(f"No 'name' field found in the following CMG {dataset_type}:")
        # logger.info(str(cmg_dataset))
        continue
      # logger.info(f"Processing event for CMG {dataset_type}: {cmg_dataset['name']}")
      
      original_name = cmg_dataset["name"]
      # logger.info(f"Original name: {original_name}")
      is_deleted = cmg_dataset.get("visible", False)
      # logger.info(f"Is deleted: {is_deleted}")

        # Determine which dataset this cmg_dataset corresponds to
      corresponding_bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, cmg_dataset['_id'])

      if corresponding_bioloop_dataset is None:
        # logger.info(f"No corresponding dataset found for {original_name} in Bioloop")
        continue

      bioloop_dataset_id = corresponding_bioloop_dataset['id']
      # bioloop_dataset_name = corresponding_bioloop_dataset['name']

      # Convert CMG Events to this Bioloop Audit Logs for this Dataset
      for event in cmg_dataset.get('events', []):
        action = event.get('description')
        timestamp = event.get('stamp')

        pg_cursor.execute(
          """
          INSERT INTO dataset_audit (action, timestamp, dataset_id, user_id)
          VALUES (%s, %s, %s, %s)
          """,
          (action, timestamp, bioloop_dataset_id, cmg_user_id)
        )

      # logger.info(f"Created audit logs for dataset: {bioloop_dataset_name} (id: {bioloop_dataset_id})")
      

# def system_events_to_audit_logs(pg_cursor: cursor, mongo_db: Database):
#   """
#   Convert system-wide Events from CMG MongoDB to Audit Logs in Bioloop PostgreSQL.

#   This function iterates through the events collection in CMG MongoDB,
#   and creates corresponding audit log entries in Bioloop PostgreSQL.

#   Args:
#       pg_cursor (cursor): PostgreSQL database cursor
#       mongo_db (Database): MongoDB database object

#   Returns:
#       None
#   """
#   cmg_user_id = get_bioloop_cmguser_id(pg_cursor)

#   events_collection = mongo_db.events

#   for event in events_collection.find():

#     # logger.info(f"Processing event: {event['action']}")
#     # logger.info(event)

#     action = event.get('action', 'Unknown Action')
#     timestamp = event.get('createdAt', None)
#     description = event.get('details')

#     # Determine the dataset_id if the event is related to a dataset or dataproduct
#     corresponding_bioloop_dataset = None
#     try:
#       if event.get('dataset'):
#         corresponding_bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, event['dataset'])
#       elif event.get('dataproduct'):
#         corresponding_bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, event['dataproduct'])
#     except CMGDatasetNotFoundException as e:
#       # logger.warning(e)
#       traceback.print_exc()
#       # logger.warning(f"No corresponding dataset found for {event['dataset']} in Bioloop")
#       continue
#     except Exception:
#       raise

#     # Insert the audit log entry
#     pg_cursor.execute(
#       """
#       INSERT INTO dataset_audit (action, description, timestamp, user_id, dataset_id)
#       VALUES (%s, %s, %s, %s, %s)
#       """,
#       (action, description, timestamp, cmg_user_id, corresponding_bioloop_dataset['id'])
#     )

#     # logger.info(f"Created audit log for event: {action}")

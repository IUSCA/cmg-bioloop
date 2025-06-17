from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
from typing import Literal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from ..common import find_corresponding_dataset


def events_to_audit_logs(pg_cursor: cursor, mongo_db: Database):
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
  for dataset_type in ["RAW_DATA", "DATA_PRODUCT"]:
    collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

    for mongo_item in collection.find():
      if 'name' not in mongo_item or not mongo_item['name']:
        logger.warning(f"No 'name' field found in the following CMG {dataset_type}:")
        logger.warning(str(mongo_item))
        continue

      original_name = mongo_item["name"]
      is_deleted = mongo_item.get("visible", False)

      # Find all possible matches (original and duplicates)
      pg_cursor.execute(
        """
        SELECT id, name, type, is_deleted, description, num_directories, num_files, 
               du_size, size, created_at, updated_at, origin_path, archive_path, is_staged
        FROM dataset
        WHERE (name = %s OR name LIKE %s)
        AND type = %s AND is_deleted = %s
        """,
        (original_name, f"Duplicate_%{original_name}", dataset_type, is_deleted)
      )
      matching_datasets = pg_cursor.fetchall()

      if not matching_datasets:
        logger.warning(f"Dataset {original_name} not found in Bioloop")
        continue

      try:
        # Determine which dataset this mongo_item corresponds to
        corresponding_dataset = find_corresponding_dataset(pg_cursor, mongo_item)

        if corresponding_dataset is None:
          logger.warning(f"No corresponding dataset found for {original_name} in Bioloop")
          continue

        dataset_id, dataset_name = corresponding_dataset

        # Convert CMG Events to this Bioloop Audit Logs for this Dataset
        for event in mongo_item.get('events', []):
          action = event.get('description')
          timestamp = event.get('stamp')

          pg_cursor.execute(
            """
            INSERT INTO dataset_audit (action, timestamp, dataset_id)
            VALUES (%s, %s, %s)
            """,
            (action, timestamp, dataset_id)
          )

        logger.info(f"Created audit logs for dataset: {dataset_name} (id: {dataset_id})")
      except ValueError as e:
        logger.warning(str(e))

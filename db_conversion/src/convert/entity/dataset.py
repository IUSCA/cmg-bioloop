import logging
from datetime import datetime
from tkinter import NO, N
from typing import Literal

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

DUPLICATE_PREFIX = "DUPLICATE"
UNKNOWN_PREFIX = "UNKNOWN"

logger = logging.getLogger(__name__)

def convert_cmg_datasets(pg_cursor: cursor,
                         mongo_db: Database,
                         dataset_type: Literal["DATA_PRODUCT", "RAW_DATA"]):
  collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

  # logger.info("Processing CMG collection {collection.name} (dataset type {dataset_type})")

  # Group CMG datasets by `name`
  """ 
  Create a mapping of CMG datasets:
    - key:
        the name of a CMG dataset
    - value:
        a list of CMG datasets with this name
  """
  name_groups = {}
  unknown_count = 0
  for index, mongo_item in enumerate(collection.find(), start=1):
    # logger.info(f"Processing CMG {dataset_type} {mongo_item['name']}, Id: {mongo_item['_id']}")
    
    if "name" not in mongo_item:
      # logger.info(f"No 'name' field found in the following CMG {dataset_type}:")
      # logger.info(f"Id: {mongo_item['_id']}")
      unknown_count += 1
      assigned_name = f"{UNKNOWN_PREFIX}-{unknown_count}" if unknown_count > 1 else UNKNOWN_PREFIX
      # logger.info(f"Assigned name: {assigned_name}")
      mongo_item["name"] = assigned_name

    name = mongo_item["name"]
    if name not in name_groups:
      name_groups[name] = []
    name_groups[name].append(mongo_item)
    # logger.info(f"Added CMG {dataset_type} {mongo_item['name']}, Id: {mongo_item['_id']} to name groups")

  """
  For the name of each CMG dataset, process the group of CMG datasets with the same name
      - original_name:
          the name of a CMG dataset
      - items:
          list of CMG datasets with this name
  """
  # logger.info(f"Found {len(name_groups)} unique CMG {dataset_type} names")
  for original_name, items in name_groups.items():
    # logger.info(f"Number of CMG {dataset_type} {original_name} name groups, len(items): {len(items)}")
    if len(items) == 1:
      insert_dataset(pg_cursor, items[0], dataset_type, original_name, False)
    else:
      for item in items:
        new_name = handle_duplicate_name(pg_cursor, original_name, dataset_type, False)
        # logger.info(f"Processing CMG {dataset_type} {item['name']}, new_name: {new_name}")
        insert_dataset(pg_cursor, item, dataset_type, new_name, False)


def insert_dataset(pg_cursor, mongo_item, dataset_type, name, is_deleted):
  # logger.info(f"Inserting dataset: {name}, type: {dataset_type}, is_deleted: {is_deleted}, mongo_item id: {mongo_item['_id']}, mongo_item name: {mongo_item['name']}")
  if not name:
    raise ValueError(f"Dataset Name must be specified")
  elif not dataset_type:
    raise ValueError(f"Dataset Type must be specified")
  if is_deleted is None:
    raise ValueError(f"Deletion status must be specified")

  # string used in logs to represent deletion status
  log_msg_deleted = 'deleted' if is_deleted else 'non-deleted'

  # Get current timestamp for default values
  current_timestamp = datetime.utcnow()

  # CMG Datasets with missing names are prefixed with string "UNKNOWN" before insertion into Bioloop.
  # These CMG datasets may not have fields `createdAt` or `deletedAt`, in which case we default those
  # to the current timestamp
  if name.startswith(UNKNOWN_PREFIX):
    created_at = mongo_item.get("createdAt", current_timestamp)
    updated_at = mongo_item.get("updatedAt", current_timestamp)
  else:
    created_at = mongo_item.get("createdAt", None)
    updated_at = mongo_item.get("updatedAt", None)
  
  pg_cursor.execute(
    """
    INSERT INTO dataset (name, type, is_deleted, cmg_id, description, num_directories, num_files, 
                          du_size, size, created_at, updated_at, origin_path, 
                          archive_path, staged_path, is_staged, metadata)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """,
    (
      name,
      dataset_type,
      is_deleted,
      str(mongo_item["_id"]),
      mongo_item.get("description", None),
      mongo_item.get("directories", 0),
      mongo_item.get("files", 0) if dataset_type == "RAW_DATA" else 0,
      mongo_item.get("du_size", 0) if dataset_type == "RAW_DATA" else 0,
      mongo_item.get("size", 0),
      created_at,
      updated_at,
      mongo_item.get("paths", {}).get("origin", None),
      mongo_item.get("paths", {}).get("archive", None),
      mongo_item.get("paths", {}).get("staged", None),
      mongo_item.get("staged", False),
      None
    )
  )
  dataset_id = pg_cursor.fetchone()['id']

  # logger.info(
    # f"""
    # Converted {log_msg_deleted} CMG {dataset_type} {mongo_item['name']} (CMG {dataset_type} Id: {mongo_item['_id']})
    # into {log_msg_deleted} Bioloop {dataset_type} {name} (Bioloop {dataset_type} Id: {dataset_id})
    # """)
  
  # dataset_id = pg_cursor.fetchone()['id']
  # # logger.info(f"Inserted {dataset_type}: {mongo_item['_id']}, {name} to dataset_id: {dataset_id}")


# Usage
def convert_all_datasets(pg_cursor: cursor, mongo_db: Database):
  # convert records from CMG's `dataset` collection to Bioloop's Raw Data
  convert_cmg_datasets(pg_cursor, mongo_db, dataset_type="RAW_DATA")
  # convert records from CMG's `data_products` collection to Bioloop Data Products
  convert_cmg_datasets(pg_cursor, mongo_db, dataset_type="DATA_PRODUCT")


def handle_duplicate_name(pg_cursor, original_name, dataset_type, is_deleted):
  if not original_name:
    raise ValueError(
      f"Dataset Name must be specified")
  elif not dataset_type:
    raise ValueError(
      f"Dataset Type must be specified")
  elif is_deleted is None:
    raise ValueError(f"Deletion status must be specified")

  # logger.info(f"Handling duplicate for: name {original_name}, type {dataset_type}, is_deleted {is_deleted}")
  new_name = f"{DUPLICATE_PREFIX}_{original_name}"
  duplicate_count = 1
  while True:
    # logger.info(f"Checking if name '{new_name}' exists...")
    pg_cursor.execute(
      """
      SELECT id FROM dataset
      WHERE name = %s AND type = %s AND is_deleted = %s
      """,
      (new_name, dataset_type, is_deleted)
    )
    
    result = pg_cursor.fetchone()
    if result is not None:
      result = result['id']
    # logger.info(f"Query result: {result}")
    else:
      break
    duplicate_count += 1
    new_name = f"{DUPLICATE_PREFIX}_{duplicate_count}_{original_name}"
    # logger.info(f"Name exists, trying new name: {new_name}")
  # logger.info(f"Final name: {new_name}")
  return new_name

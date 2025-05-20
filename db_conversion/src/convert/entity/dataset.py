from psycopg2.extensions import cursor
from pymongo.database import Database

from typing import Literal

DUPLICATE_SUFFIX = "Duplicate"


def convert_cmg_datasets(pg_cursor: cursor, mongo_db: Database,
                         dataset_type: Literal["DATA_PRODUCT", "RAW_DATA"]):
  collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

  # Group CMG Datasets by `name`
  name_groups = {}
  for mongo_item in collection.find():
    name = mongo_item["name"]
    if name not in name_groups:
      name_groups[name] = []
    name_groups[name].append(mongo_item)

  for original_name, items in name_groups.items():
    if dataset_type == "DATA_PRODUCT":
      visible_items = [item for item in items if item.get("visible", False)]
      if len(visible_items) == 1 and len(items) > 1:
        # If only a single visible (i.e. not deleted) CMG Dataset is found out of several duplicates,
        # insert it in Bioloop with original name from CMG
        insert_dataset(pg_cursor, visible_items[0], dataset_type, original_name)
        # Insert other duplicates with the `Duplicate-[n]` prefix
        for item in items:
          if not item.get("visible", False):
            new_name = handle_duplicate_name(pg_cursor, original_name, dataset_type, False, DUPLICATE_SUFFIX)
            insert_dataset(pg_cursor, item, dataset_type, new_name)
      else:
        # If Multiple visible items or all invisible found among several duplicates, use duplicate prefix for all
        for item in items:
          new_name = handle_duplicate_name(pg_cursor, original_name, dataset_type, item.get("visible", False),
                                           DUPLICATE_SUFFIX)
          insert_dataset(pg_cursor, item, dataset_type, new_name)
    else:  # RAW_DATA
      # For datasets collection, all are considered non-deleted
      if len(items) == 1:
        insert_dataset(pg_cursor, items[0], dataset_type, original_name)
      else:
        for item in items:
          new_name = handle_duplicate_name(pg_cursor, original_name, dataset_type, False, DUPLICATE_SUFFIX)
          insert_dataset(pg_cursor, item, dataset_type, new_name)


def insert_dataset(pg_cursor, mongo_item, dataset_type, name):
  # For DATA_PRODUCT:
  #   - We get the "visible" value from mongo_item, defaulting to True if not present
  #   - We then negate this value to get "is_deleted"
  # For RAW_DATA:
  #   - We always set "is_deleted" to False
  is_deleted = not mongo_item.get("visible", True) if dataset_type == "DATA_PRODUCT" else False

  pg_cursor.execute(
    """
    INSERT INTO dataset (name, type, is_deleted, cmg_id, description, num_directories, num_files, 
                         du_size, size, created_at, updated_at, origin_path, 
                         archive_path, is_staged, metadata)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
      mongo_item.get("createdAt", None),
      mongo_item.get("updatedAt", None),
      mongo_item.get("paths", {}).get("origin", None),
      mongo_item.get("paths", {}).get("archive", None),
      mongo_item.get("staged", False),
      None
    )
  )
  dataset_id = pg_cursor.fetchone()[0]
  print(f"Inserted {dataset_type}: {mongo_item['_id']}, {name} to dataset_id: {dataset_id}")


# Usage
def convert_all_datasets(pg_cursor: cursor, mongo_db: Database):
  # convert records from CMG's `dataset` collection to Bioloop's `dataset` table rows (with Dataset Type `RAW_DATA`)
  convert_cmg_datasets(pg_cursor, mongo_db, dataset_type="RAW_DATA")
  # convert records from CMG's `data_products` collection to Bioloop's `dataset` table (with Dataset Type `DATA_PRODUCT`)
  convert_cmg_datasets(pg_cursor, mongo_db, dataset_type="DATA_PRODUCT")


def handle_duplicate_name(pg_cursor, original_name, dataset_type, is_deleted, duplicate_suffix):
  """
    Generates a new, unique name for a Bioloop Dataset when a duplicate corresponding Dataset is found in Bioloop.

    This function is used during the dataset conversion process to handle cases where
    a CMG Dataset with the same name already exists in the Bioloop database. It appends a
    suffix to the original name and increments it until a unique name is found.

    A Bioloop Dataset is considered a duplicate of another if they have the same `name`, `type`, and `is_deleted` values.

    Args:
        pg_cursor (cursor): PostgreSQL database cursor.
        original_name (str): The original name of the CMG Dataset that has a duplicate in CMG.
        dataset_type (str): The type of the Bioloop Dataset (e.g., 'RAW_DATA' or 'DATA_PRODUCT') that the CMG Dataset is to be converted to.
        is_deleted (bool): Indicates whether the dataset is marked as deleted.
        duplicate_suffix (str): The prefix to use for duplicate names (e.g., 'Duplicate').

    Returns:
        str: A new, unique name for the dataset.

    Example:
        If 'Dataset1' already exists, this function might return 'Duplicate_Dataset_1'.
        If 'Duplicate_Dataset_1' also exists, it might return 'Duplicate_2_Dataset_1', and so on.

    Note:
        This function modifies the database by checking for existing names but does not
        insert or update any records. It only generates a new name.
    """
  print(f"Duplicate dataset found: {original_name}")
  new_name = f"{duplicate_suffix}_{original_name}"
  suffix = 1
  while True:
    pg_cursor.execute(
      """
      SELECT id FROM dataset
      WHERE name = %s AND type = %s AND is_deleted = %s
      """,
      (new_name, dataset_type, is_deleted)
    )
    if not pg_cursor.fetchone():
      break
    suffix += 1
    new_name = f"{duplicate_suffix}_{suffix}_{original_name}"
  print(f"Renaming to: {new_name}")
  return new_name

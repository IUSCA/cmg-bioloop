from psycopg2.extensions import cursor
from pymongo.database import Database

from .audit_log import create_audit_logs_from_dataset_events
from .user import get_bioloop_cmguser_id
from .file import create_file_and_directories

from typing import Literal


def convert_cmg_datasets(pg_cursor: cursor, mongo_db: Database,
                         dataset_type: Literal["DATA_PRODUCT", "RAW_DATA"]):
  # Note: CMG's `dataset` collection corresponds to Bioloop's RAW_DATA datasets
  # Note: CMG's `data_product` collection corresponds to Bioloop's DATA_PRODUCTS datasets

  # used to handle duplicate Dataset names from CMG during conversion into Bioloop Datasets (see
  # method `handle_duplicate_name`)
  duplicate_suffix = "Duplicate"

  collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

  for mongo_item in collection.find():
    original_name = mongo_item["name"]
    is_deleted = mongo_item.get("visible", False)

    # Check if the dataset already exists
    pg_cursor.execute(
      """
      SELECT id FROM dataset
      WHERE name = %s AND type = %s AND is_deleted = %s
      """,
      (original_name, dataset_type, is_deleted)
    )
    existing_dataset = pg_cursor.fetchone()

    if existing_dataset:
      new_name = handle_duplicate_name(pg_cursor, original_name, dataset_type, is_deleted, duplicate_suffix)
      mongo_item["name"] = new_name
    else:
      new_name = original_name

    # Insert the dataset
    pg_cursor.execute(
      """
      INSERT INTO dataset (name, type, is_deleted, description, num_directories, num_files, 
                           du_size, size, created_at, updated_at, origin_path, 
                           archive_path, is_staged, metadata)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        new_name,
        dataset_type,
        is_deleted,
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
    print(f"Converted {dataset_type}: {mongo_item['_id']}, {new_name} to dataset_id: {dataset_id}")

    # if type == "RAW_DATA":
  # Additional processing for raw datasets
  # create_audit_logs_from_dataset_events(pg_cursor, mongo_item, dataset_id, mongo_db)
  # for checksum in mongo_item.get("checksums", []):
  #     create_file_and_directories(pg_cursor, checksum, dataset_id)


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

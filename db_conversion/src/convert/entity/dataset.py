from datetime import datetime
import json
from bson import ObjectId
from json import JSONEncoder

from psycopg2.extensions import cursor
from pymongo.database import Database

from .audit_log import create_audit_logs_from_dataset_events
from .user import get_bioloop_cmguser_id
from .file import create_file_and_directories

from bson import ObjectId
from json import JSONEncoder

from bson import ObjectId
from json import JSONEncoder
from datetime import datetime


# class MongoJSONEncoder(JSONEncoder):
#   def default(self, o):
#     if isinstance(o, ObjectId):
#       return str(o)
#     if isinstance(o, datetime):
#       return o.isoformat()
#     return JSONEncoder.default(self, o)


def convert_datasets(pg_cursor: cursor, mongo_db: Database):
  duplicate_suffix = "Duplicate"
  mongo_datasets = mongo_db.datasets.find()
  for mongo_dataset in mongo_datasets:
    original_name = mongo_dataset["name"]
    dataset_type = "RAW_DATA"
    is_deleted = mongo_dataset.get("visible", False)

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
      print(f"Duplicate dataset found: {original_name}")
      # Generate a new name with "duplicate_" prefix
      new_name = f"{duplicate_suffix}_{original_name}"

      # Check if the new name also exists (in case of multiple duplicates)
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
      mongo_dataset["name"] = new_name
    else:
      new_name = original_name

    # Insert the dataset (either with original name or new name)
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
        mongo_dataset.get("description", None),
        mongo_dataset.get("directories", 0),
        mongo_dataset.get("files", 0),
        mongo_dataset.get("du_size", 0),
        mongo_dataset.get("size", 0),
        mongo_dataset.get("createdAt", None),
        mongo_dataset.get("updatedAt", None),
        mongo_dataset.get("paths", {}).get("origin", None),
        mongo_dataset.get("paths", {}).get("archive", None),
        mongo_dataset.get("staged", False),
        None
      )
    )
    raw_data_dataset_id = pg_cursor.fetchone()[0]
    print(
      f"Converted mongo_dataset: {mongo_dataset['_id']}, {new_name} to raw_data_dataset_id: {raw_data_dataset_id}"
    )

    # Insert dataset_audit records if events exist
    # create_audit_logs_from_dataset_events(pg_cursor, mongo_dataset, raw_data_dataset_id, mongo_db)

    # for checksum in mongo_dataset.get("checksums", []):
    #   create_file_and_directories(pg_cursor, checksum, raw_data_dataset_id)


def convert_data_products(pg_cursor: cursor, mongo_db: Database):
  duplicate_suffix = "Duplicate"
  mongo_data_products = mongo_db.dataproducts.find()
  for mongo_data_product in mongo_data_products:
    original_name = mongo_data_product["name"]
    dataset_type = "DATA_PRODUCT"
    is_deleted = mongo_data_product.get("visible", False)

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
      print(f"Duplicate dataset found: {original_name}")
      # Generate a new name with "duplicate_" prefix
      new_name = f"{duplicate_suffix}_{original_name}"

      # Check if the new name also exists (in case of multiple duplicates)
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
      mongo_data_product["name"] = new_name
    else:
      new_name = original_name

    # Insert the dataset (either with original name or new name)
    pg_cursor.execute(
      """
      INSERT INTO dataset (name, type, is_deleted, description, num_directories, num_files, du_size, size, created_at, updated_at, origin_path, archive_path, is_staged, metadata)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        new_name,
        dataset_type,
        is_deleted,
        mongo_data_product.get("description", None),
        mongo_data_product.get("directories", 0),
        0,
        0,
        mongo_data_product.get("size", 0),
        mongo_data_product.get("createdAt", None),
        mongo_data_product.get("updatedAt", None),
        mongo_data_product.get("paths", {}).get("origin", None),
        mongo_data_product.get("paths", {}).get("archive", None),
        mongo_data_product.get("staged", False),
        None
      )
    )
    data_product_dataset_id = pg_cursor.fetchone()[0]
    print(
      f"Converted mongo_dataset: {mongo_data_product['_id']}, {new_name} to data_product_dataset_id: {data_product_dataset_id}"
    )

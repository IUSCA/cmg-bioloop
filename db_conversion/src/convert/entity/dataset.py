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


class MongoJSONEncoder(JSONEncoder):
  def default(self, o):
    if isinstance(o, ObjectId):
      return str(o)
    if isinstance(o, datetime):
      return o.isoformat()
    return JSONEncoder.default(self, o)


def convert_datasets(pg_cursor: cursor, mongo_db: Database):
  print("convert_datasets")
  mongo_datasets = mongo_db.datasets.find()
  for mongo_dataset in mongo_datasets:
    print(f"Converting dataset: {mongo_dataset['name']}")
    print("origin", mongo_dataset.get("paths", {}).get("origin", ""))
    # print(mongo_dataset)
    # print("mongo_dataset.get('description'):", mongo_dataset.get("description"))
    pg_cursor.execute(
      """
      INSERT INTO dataset (name, type, description, num_directories, num_files, 
                           du_size, size, created_at, updated_at, origin_path, 
                           archive_path, is_deleted, is_staged, metadata)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        mongo_dataset["name"],
        "RAW_DATA",
        mongo_dataset.get("description", None),
        mongo_dataset.get("directories", 0),
        mongo_dataset.get("files", 0),
        mongo_dataset.get("du_size", 0),
        mongo_dataset.get("size", 0),
        mongo_dataset.get("createdAt", None),
        mongo_dataset.get("updatedAt", None),
        mongo_dataset.get("paths", {}).get("origin", None),
        mongo_dataset.get("paths", {}).get("archive", None),
        mongo_dataset.get("visible", False),
        mongo_dataset.get("staged", False),
        None
      )
    )
    raw_data_dataset_id = pg_cursor.fetchone()[0]
    print(
      f"Converted mongo_dataset: {mongo_dataset['_id']}, {mongo_dataset['name']} to raw_data_dataset_id: {raw_data_dataset_id}")

    # Insert dataset_audit records if events exist
    # create_audit_logs_from_dataset_events(pg_cursor, mongo_dataset, raw_data_dataset_id, mongo_db)

    # for checksum in mongo_dataset.get("checksums", []):
    #   create_file_and_directories(pg_cursor, checksum, raw_data_dataset_id)


def convert_data_products(pg_cursor: cursor, mongo_db: Database):
  mongo_data_products = mongo_db.dataproducts.find()
  for mongo_data_product in mongo_data_products:
    mongo_data_product['last_staged'] = None
    mongo_data_product['taken_at'] = None
    mongo_data_product['events'] = []
    mongo_data_product['id'] = None
    print("mongo_data_product:")
    print(json.dumps(mongo_data_product, indent=2, cls=MongoJSONEncoder))
    print("---------")
    # Check if the dataset already exists
    pg_cursor.execute(
      """
      SELECT id FROM dataset
      WHERE name = %s AND type = %s AND is_deleted = %s
      """,
      (
        mongo_data_product["name"],
        "DATA_PRODUCT",
        mongo_data_product.get("visible", False)
      )
    )
    existing_dataset = pg_cursor.fetchone()

    if existing_dataset:
      print(f"Skipping existing dataset: {mongo_data_product['name']}")
      continue

    # If the dataset doesn't exist, insert it
    pg_cursor.execute(
      """
      INSERT INTO dataset (name, type, is_deleted, description, num_directories, num_files, du_size, size, created_at, updated_at, origin_path, archive_path, is_staged, metadata)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        mongo_data_product["name"],
        "DATA_PRODUCT",
        mongo_data_product.get("visible", False),
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
        mongo_data_product.get("metadata", None)
      )
    )
    data_product_dataset_id = pg_cursor.fetchone()[0]
    print(
      f"Converted mongo_dataset: {mongo_data_product['_id']}, {mongo_data_product['name']} to data_product_dataset_id: {data_product_dataset_id}"
    )

  # The rest of your code (commented out sections) can remain as is

# def convert_data_products(pg_cursor: cursor, mongo_db: Database):
#   mongo_data_products = mongo_db.dataproducts.find()
#   for mongo_data_product in mongo_data_products:
#     pg_cursor.execute(
#       """
#       INSERT INTO dataset (name, type)
#       VALUES (%s, %s)
#       RETURNING id
#       """,
#       (
#         mongo_data_product["name"],
#         "DATA_PRODUCT"
#       )
#     )
#     """
#     # print("mongo_data_product", mongo_data_product)
#     # pg_cursor.execute(
#     #   """
#     #   INSERT INTO dataset (name, type, description, num_directories, num_files,
#     #                        du_size, size, created_at, updated_at, origin_path,
#     #                        archive_path, is_deleted, is_staged, metadata)
#     #   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     #   RETURNING id
#     #   """,
#     #   (
#     #     mongo_data_product["name"],
#     #     "DATA_PRODUCT",
#     #     mongo_data_product.get("description", None),
#     #     mongo_data_product.get("directories", 0),
#     #     mongo_data_product.get("files", 0),
#     #     mongo_data_product.get("du_size", 0),
#     #     mongo_data_product.get("size", 0),
#     #     mongo_data_product.get("createdAt", None),
#     #     mongo_data_product.get("updatedAt", None),
#     #     mongo_data_product.get("paths", {}).get("origin", None),
#     #     mongo_data_product.get("paths", {}).get("archive", None),
#     #     mongo_data_product.get("visible", False),
#     #     mongo_data_product.get("staged", False),
#     #     mongo_data_product.get("metadata", None)
#     #   )
#     # )
#     data_product_dataset_id = pg_cursor.fetchone()[0]
#     print(
#       f"Converted mongo_dataset: {mongo_data_product['_id']}, {mongo_data_product['name']} to data_product_dataset_id: {data_product_dataset_id}")
#
#     # Create dataset hierarchies
#     # if "dataset" in mongo_data_product:
#     #   # Fetch the corresponding RAW_DATA dataset id from postgres
#     #   pg_cursor.execute(
#     #     """
#     #     SELECT id FROM dataset
#     #     WHERE name = %s AND is_deleted = %s AND type = 'RAW_DATA'
#     #     """,
#     #     (mongo_data_product["dataset"], mongo_data_product["visible"])
#     #   )
#     #   raw_data_result = pg_cursor.fetchone()
#     #   if raw_data_result:
#     #     raw_data_dataset_id = raw_data_result[0]
#     #     # Create dataset hierarchy
#     #     pg_cursor.execute(
#     #       """
#     #       INSERT INTO dataset_hierarchy (source_id, derived_id)
#     #       VALUES (%s, %s)
#     #       """,
#     #       (raw_data_dataset_id, data_product_dataset_id)
#     #     )
#     #   else:
#     #     print(f"Warning: RAW_DATA dataset not found for {mongo_data_product['dataset']}")
#
#     # Insert dataset_audit records if events exist
#     # create_audit_logs_from_dataset_events(pg_cursor, mongo_data_product, data_product_dataset_id,
#     #                                       cmg_user_id)
#
#     # for file in mongo_data_product.get("files", []):
#     #   create_file_and_directories(pg_cursor, file, data_product_dataset_id)

from datetime import datetime
import json

from audit_log import create_audit_logs_from_dataset_events
from user import get_cmguser_id
from file import create_file_and_directories


def convert_datasets(cur, mongo_db):
  with cur:
    cmg_user_id = get_cmguser_id(cur)

    mongo_datasets = mongo_db.dataset.find()
    for mongo_dataset in mongo_datasets:
      cur.execute(
        """
        INSERT INTO dataset (name, type, description, num_directories, num_files, 
                             du_size, size, created_at, updated_at, origin_path, 
                             archive_path, id_deleted, is_staged, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
          mongo_dataset["name"],
          "RAW_DATA",
          mongo_dataset.get("description"),
          mongo_dataset.get("directories"),
          mongo_dataset.get("files"),
          mongo_dataset.get("du_size"),
          mongo_dataset.get("size"),
          mongo_dataset.get("createdAt", datetime.utcnow()),
          mongo_dataset.get("updatedAt", datetime.utcnow()),
          mongo_dataset.get("paths", {}).get("origin"),
          mongo_dataset.get("paths", {}).get("archive"),
          mongo_dataset.get("visible", False),
          mongo_dataset.get("staged", False),
          json.dumps(mongo_dataset),
        )
      )
      raw_data_dataset_id = cur.fetchone()[0]

      # Insert dataset_audit records if events exist
      create_audit_logs_from_dataset_events(cur, mongo_dataset, raw_data_dataset_id, cmg_user_id)

      for checksum in mongo_dataset.get("checksums", []):
        create_file_and_directories(cur, checksum, raw_data_dataset_id)


def convert_data_products(cur, mongo_db):
  with cur:
    cmg_user_id = get_cmguser_id(cur)

    mongo_data_products = mongo_db.dataproduct.find()
    for mongo_data_product in mongo_data_products:
      cur.execute(
        """
        INSERT INTO dataset (name, type, description, num_files, size, created_at, 
                             updated_at, archive_path, is_staged, is_deleted, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
          mongo_data_product["name"],
          "DATA_PRODUCT",
          None,
          len(mongo_data_product.get("files", [])),
          mongo_data_product.get("size"),
          mongo_data_product.get("createdAt", datetime.utcnow()),
          mongo_data_product.get("updatedAt", datetime.utcnow()),
          mongo_data_product.get("paths", {}).get("archive"),
          mongo_data_product.get("staged", False),
          mongo_data_product.get("visible", False),
          json.dumps(mongo_data_product),
        )
      )
      data_product_dataset_id = cur.fetchone()[0]

      # Create dataset hierarchies
      if "dataset" in mongo_data_product:
        # Fetch the corresponding RAW_DATA dataset id from postgres
        cur.execute(
          """
          SELECT id FROM dataset
          WHERE name = %s AND is_deleted = %s AND type = 'RAW_DATA'
          """,
          (mongo_data_product["dataset"], mongo_data_product["visible"])
        )
        raw_data_result = cur.fetchone()
        if raw_data_result:
          raw_data_dataset_id = raw_data_result[0]
          # Create dataset hierarchy
          cur.execute(
            """
            INSERT INTO dataset_hierarchy (source_id, derived_id)
            VALUES (%s, %s)
            """,
            (raw_data_dataset_id, data_product_dataset_id)
          )
        else:
          print(f"Warning: RAW_DATA dataset not found for {mongo_data_product['dataset']}")

      # Insert dataset_audit records if events exist
      create_audit_logs_from_dataset_events(cur, mongo_data_product, data_product_dataset_id,
                                            cmg_user_id)

      for file in mongo_data_product.get("files", []):
        create_file_and_directories(cur, file, data_product_dataset_id)

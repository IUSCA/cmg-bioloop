import logging
import os
import traceback

from psycopg2 import errors
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_bioloop_dataset

logger = logging.getLogger(__name__)


def convert_files(pg_cursor: cursor, mongo_db: Database):
  # logger.info("converting dataset files")
  convert_dataset_files(pg_cursor, mongo_db)
  # logger.info("converting data_product files")
  convert_data_product_files(pg_cursor, mongo_db)
  # logger.info("finished converting files")


# def create_dataset_file_hierarchies(pg_cursor: cursor, mongo_db: Database):


def convert_dataset_files(pg_cursor: cursor, mongo_db: Database):
  """
  Convert 'files' field from CMG datasets to Bioloop dataset_file table rows.
  """
  collection = mongo_db.datasets

  for cmg_dataset in collection.find():
    # logger.info(f"Processing Dataset file for CMG Dataset: {mongo_item['name']}, ID: {mongo_item['_id']}")
    # Find the Bioloop Dataset that matches CMG dataset most closely
    bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, cmg_dataset['_id'])
    if bioloop_dataset is None:
      # logger.warning(f"No matching dataset found for {mongo_item['name']} in Bioloop")
      continue
    dataset_id = bioloop_dataset['id']
    dataset_name = bioloop_dataset['name']
    # logger.info(f"Processing Dataset files for Bioloop Dataset: {dataset_name}, ID: {dataset_id}")
    for file in cmg_dataset.get('checksums', []):
      # logger.info(f"Processing file {file.get('path')}")
      # Extract file name from path
      file_name = file.get('path').split('/')[-1]
      try:
        pg_cursor.execute(
          """
          INSERT INTO dataset_file (dataset_id, path, name, md5)
          VALUES (%s, %s, %s, %s)
          """,
          (dataset_id, file.get('path'), file_name, file.get('md5'))
        )
      except errors.UniqueViolation as e:
        # logger.warning(e)
        traceback.print_exc()
        # logger.info(f"Unique violation for file: {file.get('path')}")
      except Exception:
        raise
    # logger.info(f"Inserted files for dataset: {dataset_name} (id: {dataset_id})")

def convert_data_product_files(pg_cursor: cursor, mongo_db: Database):
  """
  Convert 'files' field from CMG dataproducts to Bioloop dataset_file table rows.
  """
  collection = mongo_db.dataproducts

  for cmg_data_product in collection.find():
    # Find the Bioloop Dataset that matches CMG Dataproduct most closely
    bioloop_data_product = find_corresponding_bioloop_dataset(pg_cursor, cmg_data_product['_id'])
    # logger.info(f"Processing Data Product files for CMG Data Product: {cmg_data_product['name']}, ID: {cmg_data_product['_id']}")
    if bioloop_data_product is None:
      # logger.warning(f"No matching dataset found for {cmg_data_product['name']} in Bioloop")
      continue

    dataset_id = bioloop_data_product['id']
    dataset_name = bioloop_data_product['name']
    # logger.info(f"Processing Data Product files for Bioloop Data Product: {dataset_name}, ID: {dataset_id}")
    # Process 'files' array and insert into Bioloop's `dataset_file` table
    for file in cmg_data_product.get('files', []):
      # todo - insert file name
      # logger.info(f"Processing file {file.get('path')}")
      # Extract file name from path
      file_name = file.get('path').split('/')[-1]
      try:
        pg_cursor.execute(
          """
          INSERT INTO dataset_file (dataset_id, path, name, size, md5)
          VALUES (%s, %s, %s, %s, %s)
          """,
          (dataset_id, file.get('path'), file_name, file.get('size'), file.get('md5'))
        )
      except errors.UniqueViolation as e:
        # logger.warning(e)
        traceback.print_exc()
        # logger.info(f"Unique violation for file: {file.get('path')}")
      except Exception:
        raise

    # logger.info(f"Inserted files for Bioloop Data Product: {dataset_name} (id: {dataset_id})")


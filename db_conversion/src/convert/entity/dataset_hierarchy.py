import logging

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_bioloop_dataset

logger = logging.getLogger(__name__)


def convert_dataset_hierarchies(pg_cursor: cursor, mongo_db: Database):
  """
  Convert dataset hierarchies from CMG to Bioloop.

  This function reads the dataset-hierarchy relationships from the CMG MongoDB
  between CMG's `dataproducts` and `datasets` (CMG `dataproducts` are derived from
  `datasets`) and creates corresponding `dataset_hierarchy` records in Bioloop PostgreSQL.
  """
  cmg_data_products = mongo_db.dataproducts.find({})

  for cmg_data_product in cmg_data_products:
    # Find the corresponding Bioloop DATA_PRODUCT
    bioloop_data_product = find_corresponding_bioloop_dataset(pg_cursor, cmg_data_product['_id'])

    # Get the source dataset for this CMG dataproduct
    cmg_source_dataset_id = cmg_data_product.get('dataset')

    if not cmg_source_dataset_id:
      # logger.info(f"Skipped hierarchy: No source dataset found for CMG dataproduct {cmg_data_product['name']}")
      continue

    # Find the corresponding source dataset in CMG
    cmg_source_dataset = mongo_db.datasets.find_one({'_id': ObjectId(cmg_source_dataset_id)})

    if not cmg_source_dataset:
      # logger.info(f"Skipped hierarchy: CMG source dataset not found for dataproduct {cmg_data_product['name']}")
      continue

    # Find the corresponding Bioloop RAW_DATA
    bioloop_raw_data = find_corresponding_bioloop_dataset(pg_cursor, cmg_source_dataset['_id'])

    if not bioloop_raw_data:
      # logger.info(f"Skipped hierarchy: Bioloop RAW_DATA not found for CMG dataset {cmg_source_dataset['name']}")
      continue

    # Insert the hierarchy relationship into Bioloop
    pg_cursor.execute(
      """
      INSERT INTO dataset_hierarchy (source_id, derived_id)
      VALUES (%s, %s)
      """,
      (bioloop_raw_data['id'], bioloop_data_product['id'])
    )
    # logger.info(f"Created hierarchy:")
    # logger.info(f"Bioloop RAW_DATA: {bioloop_raw_data['name']} (Id: {bioloop_raw_data['id']})")
    # logger.info(f"Bioloop DATA_PRODUCT: {bioloop_data_product['name']} (Id: {bioloop_data_product['id']})")

  # logger.info("Dataset hierarchy conversion completed.")

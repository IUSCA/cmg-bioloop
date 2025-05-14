from pymongo.database import Database
from psycopg2.extensions import cursor
from bson import ObjectId
import logging

from pymongo.database import Database
from psycopg2.extensions import cursor
from bson import ObjectId

from ..common import find_corresponding_dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_dataset_hierarchies(pg_cursor: cursor, mongo_db: Database):
  """
  Convert dataset hierarchies from CMG to Bioloop.

  This function reads the dataset-hierarchy relationships from the CMG MongoDB
  between CMG's `dataproducts` and `datasets` (CMG `dataproducts` are derived from
  `datasets`) and creates corresponding `dataset_hierarchy` records in Bioloop PostgreSQL.
  """
  cmg_dataproducts = mongo_db.dataproducts.find({})

  for cmg_dataproduct in cmg_dataproducts:
    # Find the corresponding Bioloop DATA_PRODUCT
    bioloop_dataproduct = find_corresponding_dataset(pg_cursor, cmg_dataproduct, "DATA_PRODUCT")

    if not bioloop_dataproduct:
      logger.warning(f"Skipped hierarchy: Bioloop DATA_PRODUCT not found for CMG dataproduct {cmg_dataproduct['name']}")
      continue

    # Get the source dataset for this dataproduct
    source_dataset_id = cmg_dataproduct.get('dataset')

    if not source_dataset_id:
      logger.warning(f"Skipped hierarchy: No source dataset found for CMG dataproduct {cmg_dataproduct['name']}")
      continue

    # Find the corresponding source dataset in CMG
    cmg_source_dataset = mongo_db.datasets.find_one({'_id': ObjectId(source_dataset_id)})

    if not cmg_source_dataset:
      logger.warning(f"Skipped hierarchy: CMG source dataset not found for dataproduct {cmg_dataproduct['name']}")
      continue

    # Find the corresponding Bioloop RAW_DATA
    bioloop_raw_data = find_corresponding_dataset(pg_cursor, cmg_source_dataset, "RAW_DATA")

    if not bioloop_raw_data:
      logger.warning(f"Skipped hierarchy: Bioloop RAW_DATA not found for CMG dataset {cmg_source_dataset['name']}")
      continue

    # Insert the hierarchy relationship into Bioloop
    pg_cursor.execute(
      """
      INSERT INTO dataset_hierarchy (source_id, derived_id)
      VALUES (%s, %s)
      ON CONFLICT (source_id, derived_id) DO NOTHING
      """,
      (bioloop_raw_data[0], bioloop_dataproduct[0])
    )
    logger.info(f"Created hierarchy: {bioloop_raw_data[1]} -> {bioloop_dataproduct[1]}")

  logger.info("Dataset hierarchy conversion completed.")

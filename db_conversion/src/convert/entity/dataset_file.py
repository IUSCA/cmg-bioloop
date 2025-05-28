from psycopg2.extensions import cursor
from pymongo.database import Database
import logging

from ..common import find_corresponding_dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_dataproduct_files(pg_cursor: cursor, mongo_db: Database):
  """
  Convert 'files' field from CMG dataproducts to Bioloop dataset_file table rows.
  """
  collection = mongo_db.dataproducts

  for mongo_item in collection.find():
    try:
      # Find the Bioloop Dataset that matches CMG dataproduct most closely
      bioloop_dataset = find_corresponding_dataset(pg_cursor, mongo_item)

      if bioloop_dataset is None:
        logger.warning(f"No matching dataset found for {mongo_item['name']} in Bioloop")
        continue

      dataset_id, dataset_name = bioloop_dataset

      # Process 'files' array and insert into Bioloop's `dataset_file` table
      for file in mongo_item.get('files', []):
        pg_cursor.execute(
          """
          INSERT INTO dataset_file (dataset_id, path, size, md5)
          VALUES (%s, %s, %s, %s)
          """,
          (dataset_id, file.get('path'), file.get('size'), file.get('md5'))
        )

      logger.info(f"Inserted files for dataset: {dataset_name} (id: {dataset_id})")

    except Exception as e:
      logger.error(f"Error processing dataset {mongo_item.get('name', 'Unknown')}: {str(e)}")

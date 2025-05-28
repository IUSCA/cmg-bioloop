from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
from typing import Literal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_corresponding_dataset(pg_cursor, mongo_item):
  """
  Find the Dataset in Bioloop that corresponds to a given CMG Dataset/Data Product.

  This function uses the `cmg_id` field on the Bioloop Dataset row to match with
  the `_id` field of the CMG Dataset.

  Args:
      pg_cursor: PostgreSQL cursor
      mongo_item (dict): A dictionary representing a dataset from CMG MongoDB

  Returns:
      tuple: A tuple containing the id and name of the matching Dataset, or None if not found
  """
  cmg_id = str(mongo_item.get('_id'))

  if not cmg_id:
    logger.warning(f"MongoDB item does not have an _id field: {mongo_item}")
    return None

  # Find the matching dataset based on cmg_id
  pg_cursor.execute(
    """
    SELECT id, name
    FROM dataset
    WHERE cmg_id = %s
    """,
    cmg_id
  )
  matching_dataset = pg_cursor.fetchone()

  if not matching_dataset:
    logger.warning(f"Dataset with CMG ID {cmg_id} not found in Bioloop")
    return None

  return matching_dataset[0], matching_dataset[1]  # Return id and name of the matching dataset

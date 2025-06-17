from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
from typing import Literal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_corresponding_dataset(pg_cursor, mongo_item_id):
  """
  Find the Dataset in Bioloop that corresponds to a given CMG Dataset/Data Product.

  This function uses the `cmg_id` field on the Bioloop Dataset row to match with
  the `_id` field of the CMG Dataset.

  Args:
      pg_cursor: PostgreSQL cursor
      mongo_item_id (dict): A string representing a Dataset ID from CMG MongoDB

  Returns:
      tuple: A tuple containing the id and name of the matching Dataset, or None if not found
  """

  if not mongo_item_id:
    logger.warning(f"Provided CMG Mongo item does not have an _id field: {mongo_item_id}")
    return None

  mongo_id_str = str(mongo_item_id) if isinstance(mongo_item_id, ObjectId) else mongo_item_id

  # Find the matching dataset based on cmg_id
  pg_cursor.execute(
    """
    SELECT id, name
    FROM dataset
    WHERE cmg_id = %s
    """,
    (mongo_id_str,)
  )
  matching_dataset = pg_cursor.fetchone()

  if not matching_dataset:
    logger.warning(f"Dataset with CMG ID {matching_dataset} not found in Bioloop")
    return None

  return matching_dataset[0], matching_dataset[1]  # Return id and name of the matching dataset


def find_corresponding_user(pg_cursor: cursor, mongo_db: Database, mongo_user_id: ObjectId) -> int:
  """
  Find the corresponding user ID in PostgreSQL for a given MongoDB user ID.

  This function first retrieves the user from CMG (MongoDB), then finds the
  corresponding user in Bioloop (PostgreSQL) based on username, email, and cas_id.

  Args:
      pg_cursor (cursor): PostgreSQL cursor
      mongo_db (Database): MongoDB database object
      mongo_user_id (ObjectId): MongoDB user ID

  Returns:
      int: Corresponding user ID in PostgreSQL, or None if not found
  """
  # Retrieve user from CMG (MongoDB)
  cmg_user = mongo_db.users.find_one({"_id": mongo_user_id})

  if not cmg_user:
    logger.warning(f"User with ID {mongo_user_id} not found in CMG (MongoDB)")
    return None

  # Extract relevant fields
  username = cmg_user.get('username')
  email = cmg_user.get('email')
  cas_id = cmg_user.get('username')

  # Find corresponding user in Bioloop (PostgreSQL)
  pg_cursor.execute(
    """
    SELECT id 
    FROM "user" 
    WHERE username = %s 
       AND email = %s 
       AND cas_id = %s
    """,
    (username, email, cas_id)
  )
  result = pg_cursor.fetchone()

  if result:
    return result[0]
  else:
    logger.warning(f"No corresponding user found in Bioloop for CMG user: {username}")
    return None

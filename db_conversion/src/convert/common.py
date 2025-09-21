import logging
import re
from typing import Any, Generator

from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from .exceptions.exceptions import (CMGDatasetNotFoundException,
                                    CMGProjectNotFoundException,
                                    CMGUserNotFoundException)

logger = logging.getLogger(__name__)


def find_corresponding_bioloop_project(pg_cursor: cursor, cmg_project_id: ObjectId) -> dict | None:
  """
  Find the Project in Bioloop that corresponds to a given CMG Project.

  Args:
      pg_cursor (cursor): PostgreSQL cursor
      cmg_project_id (ObjectId): MongoDB project ID

  Returns:
      tuple: A tuple containing the id and name of the matching Project, or None if not found
  """
  if not cmg_project_id:
    # # logger.warning(f"Provided CMG Mongo project does not have an _id field: {mongo_project_id}")
    # return None, None
    raise ValueError(f"Provided CMG Mongo project does not have an _id field: {cmg_project_id}")

  cmg_project_id_str: str = str(cmg_project_id) if isinstance(cmg_project_id, ObjectId) else cmg_project_id
  # logger.info(f"Finding corresponding project for CMG ID: {cmg_project_id_str}")
  pg_cursor.execute(
    """
    SELECT * FROM project WHERE cmg_id = %s""",
    (cmg_project_id_str,)
  )
  matching_bioloop_project: dict | None = pg_cursor.fetchone() or None  
  # logger.info(f"Found corresponding Project for CMG ID: {cmg_project_id_str}: (Id: {matching_bioloop_project['id']})")

  if matching_bioloop_project is None:
    # # logger.info(f"Project with CMG ID {cmg_project_id_str} not found in Bioloop")
    raise CMGProjectNotFoundException(f"Project with CMG ID {cmg_project_id_str} not found in Bioloop")

  # Return ID of the matching Project
  return matching_bioloop_project


def find_corresponding_bioloop_dataset(pg_cursor, cmg_dataset_id) -> dict | None:
  """
  Find the Dataset in Bioloop that corresponds to a given CMG Dataset/Data Product.

  This function uses the `cmg_id` field on the Bioloop Dataset row to match with
  the `_id` field of the CMG Dataset.

  Args:
      pg_cursor: PostgreSQL cursor
      cmg_dataset_id (dict): A string representing a Dataset ID from CMG MongoDB

  Returns:
      tuple: A tuple containing the id and name of the matching Dataset, or None if not found
  """

  if not cmg_dataset_id:
    # # logger.warning(f"Provided CMG Mongo item does not have an _id field: {cmg_dataset_id}")
    # return None, None
    raise ValueError(f"Provided CMG Mongo item does not have an _id field: {cmg_dataset_id}")

  cmg_id_str: str = str(cmg_dataset_id) if isinstance(cmg_dataset_id, ObjectId) else cmg_dataset_id
  # logger.info(f"Finding corresponding dataset for CMG ID: {cmg_id_str}")

  # Find the matching dataset based on cmg_id
  pg_cursor.execute(
    """
    SELECT *
    FROM dataset
    WHERE cmg_id = %s
    """,
    (cmg_id_str,)
  )
  matching_bioloop_dataset: dict | None = pg_cursor.fetchone() or None

  if matching_bioloop_dataset is None:
    # logger.info(f"Dataset with CMG ID {cmg_id_str} not found in Bioloop")
    raise CMGDatasetNotFoundException(f"Dataset with CMG ID {cmg_id_str} not found in Bioloop")

  # logger.info(f"Found corresponding Dataset for CMG ID: {cmg_id_str}: (Id: {matching_bioloop_dataset['id']}, Name: {matching_bioloop_dataset['name']})")
  # Return ID and name of the matching Dataset
  return matching_bioloop_dataset


def find_corresponding_bioloop_user(pg_cursor: cursor,
                                    mongo_db: Database,
                                    cmg_user_id: ObjectId,) -> dict | None:
  """
  Find the corresponding user ID in PostgreSQL for a given MongoDB user ID.

  This function first retrieves the user from CMG (MongoDB), then finds the
  corresponding user in Bioloop (PostgreSQL) based on username, email, and cas_id.

  Args:
      pg_cursor (cursor): PostgreSQL cursor
      mongo_db (Database): MongoDB database object
      cmg_user_id (ObjectId): MongoDB user ID

  Returns:
      tuple[Any]: Corresponding user ID in PostgreSQL, or None if not found
  """

  # Find corresponding user in Bioloop (PostgreSQL)
  pg_cursor.execute(
    """
    SELECT * 
    FROM "user" 
    WHERE cmg_id = %s 
    """,
    (str(cmg_user_id),)
  )
  result: dict | None = pg_cursor.fetchone()

  if not result:
    raise CMGUserNotFoundException(f"No corresponding user found in Bioloop for CMG user ID: {cmg_user_id}")

  return result


NATO_ALPHABET = [
    'alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel',
    'india', 'juliet', 'kilo', 'lima', 'mike', 'november', 'oscar', 'papa',
    'quebec', 'romeo', 'sierra', 'tango', 'uniform', 'victor', 'whiskey',
    'xray', 'yankee', 'zulu',
]

def normalize_project_name(name: str) -> str:
    return re.sub(r'-+', '-', re.sub(r'[\W_]+', '-', name.lower().strip()))


def identifier_suffix_gen(identifiers: list[str]) -> Generator[str, None, None]:
    i = 0
    N = len(identifiers)
    while True:
        if i < N:
            yield identifiers[i]
        else:
            yield f"{identifiers[i % N]}-{i // N}"
        i += 1


def is_slug_unique(pg_cursor: cursor, slug: str, project_id: str = None) -> bool:
    if project_id is not None:
        # Check if any OTHER project has this slug
        pg_cursor.execute(
            """
            SELECT id FROM project WHERE slug = %s AND cmg_id != %s LIMIT 1
            """,
            (slug, project_id)
        )
    else:
        # Check if ANY project has this slug
        pg_cursor.execute(
            """
            SELECT id FROM project WHERE slug = %s LIMIT 1
            """,
            (slug,)
        )
    result = pg_cursor.fetchone()
    return result is None


def generate_slug(pg_cursor: cursor, name: str, project_id: str) -> str:
    normalized_name = normalize_project_name(name)

    if is_slug_unique(pg_cursor, normalized_name, project_id):
        return normalized_name

    suffix_gen = identifier_suffix_gen(NATO_ALPHABET)
    while True:
        suffix = next(suffix_gen)
        slug = f"{normalized_name}-{suffix}"
        if is_slug_unique(pg_cursor, slug, project_id):
            return slug

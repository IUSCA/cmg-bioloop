from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
from typing import Literal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_corresponding_dataset(pg_cursor, mongo_item, dataset_type):
  """
    Find the Dataset in Bioloop that most closely matches a given CMG Dataset.

    Finding the closest match is necessary because a CMG Dataset/Data Product may have
    duplicates in the CMG MongoDB, since CMG does not enforce uniqueness on Datasets/Data Product
    collections. Therefore, to retain the duplicate Datasets from CMG in Bioloop while
    converting CMG's duplicate Datasets to Bioloop Raw Data/Data Product, we rename CMG's
    duplicate Datasets before inserting it into Bioloop PostgresSQL (to maintain Bioloop Postgres's
    unique composite key (`name`, `type`, `is_deleted`) constraint on Bioloop's `dataset` table).
    As a result, in case of converting CMG's duplicate Datasets/Data Products to Bioloop's
    Raw Data/Data Products, it may not be possible to find the corresponding Dataset/Data Product
    in the converted Bioloop Datasets based solely on the Dataset name.

    Example:
    Consider a scenario where CMG has two duplicate Datasets with the same name:

    CMG MongoDB:
    1. {
        "name": "Dataset_A",
        "description": "Original dataset",
        "directories": 5,
        "files": 100,
        "du_size": 1000000,
        "size": 1000000,
        "createdAt": "2023-01-01T00:00:00Z",
        "updatedAt": "2023-01-02T00:00:00Z",
        "paths": {"origin": "/path/to/dataset_a", "archive": "/archive/dataset_a"},
        "staged": True
    }
    2. {
        "name": "Dataset_A",
        "description": "Duplicate dataset",
        "directories": 6,
        "files": 120,
        "du_size": 1200000,
        "size": 1200000,
        "createdAt": "2023-01-03T00:00:00Z",
        "updatedAt": "2023-01-04T00:00:00Z",
        "paths": {"origin": "/path/to/dataset_a_duplicate", "archive": "/archive/dataset_a_duplicate"},
        "staged": False
    }

    During migration to Bioloop, these datasets would be converted as follows, with the name inserted in Bioloop modified:

    Bioloop PostgreSQL:
    1. {
        "name": "Dataset_A",
        "description": "Original dataset",
        "num_directories": 5,
        "num_files": 100,
        "du_size": 1000000,
        "size": 1000000,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-02T00:00:00Z",
        "origin_path": "/path/to/dataset_a",
        "archive_path": "/archive/dataset_a",
        "is_staged": True
    }
    2. {
        "name": "Duplicate_Dataset_A",
        "description": "Duplicate dataset",
        "num_directories": 6,
        "num_files": 120,
        "du_size": 1200000,
        "size": 1200000,
        "created_at": "2023-01-03T00:00:00Z",
        "updated_at": "2023-01-04T00:00:00Z",
        "origin_path": "/path/to/dataset_a_duplicate",
        "archive_path": "/archive/dataset_a_duplicate",
        "is_staged": False
    }

    When converting CMG Events to Bioloop Audit Logs for the second CMG Dataset, this function would need to
    find the corresponding Bioloop Dataset to associate the Audit Logs with. It can't rely solely on the name
    to find the corresponding Bioloop Dataset of either of the two CMG duplicates, as the name of the corresponding
    Dataset inserted into Bioloop
    has been modified compared to the name of the corresponding duplicate Dataset in CMG, before
    conversion of this CMG Dataset from CMG to Bioloop. Therefore, this method compares various attributes like
    description, number of files, size, creation date, etc., to determine the closest
    match of the CMG Dataset. In this case, it would match
    the second CMG dataset with the "Duplicate_Dataset_A" in Bioloop based on these attributes.

    The scoring system used in the method to find the closet match helps to:
    1. Handle cases where datasets were duplicated during migration.
    2. Match datasets that may have been renamed but retain other similar attributes.
    3. Ensure that audit logs are associated with the correct Dataset in Bioloop.

    Args:
        mongo_item (dict): A dictionary representing a dataset from CMG MongoDB
        matching_datasets (list): A list of tuples, each representing a potential matching dataset from Bioloop PostgreSQL

    Returns:
        tuple: A tuple containing the id and name of the best matching Dataset

    Raises:
        ValueError: If no matching dataset is found or if multiple datasets have the same highest match score
    """

  original_name = mongo_item["name"]
  is_deleted = not mongo_item.get("visible", True)

  # Find all possible matches (original and duplicates)
  pg_cursor.execute(
    """
    SELECT id, name, type, is_deleted, description, num_directories, num_files, 
           du_size, size, created_at, updated_at, origin_path, archive_path, is_staged
    FROM dataset
    WHERE (name = %s OR name LIKE %s)
    AND type = %s AND is_deleted = %s
    """,
    (original_name, f"Duplicate_%{original_name}", dataset_type, is_deleted)
  )
  matching_datasets = pg_cursor.fetchall()

  if not matching_datasets:
    logger.warning(f"Dataset {original_name} not found in Bioloop")
    return None

  try:
    return find_best_match(mongo_item, matching_datasets)
  except ValueError as e:
    logger.warning(str(e))
    return None


def find_best_match(mongo_item, matching_datasets):
  best_matches = []
  best_match_score = -1

  for dataset in matching_datasets:
    match_score = compare_datasets(mongo_item, dataset)
    if match_score > best_match_score:
      best_matches = [dataset]
      best_match_score = match_score
    elif match_score == best_match_score:
      best_matches.append(dataset)

  if len(best_matches) == 0:
    raise ValueError(f"No matching dataset found for {mongo_item['name']}")
  elif len(best_matches) > 1:
    raise ValueError(f"Multiple identical matches found for {mongo_item['name']}. Skipping hierarchy creation.")

  return best_matches[0][0], best_matches[0][1]  # Return id and name of the best match


def compare_datasets(mongo_item, postgres_dataset):
  """
    Compare a CMG MongoDB dataset with a Bioloop PostgreSQL dataset and return a similarity score.

    This function is crucial in the process of matching CMG datasets to their corresponding
    Bioloop datasets, when dealing with duplicates or renamed datasets during
    the migration process.

    The function compares various attributes of the CMG datasets and the Bioloop datasets and
    increments a score for each matching attribute. This scoring system allows for a more flexible and robust
    matching process, as it can handle cases where some attributes might have changed during migration.

    Attributes compared:
    1. Name (exact match between the Bioloop and CMG dataset, or ends with CMG dataset's original name)
    2. Visibility/Deletion status
    3. Description
    4. Number of directories
    5. Number of files
    6. Disk usage size
    7. Size
    8. Creation timestamp
    9. Update timestamp
    10. Origin path
    11. Archive path
    12. Staged status

    Each matching attribute contributes 1 point to the overall score. The higher the score,
    the more likely the datasets are to be corresponding entries.

    Args:
        mongo_item (dict): A dictionary representing a dataset from CMG MongoDB.
            Expected keys:
            - name (str): Name of the dataset
            - visible (bool, optional): Visibility status
            - description (str, optional): Dataset description
            - directories (int, optional): Number of directories
            - files (int, optional): Number of files
            - du_size (int, optional): Disk usage size
            - size (int, optional): Dataset size
            - createdAt (datetime, optional): Creation timestamp
            - updatedAt (datetime, optional): Update timestamp
            - paths (dict, optional): Contains 'origin' and 'archive' paths
            - staged (bool, optional): Staged status

        postgres_dataset (tuple): A tuple representing a dataset from Bioloop PostgreSQL.
            Expected order of elements:
            0: id, 1: name, 2: type, 3: is_deleted, 4: description,
            5: num_directories, 6: num_files, 7: du_size, 8: size,
            9: created_at, 10: updated_at, 11: origin_path, 12: archive_path, 13: is_staged

    Returns:
        int: The similarity score between the two datasets. A higher score indicates
             a closer match.

    Note:
        This function assumes that the structure of both mongo_item and postgres_dataset
        follows the expected format. Any deviations from this format may lead to
        KeyErrors or IndexErrors.
    """
  score = 0

  (
    postgres_id, postgres_name, postgres_type, postgres_is_deleted, postgres_description,
    postgres_num_directories, postgres_num_files, postgres_du_size, postgres_size,
    postgres_created_at, postgres_updated_at, postgres_origin_path, postgres_archive_path, postgres_is_staged
  ) = postgres_dataset

  # Compare fields that exist in both CMG and Bioloop
  if mongo_item['name'] == postgres_name or postgres_name.endswith(f"_{mongo_item['name']}"):
    score += 1
  if mongo_item.get('visible', False) == postgres_is_deleted:
    score += 1
  if mongo_item.get('description') == postgres_description:
    score += 1
  if mongo_item.get('directories', 0) == postgres_num_directories:
    score += 1
  if mongo_item.get('files', 0) == postgres_num_files:
    score += 1
  if mongo_item.get('du_size', 0) == postgres_du_size:
    score += 1
  if mongo_item.get('size', 0) == postgres_size:
    score += 1
  if mongo_item.get('createdAt') == postgres_created_at:
    score += 1
  if mongo_item.get('updatedAt') == postgres_updated_at:
    score += 1
  if mongo_item.get('paths', {}).get('origin') == postgres_origin_path:
    score += 1
  if mongo_item.get('paths', {}).get('archive') == postgres_archive_path:
    score += 1
  if mongo_item.get('staged', False) == postgres_is_staged:
    score += 1

  return score

from bson.objectid import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
from typing import Literal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def events_to_audit_logs(pg_cursor: cursor, mongo_db: Database):
  for dataset_type in ["RAW_DATA", "DATA_PRODUCT"]:
    collection = mongo_db.dataproducts if dataset_type == "DATA_PRODUCT" else mongo_db.datasets

    for mongo_item in collection.find():
      original_name = mongo_item["name"]
      is_deleted = mongo_item.get("visible", False)

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
        continue

      try:
        # Determine which dataset this mongo_item corresponds to
        dataset_id, dataset_name = find_corresponding_dataset(mongo_item, matching_datasets)

        # Convert CMG Events to this Bioloop Audit Logs for this Dataset
        for event in mongo_item.get('events', []):
          action = event.get('description')
          timestamp = event.get('stamp')

          pg_cursor.execute(
            """
            INSERT INTO dataset_audit (action, timestamp, dataset_id)
            VALUES (%s, %s, %s)
            """,
            (action, timestamp, dataset_id)
          )

        logger.info(f"Created audit logs for dataset: {dataset_name} (id: {dataset_id})")
      except ValueError as e:
        logger.warning(str(e))


def find_corresponding_dataset(mongo_item, matching_datasets):
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
    raise ValueError(f"Multiple identical matches found for {mongo_item['name']}. Skipping audit log creation.")

  return best_matches[0][0], best_matches[0][1]  # Return id and name of the best match


def compare_datasets(mongo_item, postgres_dataset):
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

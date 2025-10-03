import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import (find_corresponding_bioloop_dataset,
                      find_corresponding_bioloop_user)

logger = logging.getLogger(__name__)

def _get_conversion_definition_id(pg_cursor: cursor, pipeline_name: str) -> int:
  if not pipeline_name:
    raise Exception(f"Pipeline name must be specified")
  pg_cursor.execute(
    """
    SELECT id FROM conversion_definition WHERE name = %s
    """,
    (pipeline_name,)
  )
  row = pg_cursor.fetchone()
  if row is not None:
    row = row['id']
  else:
    raise Exception(f"Conversion definition not found for pipeline: {pipeline_name}")
  return row


def _insert_conversion(pg_cursor: cursor,
                       definition_id: int,
                       dataset_id: int,
                       initiator_id: Optional[int],
                       initiated_at) -> int:
  pg_cursor.execute(
    """
    INSERT INTO conversion (initiated_at, definition_id, workflow_id, dataset_id, initiator_id)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id
    """,
    (initiated_at, definition_id, None, dataset_id, initiator_id)
  )
  return pg_cursor.fetchone()['id']


def _link_derived_datasets(pg_cursor: cursor,
                           mongo_db: Database,
                           conversion_id: int,
                           cmg_conversion_id: ObjectId):
  # Gather CMG dataproducts that were created by this conversion, find the corresponding Bioloop datasets, and
  # link them to the corresponding Bioloop conversion

  conversion_association_data = []

  for dp in mongo_db.dataproducts.find({ 'conversion': cmg_conversion_id }):
    bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, dp['_id'])
    if not bioloop_dataset:
      logger.warning(f"No corresponding Bioloop dataset found for CMG dataproduct: {dp['_id']}")
      continue
    bioloop_dataset_id = bioloop_dataset['id']
    conversion_association_data.append((conversion_id, bioloop_dataset_id))

  pg_cursor.executemany(
    """
    INSERT INTO conversion_derived_dataset (conversion_id, dataset_id)
    VALUES (%s, %s)
    """,
    conversion_association_data
  )


def convert_conversions(pg_cursor: cursor, mongo_db: Database):
  """
  Convert CMG conversions (Mongo) to Bioloop conversions (Postgres).

  Rules:
  - definition_id: exact match on conversion_definition.name == cmg_conversion.pipeline
  - dataset_id: Bioloop dataset where dataset.cmg_id == CMG conversion.dataset _id (RAW_DATA)
  - initiator_id: map CMG conversion.user to Bioloop user (username/cas_id)
  - initiated_at: CMG createdAt (fallback: updatedAt or now)
  - Link derived DATA_PRODUCT datasets via conversion_derived_dataset using CMG dataproduct.conversion
  """

  for conv in mongo_db.conversions.find():
    pipeline = conv.get('pipeline')
    definition_id = _get_conversion_definition_id(pg_cursor, pipeline)
    if not definition_id:
      raise Exception(f"Conversion definition not found for pipeline: {pipeline}")

    # Map source dataset (RAW_DATA)
    src_dataset = conv.get('dataset')
    src_dataset = find_corresponding_bioloop_dataset(pg_cursor, src_dataset)
    if not src_dataset:
      raise Exception(f"Source dataset not found for conversion: {conv['_id']}")
    dataset_id = src_dataset['id']

    # Map initiator
    initiator_id = None
    if conv.get('user'):
      initiator = find_corresponding_bioloop_user(pg_cursor, mongo_db, conv.get('user'))
      initiator_id = initiator['id']
      if not initiator_id:
        raise Exception(f"Initiator not found for conversion: {conv['_id']}")

    # Timestamps
    initiated_at = conv.get('createdAt') or conv.get('updatedAt') or datetime.utcnow()

    # Insert conversion
    conversion_id = _insert_conversion(
      pg_cursor=pg_cursor,
      definition_id=definition_id,
      dataset_id=dataset_id,
      initiator_id=initiator_id,
      initiated_at=initiated_at,
    )

    # Link derived datasets created by this conversion
    _link_derived_datasets(pg_cursor, mongo_db, conversion_id, conv['_id'])



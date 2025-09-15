from datetime import datetime
from typing import Optional

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_dataset, find_corresponding_user


def _get_conversion_definition_id(pg_cursor: cursor, pipeline_name: str) -> Optional[int]:
  if not pipeline_name:
    return None
  pg_cursor.execute(
    """
    SELECT id FROM conversion_definition WHERE name = %s
    """,
    (pipeline_name,)
  )
  row = pg_cursor.fetchone()
  if not row:
    raise Exception(f"Conversion definition not found for pipeline: {pipeline_name}")
  return row[0] if row else None


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
  return pg_cursor.fetchone()[0]


def _link_derived_datasets(pg_cursor: cursor,
                           mongo_db: Database,
                           conversion_id: int,
                           cmg_conversion_id: ObjectId):
  # Link all CMG dataproducts that reference this conversion to the inserted Bioloop conversion
  for dp in mongo_db.dataproducts.find({ 'conversion': cmg_conversion_id }):
    bioloop_dataset = find_corresponding_dataset(pg_cursor, dp['_id'])
    if not bioloop_dataset:
      continue
    bioloop_dataset_id, _ = bioloop_dataset

    # Check if the record already exists
    pg_cursor.execute(
        "SELECT 1 FROM conversion_derived_dataset WHERE conversion_id = %s AND dataset_id = %s",
        (conversion_id, bioloop_dataset_id)
    )
    if pg_cursor.fetchone():
        raise Exception(f"Record already exists: conversion_id={conversion_id}, dataset_id={bioloop_dataset_id}")

    # Link the derived dataset to the conversion 
    pg_cursor.execute(
      """
      INSERT INTO conversion_derived_dataset (conversion_id, dataset_id)
      VALUES (%s, %s)
      """,
      (conversion_id, bioloop_dataset_id)
    )


def convert_conversions(pg_cursor: cursor, mongo_db: Database):
  """
  Convert CMG conversions (Mongo) to Bioloop conversions (Postgres).

  Rules:
  - definition_id: exact match on conversion_definition.name == cmg_conversion.pipeline
  - dataset_id: Bioloop dataset where dataset.cmg_id == CMG conversion.dataset _id (RAW_DATA)
  - initiator_id: map CMG conversion.user to Bioloop user (username/cas_id)
  - initiated_at: CMG createdAt (fallback: updatedAt or now)
  - Do NOT populate additional_args
  - Link derived DATA_PRODUCT datasets via conversion_derived_dataset using CMG dataproduct.conversion
  """

  for conv in mongo_db.conversions.find():
    pipeline = conv.get('pipeline')
    definition_id = _get_conversion_definition_id(pg_cursor, pipeline)
    if not definition_id:
      raise Exception(f"Conversion definition not found for pipeline: {pipeline}")

    # Map source dataset (RAW_DATA)
    src_dataset = conv.get('dataset')
    src_dataset = find_corresponding_dataset(pg_cursor, src_dataset)
    if not src_dataset:
      raise Exception(f"Source dataset not found for conversion: {conv['_id']}")
    dataset_id, _ = src_dataset

    # Map initiator
    initiator_id = None
    if conv.get('user'):
      initiator_id = find_corresponding_user(pg_cursor, mongo_db, conv.get('user'))
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



import logging
import os
import pprint
import sys
from datetime import datetime
from typing import Optional

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import (find_corresponding_bioloop_dataset,
                      find_corresponding_bioloop_user)
from ..constants.common import ARGUMENT_DATA

from..constants.common import (CMD_LINE_PROGRAMS,
                                           OTHER_PROGRAM_NAMES)
from ..constants.common import CONVERSION_DEFINITIONS
from ..exceptions.exceptions import (CMGDatasetNotFoundException,
                                     CMGUserNotFoundException)
from .user import get_bioloop_cmguser_id

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
    # logger.info(f"Conversion definition found for pipeline: {pipeline_name}, ID: {row['id']}")
    row = row['id']
  else:
    raise Exception(f"Conversion definition not found for pipeline: {pipeline_name}")
  return row

def _populate_pipeline_definitions(pg_cursor: cursor):
    """
    Populate cmd_line_program, conversion_definition, and argument tables.
    Implements the same logic as api/prisma/seed.js lines 317-381.
    """

    # logger.info("Populating pipeline definitions...")

    cmg_bioloop_user_id = get_bioloop_cmguser_id(pg_cursor)

    # Add the workers directory to the Python path to import constants
    workers_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'workers')
    sys.path.insert(0, workers_path)
    
    # logger.info("Starting pipeline definitions population...")
    
    # Create cmd_line_programs (equivalent to seed.js lines 321-324)
    # logger.info("Inserting cmd_line_programs...")
    for program in CMD_LINE_PROGRAMS:
        # logger.info(f"Inserting cmd_line_program:")
        # logger.info(pprint.pformat(program))
        pg_cursor.execute(
            """
            INSERT INTO cmd_line_program (name, executable_path, executable_directory, allow_additional_args)
            VALUES (%s, %s, %s, %s)
            """,
            (program['name'], program['executable_path'], program['executable_directory'], program['allow_additional_args'])
        )
    
    # Get the inserted programs to map their IDs
    # logger.info("Creating program name to ID mapping...")
    pg_cursor.execute("SELECT id, name FROM cmd_line_program")
    programs = pg_cursor.fetchall()
    program_map = {}
    for program in programs:
        # logger.info(f"Program:")
        # logger.info(pprint.pformat(program))
        program_map[program['name']] = program['id']
    
    # Update conversion definitions with program_id references
    # logger.info("Inserting conversion_definitions...")
    for definition in CONVERSION_DEFINITIONS:
        # logger.info(f"Inserting conversion_definition:")
        # logger.info(pprint.pformat(definition))
        pg_cursor.execute(
            """
            INSERT INTO conversion_definition (name, description, enabled, dataset_types, tags, capture_logs, output_directory, program_id, author_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                definition['name'],
                definition['description'], 
                definition['enabled'],
                definition['dataset_types'],
                definition['tags'],
                definition['capture_logs'],
                definition['output_directory'],
                program_map[definition['name']],
                cmg_bioloop_user_id,
            )
        )
    
    # Update arguments with program_id references
    # logger.info("Inserting arguments...")
    argument_data_with_programs = []
    
    # bcl2fastq links to all args
    bcl2fastq_program_id = program_map['bcl2fastq']
    for arg in ARGUMENT_DATA:
        # logger.info(f"Inserting argument:")
        # logger.info(pprint.pformat(arg))
        argument_data_with_programs.append({
            **arg,
            'program_id': bcl2fastq_program_id
        })
    
    # Other programs link to specific shared args: --no-lane-splitting, --delete-undetermined, --filter-single-index
    shared_arg_names = ['--no-lane-splitting', '--delete-undetermined', '--filter-single-index']
    conversion_programs_shared_args = [arg for arg in ARGUMENT_DATA if arg['name'] in shared_arg_names]
    # logger.info(f"Conversion programs shared args:")
    # logger.info(pprint.pformat(conversion_programs_shared_args))
    
    # logger.info(f"Inserting other program names:")
    for program_name in OTHER_PROGRAM_NAMES:
        # logger.info(f"Inserting other program name:")
        # logger.info(pprint.pformat(program_name))
        program_id = program_map.get(program_name)
        if program_id:
            for arg in conversion_programs_shared_args:
                argument_data_with_programs.append({
                    **arg,
                    'program_id': program_id
                })
    
    # logger.info(f"Inserting argument data with programs:")
    # Insert all arguments
    for arg in argument_data_with_programs:
        # logger.info(f"Inserting argument data with program:")
        # logger.info(pprint.pformat(arg))
        pg_cursor.execute(
            """
            INSERT INTO argument (name, value_type, allowed_values, is_required, default_value, is_flag, description, 
                                min_value, max_value, min_length, max_length, position, dynamic_variable_name, program_id)
            VALUES (%s, %s::argument_value_type, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                arg['name'],
                arg['value_type'],
                arg['allowed_values'],
                arg['is_required'],
                arg['default_value'],
                arg['is_flag'],
                arg['description'],
                arg['min_value'],
                arg['max_value'],
                arg['min_length'],
                arg['max_length'],
                arg['position'],
                arg['dynamic_variable_name'],
                arg['program_id']
            )
        )
    
    # logger.info("Pipeline definitions population completed successfully!")

def _insert_conversion(pg_cursor: cursor,
                       definition_id: int,
                       dataset_id: int,
                       initiator_id: Optional[int],
                       initiated_at) -> int:
  # logger.info(f"Inserting conversion: {definition_id}, {dataset_id}, {initiator_id}, {initiated_at}")
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

  # logger.info(f"Linking derived datasets for Bioloop conversion: {conversion_id}, CMG conversion: {cmg_conversion_id}")

  conversion_association_data = []

  for dp in mongo_db.dataproducts.find({ 'conversion': cmg_conversion_id }):
    # logger.info(f"CMG dataproduct: {dp['_id']}, name: {dp['name']}")
    bioloop_dataset = find_corresponding_bioloop_dataset(pg_cursor, dp['_id'])
    if not bioloop_dataset:
      # logger.warning(f"No corresponding Bioloop dataset found for CMG dataproduct: {dp['_id']}")
      continue
    bioloop_dataset_id = bioloop_dataset['id']
    # logger.info(f"Bioloop dataset: {bioloop_dataset_id}, name: {bioloop_dataset['name']}")
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

  print("populate pipeline definitions")
  _populate_pipeline_definitions(pg_cursor)

  # logger.info("Converting conversions...")
  for conv in mongo_db.conversions.find():
    # logger.info(f"Converting conversion: {conv['_id']}, pipeline: {conv.get('pipeline')}, dataset: {conv.get('dataset')}")
    pipeline = conv.get('pipeline')
    # logger.info(f"Pipeline ID: {pipeline}")
    definition_id = _get_conversion_definition_id(pg_cursor, pipeline)
    # logger.info(f"Definition ID: {definition_id}")
    if not definition_id:
      raise Exception(f"Conversion definition not found for pipeline: {pipeline}")

    # Map source dataset (RAW_DATA)
    src_dataset = conv.get('dataset')
    # logger.info(f"Source dataset: {src_dataset}")
    bioloop_src_dataset_id = None
    try:
      bioloop_src_dataset = find_corresponding_bioloop_dataset(pg_cursor, src_dataset)
      # logger.info(f"Bioloop Dataset:")
      # logger.info(pprint.pformat(bioloop_src_dataset))
      bioloop_src_dataset_id = bioloop_src_dataset['id']
      # logger.info(f"Bioloop Source Dataset ID: {bioloop_src_dataset_id}")
    except CMGDatasetNotFoundException as e:
      # logger.warning(f"No corresponding dataset found for CMG dataset: {src_dataset}")
      continue
    
    # Map initiator
    initiator_id = None
    if conv.get('user'):
      # logger.info(f"Initiator: {conv.get('user')}")
      try:
        initiator = find_corresponding_bioloop_user(pg_cursor, mongo_db, conv.get('user'))
        # logger.info(f"Bioloop User ID: {initiator['id']}, username: {initiator['username']}")
        initiator_id = initiator['id']
      except CMGUserNotFoundException as e:
        pass
        # logger.warning(f"No corresponding user found for CMG user: {conv.get('user')}")
        # continue

    # Timestamps
    initiated_at = conv.get('createdAt') or conv.get('updatedAt') or datetime.utcnow()

    # logger.info("Inserting conversion:")
    # logger.info(f"Definition ID: {definition_id}, Dataset ID: {bioloop_src_dataset_id}, Initiator ID: {initiator_id}, Initiated At: {initiated_at}")

    # Insert conversion
    conversion_id = _insert_conversion(
      pg_cursor=pg_cursor,
      definition_id=definition_id,
      dataset_id=bioloop_src_dataset_id,
      initiator_id=initiator_id,
      initiated_at=initiated_at,
    )
    # logger.info(f"Bioloop conversion ID: {conversion_id}")

    # Link derived datasets created by this conversion
    # logger.info(f"Linking derived datasets for Bioloop conversion: {conversion_id}, CMG conversion: {conv['_id']}")
    _link_derived_datasets(pg_cursor, mongo_db, conversion_id, conv['_id'])

  # logger.info("Conversions converted successfully!")

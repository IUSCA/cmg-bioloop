import logging
from typing import List, Optional

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import (find_corresponding_bioloop_dataset,
                      find_corresponding_bioloop_user)
from ..exceptions.exceptions import (CMGDatasetNotFoundException,
                                     CMGFileNotFoundException,
                                     CMGIndeterminateFileException,
                                     CMGUserNotFoundException)

logger = logging.getLogger(__name__)


def _find_dataset_file_by_name(pg_cursor: cursor, dataset_id: int, filename: str) -> Optional[int]:
  # Match on file name if present
  pg_cursor.execute(
    """
    SELECT * FROM dataset_file
    WHERE dataset_id = %s AND name = %s
    """,
    (dataset_id, filename)
  )
  matches = []
  for row in pg_cursor.fetchall():
    file_id = row['id']
    matches.append(file_id)

  if len(matches) == 0:
    raise CMGFileNotFoundException(f"No dataset_file matched file name '{filename}' in dataset_id {dataset_id}")
  if len(matches) > 1:
    raise CMGIndeterminateFileException(f"Multiple dataset_files matched file name '{filename}' in dataset_id {dataset_id}: {matches}")

  return matches[0]


def convert_sessions(pg_cursor: cursor, mongo_db: Database):
  """
  Convert CMG sessions to Bioloop genome_browser_session and session_track.

  Rules:
  - Omit: is_public, internal_share, external_share, staging.requested, staging.notify
  - session_track: omit title, color, order
  - Tracks are derived from CMG session.tracks entries: find DATA_PRODUCT dataset by dataproduct id,
    then find dataset_file by filename basename, ensure track exists for that dataset_file.
  """

  for cmg_session in mongo_db.sessions.find():
    print(f"Converting session: {cmg_session.get('title')}, user: {cmg_session.get('user')}, genome: {cmg_session.get('genome')}, genome_type: {cmg_session.get('genome_type')}")
    # Map session owner
    user_id = None
    if cmg_session.get('user'):
      try:
        user = find_corresponding_bioloop_user(pg_cursor, mongo_db, cmg_session.get('user'))
        user_id = user['id']
      except CMGUserNotFoundException as e:
        logger.warning(f"No corresponding user found for CMG user: {cmg_session.get('user')}")
        continue

    title = cmg_session.get('title') or 'Genome Browser Session'
    genome = cmg_session.get('genome') or None
    genome_type = cmg_session.get('genome_type') or None

    print(f"Inserting session: {title}, genome: {genome}, genome_type: {genome_type}, user_id: {user_id}, access_count: {cmg_session.get('access_count', 0)}")
    # Insert session
    pg_cursor.execute(
      """
      INSERT INTO genome_browser_session (title, genome, genome_type, user_id, access_count)
      VALUES (%s, %s, %s, %s, %s)
      RETURNING id
      """,
      (title, genome, genome_type, user_id, cmg_session.get('access_count', 0))
    )
    session_id = pg_cursor.fetchone()['id']
    logger.info(f"Inserted session ID: {session_id}")

    session_track_associations = []

    # For each CMG track: map to dataset_file and create track + session_track
    for idx, tr in enumerate(cmg_session.get('tracks', [])):
      cmg_track_data_product_id = tr.get('dataproduct')
      if not cmg_track_data_product_id:
        raise Exception(f"Track missing dataproduct: {tr}")
      cmg_track_file_name = tr.get('filename')
      if not cmg_track_file_name:
        raise Exception(f"Track missing filename: {tr}")      

      logger.info(f"Converting track: {cmg_track_file_name}, dataproduct: {cmg_track_data_product_id}")

      # Find corresponding Bioloop DATA_PRODUCT
      bioloop_dataset_id = None
      try:
        bioloop_data_product = find_corresponding_bioloop_dataset(pg_cursor, cmg_track_data_product_id)
        bioloop_dataset_id = bioloop_data_product['id']
      except CMGDatasetNotFoundException:
        logger.warning(f"No corresponding data product found for CMG data product: {cmg_track_data_product_id}")
        continue
      logger.info(f"Bioloop DATA_PRODUCT: ID: {bioloop_data_product['id']}, Name: {bioloop_data_product['name']}, Type: {bioloop_data_product['type']}")

      # Find a dataset_file by file name
      bioloop_file_id = None
      try:
        bioloop_file = _find_dataset_file_by_name(pg_cursor, bioloop_dataset_id, cmg_track_file_name)
        bioloop_file_id = bioloop_file['id']
      except CMGFileNotFoundException:
        logger.warning(f"No corresponding dataset_file found for CMG dataset_file: {cmg_track_file_name}")
        continue
      except CMGIndeterminateFileException:
        logger.warning(f"Multiple dataset_files matched file name '{cmg_track_file_name}' in dataset_id {bioloop_dataset_id}: {bioloop_file}")
        continue

      # create track
      pg_cursor.execute(
        """
        INSERT INTO track (name, dataset_file_id)
        VALUES (%s, %s)
        """,
        (tr.get('title'), bioloop_file_id)
      )
      track_id = pg_cursor.fetchone()['id']
      logger.info(f"Inserted track ID: {track_id}")
      
      session_track_associations.append((session_id, track_id))
    
    # Associate all created tracks with the session
    pg_cursor.executemany(
      """
      INSERT INTO session_track (session_id, track_id)
      VALUES (%s, %s)
      """,
      session_track_associations
    )
    logger.info(f"Inserted {len(session_track_associations)} session_track associations")

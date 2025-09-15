from typing import List, Optional

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..common import find_corresponding_dataset, find_corresponding_user


def _ensure_track_for_dataset_file(pg_cursor: cursor, dataset_file_id: int, name: str) -> int:
  # Track has unique(dataset_file_id)
  pg_cursor.execute(
    """
    SELECT id FROM track WHERE dataset_file_id = %s
    """,
    (dataset_file_id,)
  )
  row = pg_cursor.fetchone()
  if not row:
    raise Exception(f"Track not found for dataset_file_id: {dataset_file_id}")
  if row:
    return row[0]

  pg_cursor.execute(
    """
    INSERT INTO track (name, dataset_file_id)
    VALUES (%s, %s)
    RETURNING id
    """,
    (name, dataset_file_id)
  )
  return pg_cursor.fetchone()[0]


def _find_dataset_file_by_basename(pg_cursor: cursor, dataset_id: int, basename: str) -> Optional[int]:
  # Match on basename of path or explicit name if present
  pg_cursor.execute(
    """
    SELECT id, COALESCE(name, ''), path FROM dataset_file
    WHERE dataset_id = %s
    """,
    (dataset_id,)
  )
  matches = []
  for file_id, name, path in pg_cursor.fetchall():
    candidate = name if name else path.split('/')[-1]
    if candidate == basename:
      matches.append(file_id)

  if len(matches) == 0:
    raise Exception(f"No dataset_file matched basename '{basename}' in dataset_id {dataset_id}")
    # return None
  if len(matches) == 1:
    return matches[0]
  raise Exception(f"Multiple dataset_files matched basename '{basename}' in dataset_id {dataset_id}: {matches}")


def convert_sessions(pg_cursor: cursor, mongo_db: Database):
  """
  Convert CMG sessions to Bioloop genome_browser_session and session_track.

  Rules:
  - Omit: is_public, internal_share, external_share, staging.requested, staging.notify
  - session_track: omit title, color, order
  - Tracks are derived from CMG session.tracks entries: find DATA_PRODUCT dataset by dataproduct id,
    then find dataset_file by filename basename, ensure track exists for that dataset_file.
  """

  for sess in mongo_db.sessions.find():
    # Map session owner
    user_id = None
    if sess.get('user'):
      user_id = find_corresponding_user(pg_cursor, mongo_db, sess.get('user'))

    title = sess.get('title') or 'Genome Browser Session'
    genome = sess.get('genome') or ''
    genome_type = sess.get('genome_type') or ''

    # Insert session
    pg_cursor.execute(
      """
      INSERT INTO genome_browser_session (title, genome, genome_type, user_id, access_count)
      VALUES (%s, %s, %s, %s, %s)
      RETURNING id
      """,
      (title, genome, genome_type, user_id, sess.get('access_count', 0))
    )
    session_id = pg_cursor.fetchone()[0]

    # For each CMG track: map to dataset_file and create track + session_track
    for idx, tr in enumerate(sess.get('tracks', [])):
      cmg_data_product_id = tr.get('dataproduct')
      filename = tr.get('filename')
      if not cmg_data_product_id:
        # continue
        raise Exception(f"Track missing dataproduct: {tr}")

      if not filename:
        # continue
        raise Exception(f"Track missing filename: {tr}")

      # Find DATA_PRODUCT dataset
      cmg_data_product = find_corresponding_dataset(pg_cursor, cmg_data_product_id)
      if not cmg_data_product:
        # continue
        raise Exception(f"DATA_PRODUCT not found for track: {tr}")
      dataset_id, _ = cmg_data_product

      # Find a dataset_file by basename
      file_id = _find_dataset_file_by_basename(pg_cursor, dataset_id, filename)
      if not file_id:
        continue

      # Ensure a track exists for this dataset_file
      track_name = tr.get('title') or filename
      track_id = _ensure_track_for_dataset_file(pg_cursor, file_id, track_name)

      # Create session_track (omit title, color, order)
      pg_cursor.execute(
        """
        INSERT INTO session_track (session_id, track_id)
        VALUES (%s, %s)
        """,
        (session_id, track_id)
      )

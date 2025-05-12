from ..utils import extract_directories, mongo_file_to_pg_file


def create_file_and_directories(pg_cursor, file, dataset_id):
  print("create_file_and_directories", file, dataset_id)
  pg_file = mongo_file_to_pg_file(file)
  directories = extract_directories(pg_file["path"])

  # Insert directories
  parent_id = None
  current_path = ""
  for dir_name in directories:
    current_path += dir_name + "/"
    pg_cursor.execute(
      """
      INSERT INTO dataset_file (name, path, dataset_id, filetype)
      VALUES (%s, %s, %s, 'directory')
      ON CONFLICT (path, dataset_id) DO NOTHING
      RETURNING id
      """,
      (dir_name, current_path.rstrip('/'), dataset_id)
    )
    dir_id = pg_cursor.fetchone()[0]

    if parent_id:
      pg_cursor.execute(
        """
        INSERT INTO dataset_file_hierarchy (parent_id, child_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (parent_id, dir_id)
      )
    parent_id = dir_id

  # Insert file
  pg_cursor.execute(
    """
    INSERT INTO dataset_file (name, path, md5, size, dataset_id, filetype)
    VALUES (%s, %s, %s, %s, %s, 'file')
    RETURNING id
    """,
    (
      pg_file["name"],
      pg_file["path"],
      pg_file["md5"],
      pg_file["size"],
      dataset_id,
    )
  )
  file_id = pg_cursor.fetchone()[0]

  # Link file to its parent directory
  if parent_id:
    pg_cursor.execute(
      """
      INSERT INTO dataset_file_hierarchy (parent_id, child_id)
      VALUES (%s, %s)
      """,
      (parent_id, file_id)
    )

  print(f"Inserted file {pg_file['name']} (id: {file_id})")
  print("create_file_and_directories successful.")
  return file_id

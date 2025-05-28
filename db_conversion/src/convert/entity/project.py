from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from ..common import find_corresponding_dataset


def get_users_from_project_groups(mongo_db: Database, group_ids: List[str]) -> List[str]:
  users = set()
  for group in mongo_db.groups.find({'_id': {'$in': [ObjectId(gid) for gid in group_ids]}}):
    users.update(group.get('users', []))
  return list(users)


def get_dataproduct_ids(mongo_db: Database, project_ids: List[str]) -> Dict[str, List[str]]:
  project_dataproduct_map = {str(project['_id']): project.get('dataproducts', [])
                             for project in
                             mongo_db.projects.find({'_id': {'$in': [ObjectId(pid) for pid in project_ids]}})}
  return project_dataproduct_map


def convert_projects(pg_cursor: cursor, mongo_db: Database):
  """
  Convert projects from CMG MongoDB to Bioloop PostgreSQL.

  This function populates the `project`, `project_user`, and `project_dataset` tables
  in Bioloop PostgreSQL based on the `projects` collection in CMG MongoDB.
  """
  cmg_projects = list(mongo_db.projects.find({}))
  project_ids = [str(project['_id']) for project in cmg_projects]

  # Batch insert projects
  project_data = [(
    project.get('name'),
    project.get('description'),
    f'dummy-{i + 1}',  # Generate unique slugs
    project.get('createdAt'),
    project.get('updatedAt')
  ) for i, project in enumerate(cmg_projects)]

  pg_cursor.executemany(
    """
    INSERT INTO project (name, description, slug, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s)
    """,
    project_data
  )

  # Fetch the inserted IDs
  pg_cursor.execute(
    """
    SELECT id FROM project
    WHERE slug LIKE 'dummy-%'
    ORDER BY slug
    """
  )
  inserted_project_ids = [row[0] for row in pg_cursor.fetchall()]

  # Create mapping of CMG project's _id to Bioloop project's id
  project_id_map = dict(zip(project_ids, inserted_project_ids))

  # Get all dataproducts for all projects
  project_dataproduct_map = get_dataproduct_ids(mongo_db, project_ids)

  # Batch insert project_dataset associations
  project_dataset_data = []
  for cmg_project_id, dataproduct_ids in project_dataproduct_map.items():
    bioloop_project_id = project_id_map[cmg_project_id]
    for dataproduct_id in dataproduct_ids:
      cmg_dataproduct = mongo_db.dataproducts.find_one({'_id': ObjectId(dataproduct_id)})
      if cmg_dataproduct:
        bioloop_dataset = find_corresponding_dataset(pg_cursor, cmg_dataproduct)
        if bioloop_dataset:
          project_dataset_data.append((bioloop_project_id, bioloop_dataset[0]))
        else:
          logger.warning(f"Corresponding dataset not found for dataproduct {cmg_dataproduct['name']}")
      else:
        logger.warning(f"Dataproduct not found for id {dataproduct_id}")

  pg_cursor.executemany(
    """
    INSERT INTO project_dataset (project_id, dataset_id)
    VALUES (%s, %s)
    """,
    project_dataset_data
  )

  # Get all users for all projects
  all_users = set()
  for project in cmg_projects:
    all_users.update(project.get('users', []))
    all_users.update(get_users_from_project_groups(mongo_db, project.get('groups', [])))

  # Fetch all relevant users from MongoDB
  user_map = {str(user['_id']): user for user in mongo_db.users.find({'_id': {'$in': list(all_users)}})}

  # Fetch all users from PostgreSQL and create a mapping from MongoDB ID to PostgreSQL ID
  pg_cursor.execute("SELECT id, cas_id FROM \"user\"")
  pg_user_map = {row[1]: row[0] for row in pg_cursor.fetchall() if row[1]}

  # Batch insert project_user associations
  project_user_data = []
  for project in cmg_projects:
    bioloop_project_id = project_id_map[str(project['_id'])]
    project_users = set(project.get('users', []))
    project_users.update(get_users_from_project_groups(mongo_db, project.get('groups', [])))
    for user_id in project_users:
      mongo_user = user_map.get(str(user_id))
      if mongo_user:
        cas_id = mongo_user.get('cas_id')
        if cas_id in pg_user_map:
          project_user_data.append((bioloop_project_id, pg_user_map[cas_id]))
        else:
          logger.warning(f"User with cas_id {cas_id} not found in PostgreSQL for MongoDB user {user_id}")
      else:
        logger.warning(f"User not found in MongoDB for id {user_id}")

  logger.info(f"Total users in MongoDB: {len(user_map)}")
  logger.info(f"Total users in PostgreSQL: {len(pg_user_map)}")
  logger.info(f"Total project-user associations created: {len(project_user_data)}")

  pg_cursor.executemany(
    """
    INSERT INTO project_user (project_id, user_id)
    VALUES (%s, %s)
    """,
    project_user_data
  )

  # After insertion, check how many rows were actually inserted
  pg_cursor.execute("SELECT COUNT(*) FROM project_user")
  inserted_count = pg_cursor.fetchone()[0]
  logger.info(f"Rows inserted into project_user: {inserted_count}")

  logger.info(f"Converted {len(cmg_projects)} projects")
  logger.info("Project conversion completed.")

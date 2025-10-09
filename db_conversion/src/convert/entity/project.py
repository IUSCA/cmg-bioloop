import logging
import pprint
from gettext import find
from typing import Any, Dict, List, Tuple

from bson import ObjectId
from psycopg2.extensions import cursor
from pymongo.database import Database

from ..exceptions.exceptions import (CMGDatasetNotFoundException,
                                     CMGProjectNotFoundException,
                                     CMGUserNotFoundException)

logger = logging.getLogger(__name__)

from ..common import (find_corresponding_bioloop_dataset,
                      find_corresponding_bioloop_project,
                      find_corresponding_bioloop_user, generate_slug)


def get_cmg_users_from_project_groups(mongo_db: Database, cmg_group_ids: List[str]) -> List[str]:
  users = set()
  for group in mongo_db.groups.find({'_id': {'$in': [ObjectId(gid) for gid in cmg_group_ids]}}):
    users.update(group.get('users', []))
  # logger.info(f"Found {len(users)} users from groups: {cmg_group_ids}")
  # logger.info(pprint.pformat(users))
  return list(users)


def convert_projects(pg_cursor: cursor, mongo_db: Database):
  """
  Convert projects from CMG MongoDB to Bioloop PostgreSQL.

  This function populates the `project`, `project_user`, and `project_dataset` tables
  in Bioloop PostgreSQL based on the `projects` collection in CMG MongoDB.
  """
  cmg_projects: List[Dict[str, Any]] = list(mongo_db.projects.find({}))
  # logger.info(f"Converting {len(cmg_projects)} projects")
  
  cmg_project_ids: List[str] = [str(project['_id']) for project in cmg_projects]
  # logger.info(f"Generated {len(cmg_project_ids)} project IDs")

  project_data: List[Tuple[str, str, str, str, str, str]] = [(
    project.get('name'),
    project.get('description'),
    generate_slug(pg_cursor, project.get('name'), str(project.get('_id'))),
    project.get('createdAt'),
    project.get('updatedAt'),
    project.get('browser', False) or False,
    str(project.get('_id'))
  ) for project in cmg_projects]
  # Batch insert Projects
  pg_cursor.executemany(
    """
    INSERT INTO project (name, description, slug, created_at, updated_at, browser_enabled, cmg_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """,
    project_data
  )

  # for project in project_data:
    # logger.info(f"Project: {project}")
    # logger.info(pprint.pformat(project))

  # logger.info(f"Inserted {len(project_data)} projects")

  # Initialize association lists outside the loop to accumulate all associations
  bioloop_project_data_product_associations = []
  bioloop_project_user_associations = []

  for cmg_project in cmg_projects:
    # logger.info(f"Converting project: {cmg_project.get('name')}")
    cmg_project_data_product_associations = cmg_project.get('dataproducts', [])
    # logger.info(f"CMG project data product associations: {cmg_project_data_product_associations}")
    cmg_project_user_associations = cmg_project.get('users', []) + \
                                    get_cmg_users_from_project_groups(mongo_db,
                                                                      cmg_project.get('groups', []))
    # logger.info(f"CMG project user associations: {cmg_project_user_associations}")

    try:
      bioloop_project = find_corresponding_bioloop_project(pg_cursor, cmg_project['_id'])
    except CMGProjectNotFoundException as e:
      logger.warning(f"No corresponding project found for CMG project: {cmg_project.get('name')}, Id: {cmg_project.get('_id')}")
      continue
    # logger.info(f"Bioloop project:")
    bioloop_project_id = bioloop_project['id']

    for cmg_data_product_id in cmg_project_data_product_associations:
      try:
        bioloop_data_product = find_corresponding_bioloop_dataset(pg_cursor, cmg_data_product_id)
        # logger.info(f"Bioloop data product:")
        # logger.info(pprint.pformat(bioloop_data_product))
        bioloop_project_data_product_associations.append((bioloop_project_id, bioloop_data_product['id']))
        # logger.info(f"Added data product association: {bioloop_project_id} - {bioloop_data_product['id']}")
      except CMGDatasetNotFoundException as e:
        logger.warning(f"No corresponding data product found for CMG data product: {cmg_data_product_id}")
        continue
    
    for cmg_user_id in cmg_project_user_associations:
      try:
        bioloop_user = find_corresponding_bioloop_user(pg_cursor, mongo_db, cmg_user_id)
        # logger.info(f"Bioloop user:")
        # logger.info(pprint.pformat(bioloop_user))
        bioloop_project_user_associations.append((bioloop_project_id, bioloop_user['id']))
        # logger.info(f"Added user association: {bioloop_project_id} - {bioloop_user['id']}")
      except CMGUserNotFoundException as e:
        logger.warning(f"No corresponding Bioloop user found for CMG user ID: {cmg_user_id}")
        continue
      
  # Batch insert Project-Dataset and Project-User associations
  # logger.info(f"Total project-dataset associations to insert: {len(bioloop_project_data_product_associations)}")
  # logger.info(f"Total project-user associations to insert: {len(bioloop_project_user_associations)}")
  
  if bioloop_project_data_product_associations:
    pg_cursor.executemany(
        """
        INSERT INTO project_dataset (project_id, dataset_id)
        VALUES (%s, %s)
        """,
        bioloop_project_data_product_associations
      )
  
  if bioloop_project_user_associations:
    pg_cursor.executemany(
        """
        INSERT INTO project_user (project_id, user_id)
        VALUES (%s, %s)
        """,
        bioloop_project_user_associations
      )
  
  pg_cursor.execute("SELECT COUNT(*) FROM project_user")
  inserted_count = pg_cursor.fetchone()['count']
  # logger.info(f"Rows inserted into project_user: {inserted_count}")

  # logger.info(f"Converted {len(cmg_projects)} projects")
  # logger.info("Project conversion completed.")

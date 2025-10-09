import logging
import os
import traceback

import psycopg2

from ..constants.bioloop import bioloop_roles
from ..constants.cmg import cmguser
from ..constants.common import role_mapping

logger = logging.getLogger(__name__)


def get_bioloop_cmguser_id(pg_cursor):
  # logger.info("get_cmguser_id")
  pg_cursor.execute(
    """
    SELECT id FROM "user" WHERE username = 'cmguser'
    """
  )
  cmg_user_id = pg_cursor.fetchone()
  if cmg_user_id is not None:
    cmg_user_id = cmg_user_id['id']
  else:
    # logger.info(f"Found cmguser_id: {cmg_user_id}")
    raise ValueError("User 'cmguser' not found in the PostgreSQL database")

  return cmg_user_id


def create_roles(pg_cursor):
  # logger.info("create_roles")
  for role in bioloop_roles:
    pg_cursor.execute(
      """
      INSERT INTO role (name, description)
      VALUES (%s, %s)
      """,
      (role['name'], role['description'])
    )

  # logger.info(f"Inserted or updated {len(bioloop_roles)} roles.")
  # logger.info("create_roles successful.")


def convert_users(pg_cursor, mongo_db):
  # logger.info("convert_users")
  users = list(mongo_db.users.find())
  # logger.info(f"users[0]: {users[0]}")
  users.append(cmguser)

  output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'output'))
  os.makedirs(output_dir, exist_ok=True)

  output_file = os.path.join(output_dir, 'cmg_users')

  # todo - remove temporary file
  with open(output_file, 'w') as f:
    for user in users:
      logger.info(f"Converting user: {user}")
      f.write(f"Converting user: {user}\n")
      email = user.get('email', 'No email')
      name = user.get('username', 'No cas_id')
      username = user.get('username', 'No username')
      # f.write(f"Converting user: {email} - {name} - {username}\n")
      # logger.info(f"Converting user: {email} - {name} - {username}")
      convert_user(user, pg_cursor)
  # logger.info("convert_users successful.")


def convert_user(cmg_user, pg_cursor):
  # logger.info("convert_user", mongo_user)
  user_id = None
  # Create user
  
  # logger.info(f"Try to Convert user: {mongo_user.get('email')}")
  # Attempt to insert the user
  try:
    pg_cursor.execute(
      """
      INSERT INTO "user" (username, email, name, cas_id, is_deleted, cmg_id)
      VALUES (%s, %s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        cmg_user.get("username"),
        cmg_user.get("email"),
        cmg_user.get("fullname"),
        cmg_user.get("username"),
        not cmg_user.get("active", False),
        str(cmg_user.get("_id")),
      )
    )
    user_id = pg_cursor.fetchone()['id']
    assign_user_roles(cmg_user, user_id, pg_cursor)
  except psycopg2.errors.UniqueViolation as e:
    # logger.warning(e)
    traceback.print_exc()
    # logger.info(f"Unique violation for user: {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")
  except Exception:
    raise

  # else:
    # pass
    # # logger.info(
    #   f"No user_id found for: {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")


def assign_user_roles(cmg_user, user_id, pg_cursor):
  # # logger.info("assign_user_roles", mongo_user, user_id)
  # Mongo (CMG) roles to Postgres (Bioloop) roles

  # Fetch all roles from Postgres - admin, operator, user
  pg_cursor.execute("SELECT id, name FROM role")
  postgres_roles = {row['name']: row['id'] for row in pg_cursor.fetchall()}

  # Create user_role associations
  for cmg_role in cmg_user.get('roles', []):
    # logger.info(f"Assigning role: {cmg_role}")
    postgres_role_name = role_mapping.get(cmg_role)
    # logger.info(f"Postgres role name: {postgres_role_name}")
    if postgres_role_name and postgres_role_name in postgres_roles:
      # logger.info(f"Assigning role: {cmg_role} - {postgres_role_name} - {user_id}")
      pg_cursor.execute(
        """
        INSERT INTO user_role (user_id, role_id)
        VALUES (%s, %s)
        """,
        (user_id, postgres_roles[postgres_role_name])
      )
      # logger.info(
        # f"Assigned role: {mongo_role} - {postgres_role_name} - {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")
  # logger.info("assign_user_roles successful.")

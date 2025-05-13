import os

from ..constants.bioloop import bioloop_roles
from ..constants.cmg import cmguser
from ..constants.common import role_mapping


def get_bioloop_cmguser_id(pg_cursor):
  print("get_cmguser_id")
  pg_cursor.execute(
    """
    SELECT id FROM "user" WHERE username = 'cmguser'
    """
  )
  cmg_user_id = pg_cursor.fetchone()
  print(f"Found cmguser_id: {cmg_user_id}")

  if cmg_user_id is None:
    raise ValueError("User 'cmguser' not found in the PostgreSQL database")

  print("get_cmguser_id successful.")
  return cmg_user_id[0]


def create_roles(pg_cursor):
  # print("create_roles")
  for role in bioloop_roles:
    pg_cursor.execute(
      """
      INSERT INTO role (name, description)
      VALUES (%s, %s)
      """,
      (role['name'], role['description'])
    )

  # print(f"Inserted or updated {len(bioloop_roles)} roles.")
  # print("create_roles successful.")


def convert_users(pg_cursor, mongo_db):
  # print("convert_users")
  users = list(mongo_db.users.find())
  # print(f"users[0]: {users[0]}")
  users.append(cmguser)

  output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'output'))
  os.makedirs(output_dir, exist_ok=True)

  output_file = os.path.join(output_dir, 'cmg_users')

  # todo - remove temporary file
  with open(output_file, 'w') as f:
    for user in users:
      email = user.get('email', 'No email')
      name = user.get('cas_id', 'No cas_id')
      username = user.get('username', 'No username')
      f.write(f"Converting user: {email} - {name} - {username}\n")
      # print(f"Converting user: {email} - {name} - {username}")
      convert_user(user, pg_cursor)
  # print("convert_users successful.")


def convert_user(mongo_user, pg_cursor):
  # print("convert_user", mongo_user)
  user_id = None
  # Create user
  try:
    # print(f"Try to Convert user: {mongo_user.get('email')}")
    # Attempt to insert the user
    pg_cursor.execute(
      """
      INSERT INTO "user" (username, email, name, cas_id, is_deleted)
      VALUES (%s, %s, %s, %s, %s)
      RETURNING id
      """,
      (
        mongo_user.get("username"),
        mongo_user.get("email"),
        mongo_user.get("fullname"),
        mongo_user.get("cas_id"),
        not mongo_user.get("active", False),
      )
    )
    user_id = pg_cursor.fetchone()[0]
    # print(
    #   f"converted user: {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")
  except Exception:
    # If a unique constraint is violated, log error and continue with the next user
    # print(
    #   f"duplicate user not converted: {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")
    pass

  # Assign role to user
  if user_id is not None:
    assign_user_roles(mongo_user, user_id, pg_cursor)
  else:
    pass
    # print(
    #   f"No user_id found for: {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")


def assign_user_roles(mongo_user, user_id, pg_cursor):
  # print("assign_user_roles", mongo_user, user_id)
  # Mongo (CMG) roles to Postgres (Bioloop) roles

  # Fetch all roles from Postgres - admin, operator, user
  pg_cursor.execute("SELECT id, name FROM role")
  postgres_roles = {row[1]: row[0] for row in pg_cursor.fetchall()}

  # Create user_role associations
  for mongo_role in mongo_user.get('roles', []):
    postgres_role_name = role_mapping.get(mongo_role)
    if postgres_role_name and postgres_role_name in postgres_roles:
      pg_cursor.execute(
        """
        INSERT INTO user_role (user_id, role_id)
        VALUES (%s, %s)
        """,
        (user_id, postgres_roles[postgres_role_name])
      )
      # print(
      #   f"Assigned role: {mongo_role} - {postgres_role_name} - {mongo_user.get('email')} - {mongo_user.get('fullname')} - {mongo_user.get('cas_id')}")
  # print("assign_user_roles successful.")

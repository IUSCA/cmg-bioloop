# DATA MANIPULATION OPERATIONS

from ..pg_queries import queries


def drop_all_enums(pg_cursor):
  print("drop_all_enums")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_enums)
  print("All enum types dropped successfully.")


def drop_all_tables(pg_cursor):
  print("drop_all_tables")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_tables)
  print("All tables dropped successfully.")
  # self.postgres_conn.commit()

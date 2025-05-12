# DATA DEFINITION LANG OPERATIONS
from ..pg_queries import queries


def create_all_enums(pg_cursor):
  print("create_all_enums")
  with pg_cursor:
    pg_cursor.execute(queries.create_all_enums)
  print("Database enums created successfully.")


def create_all_tables(pg_cursor):
  print("create_tables")
  with pg_cursor:
    pg_cursor.execute(queries.create_all_tables)
  print("Database tables created successfully.")

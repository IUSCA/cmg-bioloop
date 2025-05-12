# DATA DEFINITION OPERATIONS

from ..pg_queries import queries


def create_all_enums(pg_cursor):
  print("create_all_enums")
  pg_cursor.execute(queries.create_all_enums)
  print("Database enums created successfully.")


def create_all_tables(pg_cursor):
  print("create_tables")
  pg_cursor.execute(queries.create_all_tables)
  print("Database tables created successfully.")

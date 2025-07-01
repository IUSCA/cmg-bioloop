from pymongo.database import Database

from ..pg_queries import queries

"""
This module contains Data Definition Language (DDL) operations for managing
database schema objects such as enums and tables.

These functions execute SQL queries to create or drop database objects.
"""


def create_bioloop_enums(pg_cursor):
  print("create_all_enums")
  pg_cursor.execute(queries.create_all_enums)
  print("Database enums created successfully.")


def create_bioloop_tables(pg_cursor):
  print("create_tables")
  pg_cursor.execute(queries.create_all_tables)
  print("Database tables created successfully.")


def drop_bioloop_enums(pg_cursor):
  print("drop_all_enums")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_enums)
  print("All enum types dropped successfully.")


def drop_bioloop_tables(pg_cursor):
  print("drop_all_tables")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_tables)
  print("All tables dropped successfully.")


def drop_all_workflow_documents(rhythm_db: Database):
  print("drop_workflow_meta_documents")
  result = rhythm_db.workflow_meta.delete_many({})
  print(f"Deleted {result.deleted_count} documents from workflow_meta collection.")

  print("drop_celery_taskmeta_documents")
  result = rhythm_db.celery_taskmeta.delete_many({})
  print(f"Deleted {result.deleted_count} documents from celery_taskmeta collection.")

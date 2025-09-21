import logging

from pymongo.database import Database

from ..pg_queries import queries

logger = logging.getLogger(__name__)


"""
This module contains Data Definition Language (DDL) operations for managing
database schema objects such as enums and tables.

These functions execute SQL queries to create or drop database objects.
"""


def create_bioloop_enums(pg_cursor):
  logger.info("create_all_enums")
  pg_cursor.execute(queries.create_all_enums)
  logger.info("Database enums created successfully.")


def create_bioloop_tables(pg_cursor):
  logger.info("create_tables")
  pg_cursor.execute(queries.create_all_tables)
  logger.info("Database tables created successfully.")


def drop_bioloop_enums(pg_cursor):
  logger.info("drop_all_enums")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_enums)
  logger.info("All enum types dropped successfully.")


def drop_bioloop_tables(pg_cursor):
  logger.info("drop_all_tables")
  # with pg_cursor:
  pg_cursor.execute(queries.drop_all_tables)
  logger.info("All tables dropped successfully.")


def drop_all_workflow_documents(rhythm_db: Database):
  logger.info("drop_workflow_meta_documents")
  result = rhythm_db.workflow_meta.delete_many({})
  logger.info(f"Deleted {result.deleted_count} documents from workflow_meta collection.")

  logger.info("drop_celery_taskmeta_documents")
  result = rhythm_db.celery_taskmeta.delete_many({})
  logger.info(f"Deleted {result.deleted_count} documents from celery_taskmeta collection.")

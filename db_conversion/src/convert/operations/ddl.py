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


def drop_bioloop_workflow_documents(rhythm_db: Database):
  logger.info("drop_workflow_meta_collection")
  # rhythm_db.drop_collection("workflow_meta")
  logger.info("Dropped workflow_meta collection successfully.")

  logger.info("drop_celery_taskmeta_collection")
  # rhythm_db.drop_collection("celery_taskmeta")
  logger.info("Dropped celery_taskmeta collection successfully.")

def create_bioloop_workflow_documents(rhythm_db: Database):
    logger.info("create_workflow_meta_collection")
    # rhythm_db.create_collection("workflow_meta")
    logger.info("Workflow meta collection created successfully.")

    logger.info("create_celery_taskmeta_collection")
    # rhythm_db.create_collection("celery_taskmeta")
    logger.info("Celery taskmeta collection created successfully.")

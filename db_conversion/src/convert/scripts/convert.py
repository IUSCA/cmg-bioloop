import os

import pymongo
import psycopg2
from dotenv import load_dotenv
import fire
# needed for SSH tunneling to connect to MongoDB running on a remote server
import paramiko
from sshtunnel import SSHTunnelForwarder

from ..entity import *
from ..operations import *


# python -um src.convert.scripts.convert


class MongoToPostgresConversionManager:

  def __init__(self,
               # ssh_config,
               cmg_mongo_config,
               rhythm_mongo_config,
               pg_conn_env_vars):

    print("cmg_mongo_config:", cmg_mongo_config)
    print("rhythm_mongo_config:", rhythm_mongo_config)

    # MongoDB connection
    # Set up SSH tunnel to connect to MongoDB running on a remote server
    # self.tunnel = SSHTunnelForwarder(
    #     (ssh_config['hostname'], 22),
    #     ssh_username=ssh_config['username'],
    #     ssh_pkey=ssh_config['private_key_path'],
    #     remote_bind_address=('localhost', mongo_config['port']),
    #     local_bind_address=('localhost', 27017)
    # )
    # self.tunnel.start()
    # self.mongo_client = pymongo.MongoClient(mongo_conn_string)
    # self.mongo_db = self.mongo_client["cmg_database"]

    # Initiate MongoDB connection
    # mongo_uri = f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}?authSource={mongo_config['authSource']}"
    cmg_mongo_uri = f'mongodb://{cmg_mongo_config["host"]}:{cmg_mongo_config["port"]}'
    print(f"Connecting to MongoDB with URI: {cmg_mongo_uri}")
    self.cmg_mongo_client = pymongo.MongoClient(cmg_mongo_uri)
    self.cmg_mongo_db = self.cmg_mongo_client[cmg_mongo_config['database']]

    rhythm_mongo_uri = f'mongodb://{rhythm_mongo_config["username"]}:{rhythm_mongo_config["password"]}@{rhythm_mongo_config["host"]}:{rhythm_mongo_config["port"]}/{rhythm_mongo_config["database"]}?authSource={rhythm_mongo_config["authSource"]}'
    print(f"Connecting to MongoDB with URI: {rhythm_mongo_uri}")
    self.rhythm_mongo_client = pymongo.MongoClient(rhythm_mongo_uri)
    self.rhythm_mongo_db = self.rhythm_mongo_client[rhythm_mongo_config['database']]

    # Test Rhythm MongoDB connection
    # self.test_rhythm_mongo_connection()

    #  Initiate PostgreSQL connection
    self.postgres_conn = psycopg2.connect(
      host=pg_conn_env_vars['PG_HOST'],
      port=pg_conn_env_vars['PG_PORT'],
      database=pg_conn_env_vars['PG_DATABASE'],
      user=pg_conn_env_vars['PG_USER'],
      password=pg_conn_env_vars['PG_PASSWORD']
    )

    # disable transaction's rollback for seeing actual errors instead of
    # generic error '... psycopg2.errors.InFailedSqlTransaction: current transaction is aborted,
    # commands ignored until end of transaction block ...'
    self.postgres_conn.set_session(autocommit=True)

    self.pg_cursor = self.postgres_conn.cursor()

  def convert_mongo_to_postgres(self):
    try:
      with self.postgres_conn.cursor() as pg_cursor:
        # Drop existing Postgres tables, enums
        drop_bioloop_enums(pg_cursor=pg_cursor)
        drop_bioloop_tables(pg_cursor=pg_cursor)

        # Re-create tables, enums
        create_bioloop_enums(pg_cursor=pg_cursor)
        create_bioloop_tables(pg_cursor=pg_cursor)

        # Drop workflow documents
        drop_all_workflow_documents(rhythm_db=self.rhythm_mongo_db)

        # Convert CMG data (Mongo) to Bioloop data (Postgres)
        print("converting roles")
        create_roles(pg_cursor=pg_cursor)
        print("converting users")
        convert_users(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        print("converting datasets")
        convert_all_datasets(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        print("converting dataset audit logs")
        events_to_audit_logs(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        print("converting dataset hierarchies")
        convert_dataset_hierarchies(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        print("Converting dataset files")
        convert_dataproduct_files(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        print("converting projects")
        convert_projects(pg_cursor=pg_cursor, mongo_db=self.cmg_mongo_db)
        # convert_content_to_about(cursor, self.mongo_db)
        print("Creating workflows")
        create_workflows(pg_cursor=pg_cursor, rhythm_db=self.rhythm_mongo_db)

      # Commit the transaction
      self.postgres_conn.commit()
      print("Data conversion completed successfully.")
    except Exception as e:
      # Rollback if an error occurs
      self.postgres_conn.rollback()
      print(f"An error occurred during conversion: {e}")
      raise
    finally:
      self.close_connections()

  def close_connections(self):
    self.cmg_mongo_client.close()
    self.postgres_conn.close()
    # self.tunnel.stop()

  def test_rhythm_mongo_connection(self):
    print("Testing Rhythm MongoDB connection")
    try:
      info = self.rhythm_mongo_client.server_info()
      print("Rhythm MongoDB connection successful.")
      print(f"MongoDB version: {info['version']}")
      print(f"MongoDB host: {self.rhythm_mongo_client.address[0]}")
      print(f"MongoDB port: {self.rhythm_mongo_client.address[1]}")
      print(f"MongoDB database: {self.rhythm_mongo_db.name}")

      # Test write permission
      # test_collection = self.rhythm_mongo_db.test_collection
      # test_doc = {"test": "document"}
      # result = test_collection.insert_one(test_doc)
      # test_collection.delete_one({"_id": result.inserted_id})
      # print("Successfully performed test write and delete operations.")

      return True
    except pymongo.errors.ConnectionFailure as e:
      print(f"Rhythm MongoDB connection failed: {e}")
      raise
    except pymongo.errors.OperationFailure as e:
      print(f"Rhythm MongoDB authentication or permission error: {e}")
      raise
    except Exception as e:
      print(f"Unexpected error when connecting to Rhythm MongoDB: {e}")
      raise

  # def test_mongo_connection(self):
  #   print("Testing MongoDB connection")
  #   try:
  #     info = self.mongo_client.server_info()
  #     print("MongoDB connection successful.")
  #     print(f"MongoDB version: {info['version']}")
  #     print(f"MongoDB host: {self.mongo_client.address[0]}")
  #     print(f"MongoDB port: {self.mongo_client.address[1]}")
  #     print(f"MongoDB database: {self.mongo_db.name}")
  #     return True
  #   except pymongo.errors.ConnectionFailure as e:
  #     print(f"MongoDB connection failed: {e}")
  #     return False
  #   except Exception as e:
  #     print(f"Unexpected error when connecting to MongoDB: {e}")
  #     return False


def main():
  """
  Main function to run the MongoDB to PostgreSQL conversion.

  Usage:
      To run the conversion:
      $ python db_conversion.py

  Environment Variables:
      The following environment variables must be set:
      - MONGO_CONNECTION_STRING: MongoDB connection string
      - PG_DATABASE: PostgreSQL database name
      - PG_USER: PostgreSQL username
      - PG_PASSWORD: PostgreSQL password
      - PG_HOST: PostgreSQL host
      - PG_PORT: PostgreSQL port

  Returns:
      None
  """

  load_dotenv()

  # ssh_config = {
  #     'hostname': os.getenv('SSH_HOSTNAME'),
  #     'username': os.getenv('SSH_USERNAME'),
  #     'private_key_path': os.getenv('SSH_PRIVATE_KEY_PATH')
  # }

  print("Starting MongoDB to PostgreSQL conversion")
  # print(f"os.getenv('MONGO_USERNAME'): {os.getenv('MONGO_USERNAME')}")
  # print(f"os.getenv('MONGO_PORT'): {os.getenv('MONGO_PORT')}")

  # todo - throw error if mongo env vars are missing
  cmg_mongo_config = {
    'host': os.getenv('MONGO_HOST'),
    'port': int(os.getenv('MONGO_PORT', 27017)),
    'database': os.getenv('MONGO_DB'),
    'authSource': os.getenv('MONGO_AUTH_SOURCE'),
    'username': os.getenv('MONGO_USERNAME'),
    'password': os.getenv('MONGO_PASSWORD'),
  }

  rhythm_mongo_config = {
    'host': os.getenv('RHYTHM_MONGO_HOST'),
    'port': int(os.getenv('RHYTHM_MONGO_PORT', 27018)),
    'database': os.getenv('RHYTHM_MONGO_DB'),
    'authSource': os.getenv('RHYTHM_MONGO_AUTH_SOURCE'),
    'username': os.getenv('RHYTHM_MONGO_USERNAME'),
    'password': os.getenv('RHYTHM_MONGO_PASSWORD'),
  }

  # print(f"mongo_config: {mongo_config}")

  postgres_db = os.getenv('PG_DATABASE')
  pg_user = os.getenv('PG_USER')
  pg_password = os.getenv('PG_PASSWORD')
  pg_host = os.getenv('PG_HOST')
  pg_port = os.getenv('PG_PORT')

  pg_env_vars = {
    'PG_DATABASE': postgres_db,
    'PG_USER': pg_user,
    'PG_PASSWORD': pg_password,
    'PG_HOST': pg_host,
    'PG_PORT': pg_port
  }
  missing_pg_env_vars = [var for var, value in pg_env_vars.items() if not value]

  if missing_pg_env_vars:
    # print("Following environment variables that are expected to connect to the PostgreSQL database are missing:")
    for var in missing_pg_env_vars:
      pass
      # print(f"env variable {var} not provided")
    raise ValueError("Missing required environment variables for connecting to PostgreSQL")

  try:
    # Initialize the conversion manager (this will establish connections)
    manager = MongoToPostgresConversionManager(cmg_mongo_config, rhythm_mongo_config, pg_env_vars)
  except Exception as e:
    print(f"Error connecting to databases: {e}")
    raise

  try:
    # Begin the conversion process
    manager.convert_mongo_to_postgres()
  except Exception as e:
    print(f"Error during conversion: {e}")
    raise
  finally:
    if manager:
      manager.close_connections()


if __name__ == "__main__":
  fire.Fire(main)

from bson.objectid import ObjectId

from psycopg2.extensions import cursor
from pymongo.database import Database


def create_audit_logs_from_dataset_events(pg_cursor: cursor,
                                          mongo_db: Database,
                                          cmg_dataset: dict,
                                          bioloop_dataset_id: int):
  # 1. Create dataset_audit records for events in the `dataset`/`dataproduct` collections.
  events = cmg_dataset.get("events", [])
  if len(events) > 0:
    for event in events:
      pg_cursor.execute(
        """
        INSERT INTO dataset_audit (action, timestamp, dataset_id)
        VALUES (%s, %s, %s)
        """,
        (
          event.get("description"),
          event.get("stamp"),
          bioloop_dataset_id,
        )
      )

  # 2. Create dataset_audit records for events in the `events` collection.
  mongo_dataset_id = cmg_dataset.get("_id")
  if mongo_dataset_id:
    events_collection = mongo_db['events']
    users_collection = mongo_db['users']

    # Query events for this CMG dataset
    dataset_events = events_collection.find({'dataset': ObjectId(mongo_dataset_id)})

    for event in dataset_events:
      mongo_user_id = event.get('user')
      pg_user_id = None
      if mongo_user_id:
        # Fetch CMG user details
        mongo_user = users_collection.find_one({'_id': mongo_user_id})
        if mongo_user:
          username = mongo_user.get('username')
          email = mongo_user.get('email')

          # Fetch Bioloop user id
          pg_cursor.execute(
            """
            SELECT id FROM "user" WHERE username = %s AND email = %s
            """,
            (username, email)
          )
          result = pg_cursor.fetchone()
          if result:
            pg_user_id = result[0]

      pg_cursor.execute(
        """
        INSERT INTO dataset_audit (action, timestamp, user_id, dataset_id)
        VALUES (%s, %s, %s, %s)
        """,
        (
          event.get('action'),
          event.get('createdAt'),
          pg_user_id,
          bioloop_dataset_id,
        )
      )

  print(f"Created audit logs for dataset {bioloop_dataset_id}")

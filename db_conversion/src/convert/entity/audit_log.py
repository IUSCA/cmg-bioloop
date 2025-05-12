def create_audit_logs_from_dataset_events(pg_cursor, cmg_dataset, bioloop_dataset_id, user_id):
  events = cmg_dataset.get("events", [])
  if len(events) > 0:
    for event in events:
      pg_cursor.execute(
        """
        INSERT INTO dataset_audit (action, timestamp, dataset_id, user_id)
        VALUES (%s, %s, %s, %s)
        """,
        (
          event.get("description"),
          event.get("createdAt"),
          bioloop_dataset_id,
          user_id,
        )
      )

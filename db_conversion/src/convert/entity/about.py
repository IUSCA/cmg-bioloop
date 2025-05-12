import html

from .user import get_cmguser_id


def convert_content_to_about(pg_cursor, mongo_db):
  # with postgres_conn.cursor() as cur:
  cmg_user_id = get_cmguser_id(pg_cursor)

  contents = mongo_db.content.find()
  for content in contents:
    # Get the details
    details = content.get("details", "")
    # Escape HTML special characters
    escaped_details = html.escape(details)
    # Split the string into paragraphs, and wrap each in <p> elements
    paragraphs = escaped_details.split('\n\n')
    html_content = ''.join(f'<p>{p.strip()}</p>' for p in paragraphs if p.strip())

    pg_cursor.execute(
      """
      INSERT INTO about (html, last_updated_by_id)
      VALUES (%s, %s)
      """,
      (
        html_content,
        cmg_user_id,
      )
    )

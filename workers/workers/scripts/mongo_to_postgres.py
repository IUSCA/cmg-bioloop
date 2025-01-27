import pymongo
import psycopg2
from psycopg2 import sql

# MongoDB connection
mongo_client = pymongo.MongoClient("connection_string")
mongo_db = mongo_client["db_name"]

# Postgres connection
postgres_conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="",
    port=""
)


def convert_data():
    with postgres_conn.cursor() as cur:
        # todo - create relational associations

        # Convert Users
        users = mongo_db.users.find()
        for user in users:
            cur.execute(
                sql.SQL("""
                INSERT INTO "user" (username, email, "name", "cas_id", "is_deleted")
                VALUES (%s, %s, %s, %s, %s, %s)
                """),
                (
                    user.get("username"),
                    user.get("email"),
                    user.get("fullname"),
                    user.get("cas_id"),
                    user.get("active"),
                )
            )

        # Convert Projects
        projects = mongo_db.projects.find()
        for project in projects:
            cur.execute(
                sql.SQL("""
                INSERT INTO project (slug, name, description, browser_enabled)
                VALUES (%s, %s, %s, %s, %s)
                """),
                (
                    project.get("name"),
                    project.get("slug"),
                    project.get("description"),
                )
            )

    # Commit the changes
    postgres_conn.commit()

if __name__ == "__main__":
    convert_data()
    print("Data conversion completed.")

    # Close connections
    mongo_client.close()
    postgres_conn.close()

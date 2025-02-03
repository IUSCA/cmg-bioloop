import pymongo
import psycopg2
from psycopg2 import sql


class MongoToPostgresConversionManager:
    def __init__(self, mongo_conn_string, pg_conn_string):
        # MongoDB connection
        self.mongo_client = pymongo.MongoClient(mongo_conn_string)
        self.mongo_db = self.mongo_client["cmg_database"]

        # Postgres connection
        self.postgres_conn = psycopg2.connect(**self.parse_conn_string(pg_conn_string))
        # Postgres connection
        self.postgres_conn = psycopg2.connect(**self.parse_conn_string(pg_conn_string))

    @staticmethod
    def parse_conn_string(conn_string):
        parts = conn_string.split('//')[1].split('@')
        user_pass, host_port_db = parts[0], parts[1]
        username, password = user_pass.split(':')
        host, port_db = host_port_db.split(':')
        port, dbname = port_db.split('/')
        return {
            "dbname": dbname,
            "user": username,
            "password": password,
            "host": host,
            "port": port
        }

    def get_scauser_id(self, cur):
        cur.execute(
            """
            SELECT id FROM "user" WHERE username = 'scauser'
            """
        )
        sca_user_id = cur.fetchone()

        if sca_user_id is None:
            raise ValueError("User 'scauser' not found in the PostgreSQL database")

        return sca_user_id[0]

    def convert_users(self):
        with self.postgres_conn.cursor() as cur:
            users = self.mongo_db.users.find()
            for user in users:
                cur.execute(
                    """
                    INSERT INTO "user" (username, email, "name", cas_id, is_deleted)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        user.get("username"),
                        user.get("email"),
                        user.get("fullname"),
                        user.get("cas_id"),
                        not user.get("active", True),
                    )
                )

    def convert_projects(self):
        with self.postgres_conn.cursor() as cur:
            projects = self.mongo_db.projects.find()
            for project in projects:
                cur.execute(
                    """
                    INSERT INTO project (name, slug, description, browser_enabled)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        project.get("name"),
                        project.get("slug"),
                        project.get("description"),
                        project.get("browser_enabled", False),
                    )
                )

    @staticmethod
    def mongo_file_to_pg_file(file: dict) -> dict:
        return {
            "name": file.get("path", "").split("/")[-1],
            "path": file.get("path"),
            "md5": file.get("md5"),
            "size": file.get("size"),
        }

    def convert_datasets(self):
        with self.postgres_conn.cursor() as cur:
            sca_user_id = self.get_scauser_id(cur)

            mongo_datasets = self.mongo_db.dataset.find()
            for mongo_dataset in mongo_datasets:
                cur.execute(
                    """
                    INSERT INTO dataset (name, type, description, num_directories, num_files, 
                                         du_size, size, created_at, updated_at, origin_path, 
                                         archive_path, is_staged, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        mongo_dataset["name"],
                        "RAW_DATA",
                        mongo_dataset.get("description"),
                        mongo_dataset.get("directories"),
                        mongo_dataset.get("files"),
                        mongo_dataset.get("du_size"),
                        mongo_dataset.get("size"),
                        mongo_dataset.get("createdAt", datetime.utcnow()),
                        mongo_dataset.get("updatedAt", datetime.utcnow()),
                        mongo_dataset.get("paths", {}).get("origin"),
                        mongo_dataset.get("paths", {}).get("archive"),
                        mongo_dataset.get("staged", False),
                        json.dumps(mongo_dataset),
                    )
                )
                raw_data_dataset_id = cur.fetchone()[0]

                # Insert dataset_audit records if events exist
                events = mongo_dataset.get("events", [])
                if len(events) > 0:
                    for event in events:
                        cur.execute(
                            """
                            INSERT INTO dataset_audit (action, timestamp, dataset_id, user_id)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                event.get("description"),
                                event.get("stamp", datetime.utcnow()),
                                raw_data_dataset_id,
                                sca_user_id,
                            )
                        )

                for checksum in mongo_dataset.get("checksums", []):
                    pg_raw_data_file = self.mongo_file_to_pg_file(checksum)
                    cur.execute(
                        """
                        INSERT INTO dataset_file (name, path, md5, size, dataset_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            pg_raw_data_file["name"],
                            pg_raw_data_file["path"],
                            pg_raw_data_file["md5"],
                            pg_raw_data_file["size"],
                            raw_data_dataset_id,
                        )
                    )

    def convert_data_products(self):
        with self.postgres_conn.cursor() as cur:
            sca_user_id = self.get_scauser_id(cur)

            mongo_data_products = self.mongo_db.dataproduct.find()
            for mongo_data_product in mongo_data_products:
                cur.execute(
                    """
                    INSERT INTO dataset (name, type, description, num_files, size, created_at, 
                                         updated_at, archive_path, is_staged, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        mongo_data_product["name"],
                        "DATA_PRODUCT",
                        None,
                        len(mongo_data_product.get("files", [])),
                        mongo_data_product.get("size"),
                        mongo_data_product.get("createdAt", datetime.utcnow()),
                        mongo_data_product.get("updatedAt", datetime.utcnow()),
                        mongo_data_product.get("paths", {}).get("archive"),
                        mongo_data_product.get("staged", False),
                        json.dumps(mongo_data_product),
                    )
                )
                data_product_dataset_id = cur.fetchone()[0]

                # Insert dataset_audit records if events exist
                events = mongo_data_product.get("events", [])
                if len(events) > 0:
                    for event in events:
                        cur.execute(
                            """
                            INSERT INTO dataset_audit (action, timestamp, dataset_id, user_id)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                event.get("description"),
                                event.get("stamp", datetime.utcnow()),
                                data_product_dataset_id,
                                sca_user_id,
                            )
                        )

                for file in mongo_data_product.get("files", []):
                    pg_data_product_file = self.mongo_file_to_pg_file(file)
                    cur.execute(
                        """
                        INSERT INTO dataset_file (name, path, md5, size, dataset_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            pg_data_product_file["name"],
                            pg_data_product_file["path"],
                            pg_data_product_file["md5"],
                            pg_data_product_file["size"],
                            data_product_dataset_id,
                        )
                    )

    def convert_content_to_about(self):
        with self.postgres_conn.cursor() as cur:
            # First, fetch the user ID for 'scauser'
            cur.execute(
                """
                SELECT id FROM "user" WHERE username = 'scauser'
                """
            )
            sca_user_id = cur.fetchone()

            if sca_user_id is None:
                raise ValueError("User 'scauser' not found in the PostgreSQL database")

            sca_user_id = sca_user_id[0]

            contents = self.mongo_db.content.find()
            for content in contents:
                # Get the details string
                details = content.get("details", "")

                # Escape HTML special characters
                escaped_details = html.escape(details)

                # Split the string into paragraphs and wrap each in <p> tags
                paragraphs = escaped_details.split('\n\n')
                html_content = ''.join(f'<p>{p.strip()}</p>' for p in paragraphs if p.strip())

                cur.execute(
                    """
                    INSERT INTO about (html, last_updated_by_id)
                    VALUES (%s, %s)
                    """,
                    (
                        html_content,
                        sca_user_id,
                    )
                )

    def convert_all(self):
        try:
            # Start transaction
            self.postgres_conn.cursor().execute("BEGIN")

            self.convert_users()
            self.convert_projects()
            self.convert_datasets()
            self.convert_data_products()
            self.convert_content_to_about()

            # Commit the transaction if everything succeeds
            self.postgres_conn.commit()
            print("Data conversion completed successfully.")
        except Exception as e:
            # Rollback the transaction if an error occurs
            self.postgres_conn.rollback()
            print(f"An error occurred during conversion: {e}")
            raise
        finally:
            self.close_connections()

    def close_connections(self):
        self.mongo_client.close()
        self.postgres_conn.close()


if __name__ == "__main__":
    mongo_conn_string = ""
    pg_conn_string = ""

    converter = MongoToPostgresConversionManager(mongo_conn_string, pg_conn_string)
    converter.convert_all()

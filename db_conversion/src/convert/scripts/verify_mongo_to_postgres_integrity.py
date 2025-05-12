import pymongo
import psycopg2
import hashlib
import json

# Mongo conn
mongo_client = pymongo.MongoClient("connection_string")
mongo_db = mongo_client["db_name"]

# Postgres conn
postgres_conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="",
    port=""
)
postgres_cursor = postgres_conn.cursor()


def calculate_checksum(obj):
    return hashlib.md5(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def record_count_comparison(mongo_collection, postgres_table):
    mongo_count = mongo_db[mongo_collection].count_documents({})
    postgres_cursor.execute(f"SELECT COUNT(*) FROM {postgres_table}")
    postgres_count = postgres_cursor.fetchone()[0]
    print(f"MongoDB: {mongo_count}, PostgreSQL: {postgres_count}")
    print('EQUAL' if mongo_count == postgres_count else 'NOT EQUAL')


def checksum_comparison(mongo_collection, postgres_table, sample_size=100):
    # Compare 100 docs only for now

    # Fetch records from Mongo
    mongo_records = list(mongo_db[mongo_collection].aggregate([{"$sample": {"size": sample_size}}]))

    for mongo_record in mongo_records:
        mongo_id = str(mongo_record['_id'])
        # TODO - come up with filtering criteria to fetch the corresponding Postgres record.
        #   Criteria will be different depending on the table.
        postgres_cursor.execute(f"SELECT * FROM {postgres_table} WHERE")
        # WHERE username = %s", (mongo_record['username'],))
        postgres_record = postgres_cursor.fetchone()

        if postgres_record:
            mongo_checksum = calculate_checksum(mongo_record)
            postgres_checksum = calculate_checksum(postgres_record)

            if mongo_checksum != postgres_checksum:
                print(f"Checksum mismatch for record {mongo_id} in {mongo_collection}/{postgres_table}")


def sample_data_comparison(mongo_collection, postgres_table, sample_size=100):
    mongo_records = list(mongo_db[mongo_collection].aggregate([{"$sample": {"size": sample_size}}]))

    for mongo_record in mongo_records:
        mongo_id = str(mongo_record['_id'])
        postgres_cursor.execute(f"SELECT * FROM {postgres_table} WHERE")
        postgres_record = postgres_cursor.fetchone()

        # compare values of fields. Mongo-to-Postgres fields will need to be mapped depending on the collection/table.


def main():
    # Record count comparison
    record_count_comparison("users", "user")
    record_count_comparison("projects", "project")

    # Checksum comparison
    checksum_comparison("users", "user")
    checksum_comparison("projects", "project")

    # Sample data comparison
    sample_data_comparison("users", "user")
    sample_data_comparison("projects", "project")


if __name__ == "__main__":
    main()

    # Close connections
    mongo_client.close()
    postgres_cursor.close()
    postgres_conn.close()